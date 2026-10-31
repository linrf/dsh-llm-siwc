#!/usr/bin/env node
/**
 * siwc-gateway v2 -- production-ish bridge that turns the ChatGPT-plan
 * Responses route (Sign in with ChatGPT OAuth) into a local OpenAI-compatible
 * HTTPS endpoint for codex / DSH clients.
 *
 * Design goals:
 *   - single owner of the rotating refresh token (serial refresh, persisted)
 *   - low-latency: cached + compressible /v1/models, immediate SSE flush
 *   - safe: bearer auth (constant time), request/body limits, concurrency cap,
 *     per-IP rate limit, idle timeouts, graceful shutdown, no secrets in logs
 *   - observable: JSON logs to stdout/journal + Prometheus text at /metrics
 */
import { createServer } from 'node:http'
import { createHash, randomUUID, timingSafeEqual } from 'node:crypto'
import { existsSync, readFileSync, readdirSync, writeFileSync, renameSync, mkdirSync } from 'node:fs'
import { join } from 'node:path'
import { once } from 'node:events'

const VERSION = '2.1.1'
const STORE_DIR = process.env.SIWC_STORE ?? join(process.env.HOME ?? '/root', '.dsh', 'siwc')
const CRED_DIR = join(STORE_DIR, 'credentials')
const PORT = Number(process.env.PORT ?? 1457)
const HOST = process.env.HOST ?? '127.0.0.1'
const API_BASE = (process.env.API_BASE ?? 'https://api.openai.com/v1').replace(/\/+$/, '')
const GATEWAY_TOKEN = process.env.GATEWAY_TOKEN ?? ''
const MAX_CONCURRENCY = Number(process.env.MAX_CONCURRENCY ?? 16)
const MAX_QUEUE = Number(process.env.MAX_QUEUE ?? 32)
const QUEUE_WAIT_MS = Number(process.env.QUEUE_WAIT_MS ?? 45000)
const RATE_LIMIT_PER_MIN = Number(process.env.RATE_LIMIT_PER_MIN ?? 240)
const MODELS_TTL_MS = Number(process.env.MODELS_TTL_MS ?? 300_000)
const MODELS_TIMEOUT_MS = Number(process.env.MODELS_TIMEOUT_MS ?? 30_000)
const MODELS_MAX_STALE_MS = Number(process.env.MODELS_MAX_STALE_MS ?? 30 * 60_000)
const BODY_TIMEOUT_MS = Number(process.env.BODY_TIMEOUT_MS ?? 30_000)
const UPSTREAM_HEADERS_TIMEOUT_MS = Number(process.env.UPSTREAM_HEADERS_TIMEOUT_MS ?? 180_000)
const SSE_IDLE_TIMEOUT_MS = Number(process.env.SSE_IDLE_TIMEOUT_MS ?? 300_000)
const MAX_BODY_BYTES = Number(process.env.MAX_BODY_BYTES ?? 8 * 1024 * 1024)
const TRUST_PROXY = process.env.TRUST_PROXY === '1'
const TOKEN_URL = 'https://auth.openai.com/api/accounts/oauth/token'
const RESOURCE = 'https://api.openai.com/v1'
const REFRESH_LEAD_MS = 120_000

const UNSUPPORTED = ['background','conversation','max_output_tokens','max_tool_calls','metadata','moderation','multi_agent','prompt','prompt_cache_retention','safety_identifier','temperature','top_logprobs','top_p','truncation','user']
const FORWARD_HEADERS = ['session-id', 'conversation_id', 'originator', 'x-client-request-id']

/* ------------------------------------------------------------------ logging */

function log(level, event, fields) {
  const line = { ts: new Date().toISOString(), level, event }
  if (fields) Object.assign(line, fields)
  const text = JSON.stringify(line)
  if (level === 'error' || level === 'warn') console.error(text)
  else console.log(text)
}

/* -------------------------------------------------------------- credentials */

let record = null
let credFile = null
let refreshInFlight = null
const metrics = {
  startedAt: Date.now(),
  requests: 0,
  responses: 0,
  models: 0,
  health: 0,
  byStatus: new Map(),
  upstreamCalls: 0,
  upstreamErrors: 0,
  upstreamStatus: new Map(),
  refreshes: 0,
  refreshFailures: 0,
  modelsCacheHits: 0,
  modelsCacheMisses: 0,
  modelsRefreshFailures: 0,
  modelsStaleResponses: 0,
  sseBytes: 0,
  activeStreams: 0,
  inflight: 0,
  peakInflight: 0,
  bytesOut: 0,
}

function loadRecord() {
  if (!existsSync(CRED_DIR)) return null
  let best = null
  for (const name of readdirSync(CRED_DIR).filter((n) => n.endsWith('.json'))) {
    try {
      const rec = JSON.parse(readFileSync(join(CRED_DIR, name), 'utf8'))
      const ts = Date.parse(rec.savedAt ?? 0) || 0
      if (!best || ts > best.ts) best = { ts, rec, name }
    } catch (error) {
      log('warn', 'credential-unreadable', { file: name, error: String(error) })
    }
  }
  return best
}

function persist() {
  if (!credFile || !record) return
  const tmp = credFile + '.tmp'
  writeFileSync(tmp, JSON.stringify(record, null, 2), { mode: 0o600 })
  renameSync(tmp, credFile)
}

async function doRefresh() {
  if (!record) throw new Error('no credential loaded')
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    client_id: record.clientId,
    refresh_token: record.refreshToken,
    resource: RESOURCE,
  })
  let res
  try {
    res = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
      signal: AbortSignal.timeout(30_000),
    })
  } catch (error) {
    metrics.refreshFailures += 1
    throw new Error('refresh transport failed: ' + String((error && error.message) || error))
  }
  const text = await res.text()
  if (!res.ok) {
    metrics.refreshFailures += 1
    throw new Error('refresh failed HTTP ' + res.status + ': ' + text.slice(0, 200))
  }
  let parsed
  try {
    parsed = JSON.parse(text)
  } catch {
    metrics.refreshFailures += 1
    throw new Error('refresh returned invalid JSON')
  }
  record.accessToken = parsed.access_token
  if (parsed.refresh_token) record.refreshToken = parsed.refresh_token
  if (parsed.token_type) record.tokenType = parsed.token_type
  record.expiresAt = Date.now() + (parsed.expires_in ?? 3600) * 1000
  record.savedAt = new Date().toISOString()
  persist()
  metrics.refreshes += 1
  log('info', 'token-refreshed', { account: record.email, validSeconds: Math.round((record.expiresAt - Date.now()) / 1000) })
  return record
}

function ensureRecord() {
  if (record) return
  const initial = loadRecord()
  if (initial) {
    record = initial.rec
    credFile = join(CRED_DIR, initial.name)
  }
}

async function accessToken(force, rejectedToken) {
  ensureRecord()
  if (!record) throw new Error('no siwc credential in ' + CRED_DIR)
  // A concurrent 401 may arrive after another request already rotated the
  // rejected token. Reuse that newer credential instead of rotating it again.
  if (force && rejectedToken !== undefined && record.accessToken !== rejectedToken && Date.now() < record.expiresAt) return record.accessToken
  if (!force && Date.now() < record.expiresAt - REFRESH_LEAD_MS) return record.accessToken
  // All callers share one refresh, including its error. Clear the promise on
  // either outcome so a transient failure does not poison future attempts.
  if (!refreshInFlight) {
    refreshInFlight = doRefresh().finally(() => { refreshInFlight = null })
  }
  await refreshInFlight
  if (Date.now() >= record.expiresAt) throw new Error('access token still expired after refresh')
  return record.accessToken
}

/* ----------------------------------------------------------- http utilities */

function authorized(req) {
  if (!GATEWAY_TOKEN) return false
  const header = req.headers.authorization ?? ''
  if (!header.startsWith('Bearer ')) return false
  const given = Buffer.from(header.slice(7))
  const expected = Buffer.from(GATEWAY_TOKEN)
  return given.length === expected.length && timingSafeEqual(given, expected)
}

function clientIp(req) {
  if (TRUST_PROXY) {
    const forwarded = req.headers['x-forwarded-for']
    if (typeof forwarded === 'string' && forwarded.length > 0) return forwarded.split(',')[0].trim()
  }
  return req.socket.remoteAddress ?? 'unknown'
}

function json(res, status, obj, extraHeaders) {
  const body = Buffer.from(JSON.stringify(obj))
  res.writeHead(status, Object.assign({ 'content-type': 'application/json; charset=utf-8', 'content-length': body.length, 'cache-control': 'no-store' }, extraHeaders))
  res.end(body)
}

function readBody(req, limit, signal) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    const cleanup = () => {
      req.off('data', onData)
      req.off('end', onEnd)
      req.off('error', onError)
      signal.removeEventListener('abort', onAbort)
    }
    const fail = (error) => { cleanup(); reject(error) }
    const onAbort = () => fail(signal.reason)
    const onError = (error) => fail(error)
    const onEnd = () => { cleanup(); resolve(Buffer.concat(chunks)) }
    const onData = (chunk) => {
      size += chunk.length
      if (size > limit) {
        req.pause()
        fail(Object.assign(new Error('request body too large'), { status: 413 }))
        return
      }
      chunks.push(chunk)
    }
    if (signal.aborted) return fail(signal.reason)
    if (Number(req.headers['content-length']) > limit) return fail(Object.assign(new Error('request body too large'), { status: 413 }))
    req.on('data', onData)
    req.once('end', onEnd)
    req.once('error', onError)
    signal.addEventListener('abort', onAbort, { once: true })
  })
}

function waitWithSignal(promise, signal) {
  if (signal.aborted) return Promise.reject(signal.reason)
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason)
    signal.addEventListener('abort', onAbort, { once: true })
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort))
  })
}

/* --------------------------------------------------------- rate + inflight */

const rateBuckets = new Map()
function rateLimitOk(ip) {
  if (!RATE_LIMIT_PER_MIN) return true
  const now = Date.now()
  let bucket = rateBuckets.get(ip)
  if (!bucket) {
    bucket = { tokens: RATE_LIMIT_PER_MIN, last: now }
    rateBuckets.set(ip, bucket)
  }
  bucket.tokens = Math.min(RATE_LIMIT_PER_MIN, bucket.tokens + ((now - bucket.last) / 60_000) * RATE_LIMIT_PER_MIN)
  bucket.last = now
  if (bucket.tokens < 1) return false
  bucket.tokens -= 1
  return true
}
setInterval(() => {
  const now = Date.now()
  for (const [ip, bucket] of rateBuckets) if (now - bucket.last > 600_000) rateBuckets.delete(ip)
}, 300_000).unref()

let inflight = 0
const slotWaiters = []
function reserveSlot() {
  inflight += 1
  metrics.inflight = inflight
  metrics.peakInflight = Math.max(metrics.peakInflight, inflight)
  let released = false
  return () => {
    if (released) return
    released = true
    inflight = Math.max(0, inflight - 1)
    metrics.inflight = inflight
    while (slotWaiters.length && (!MAX_CONCURRENCY || inflight < MAX_CONCURRENCY)) {
      const waiter = slotWaiters.shift()
      waiter.cleanup()
      if (waiter.signal.aborted) waiter.reject(waiter.signal.reason)
      else waiter.resolve(reserveSlot())
    }
  }
}
function acquireSlot(signal) {
  if (signal.aborted) return Promise.reject(signal.reason)
  if (slotWaiters.length === 0 && (!MAX_CONCURRENCY || inflight < MAX_CONCURRENCY)) return Promise.resolve(reserveSlot())
  if (slotWaiters.length >= MAX_QUEUE) return Promise.reject(Object.assign(new Error('gateway busy: request queue is full'), { status: 429 }))
  return new Promise((resolve, reject) => {
    const waiter = { resolve, reject, signal, timer: null, cleanup: null }
    const remove = (error) => {
      const index = slotWaiters.indexOf(waiter)
      if (index !== -1) slotWaiters.splice(index, 1)
      waiter.cleanup()
      reject(error)
    }
    const onAbort = () => remove(signal.reason)
    waiter.cleanup = () => {
      clearTimeout(waiter.timer)
      signal.removeEventListener('abort', onAbort)
    }
    waiter.timer = setTimeout(() => {
      remove(Object.assign(new Error('gateway busy: too many concurrent requests'), { status: 429 }))
    }, QUEUE_WAIT_MS)
    signal.addEventListener('abort', onAbort, { once: true })
    slotWaiters.push(waiter)
  })
}

/* --------------------------------------------------------------- upstream */

function upstreamHeaders(token, extra) {
  const headers = { Authorization: 'Bearer ' + token }
  if (extra) Object.assign(headers, extra)
  return headers
}

async function fetchUpstream(url, init) {
  metrics.upstreamCalls += 1
  const response = await fetch(url, init)
  metrics.upstreamStatus.set(String(response.status), (metrics.upstreamStatus.get(String(response.status)) ?? 0) + 1)
  if (!response.ok) metrics.upstreamErrors += 1
  return response
}

/* ------------------------------------------------------------ /v1/models */

let modelsCache = null
let modelsRefresh = null

async function loadModels() {
  const now = Date.now()
  if (modelsCache && now - modelsCache.at < MODELS_TTL_MS) {
    metrics.modelsCacheHits += 1
    return modelsCache
  }
  if (!modelsRefresh) {
    metrics.modelsCacheMisses += 1
    modelsRefresh = (async () => {
      const token = await accessToken(false)
      const upstream = await fetchUpstream(API_BASE + '/models', {
        headers: upstreamHeaders(token),
        signal: AbortSignal.timeout(MODELS_TIMEOUT_MS),
      })
      const text = await upstream.text()
      if (!upstream.ok) throw Object.assign(new Error('models upstream HTTP ' + upstream.status), { status: 502 })
      const body = Buffer.from(text)
      const etag = '"' + createHash('sha256').update(body).digest('hex').slice(0, 32) + '"'
      modelsCache = { body, etag, at: Date.now() }
      log('info', 'models-cached', { bytes: body.length, ttlSeconds: Math.round(MODELS_TTL_MS / 1000) })
      return modelsCache
    })().catch((error) => {
      metrics.modelsRefreshFailures += 1
      const ageMs = modelsCache ? Date.now() - modelsCache.at : Infinity
      if (modelsCache && ageMs <= MODELS_MAX_STALE_MS) {
        log('warn', 'models-refresh-failed-using-cache', { ageMs, message: String(error.message).slice(0, 200) })
        return { ...modelsCache, stale: true }
      }
      throw error
    }).finally(() => { modelsRefresh = null })
  }
  return modelsRefresh
}

async function handleModels(req, res) {
  metrics.models += 1
  const entry = await loadModels()
  if (entry.stale) metrics.modelsStaleResponses += 1
  const cacheHeaders = {
    etag: entry.etag,
    'cache-control': entry.stale ? 'private, max-age=0, must-revalidate' : 'private, max-age=60',
    'x-siwc-models-cache': entry.stale ? 'stale' : 'fresh',
  }
  if (req.headers['if-none-match'] === entry.etag) {
    res.writeHead(304, cacheHeaders)
    return res.end()
  }
  res.writeHead(200, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': entry.body.length,
    ...cacheHeaders,
  })
  res.end(entry.body)
}

/* --------------------------------------------------------- /v1/responses */

async function openUpstreamStream(parsed, originalHeaders, controller) {
  const headers = { 'Content-Type': 'application/json', Accept: 'text/event-stream' }
  for (const name of FORWARD_HEADERS) {
    const value = originalHeaders[name]
    if (typeof value === 'string' && value.length > 0) headers[name] = value
  }
  const token = await waitWithSignal(accessToken(false), controller.signal)
  const send = (accessTokenValue) =>
    fetchUpstream(API_BASE + '/responses', {
      method: 'POST',
      headers: upstreamHeaders(accessTokenValue, headers),
      body: JSON.stringify(parsed),
      signal: controller.signal,
    })
  let upstream = await send(token)
  if (upstream.status === 401) {
    log('warn', 'upstream-401-refresh-retry', {})
    await upstream.body?.cancel()
    const fresh = await waitWithSignal(accessToken(true, token), controller.signal)
    upstream = await send(fresh)
  }
  return upstream
}

async function handleResponses(req, res, ip, reqId) {
  metrics.responses += 1
  const startedAt = Date.now()
  const controller = new AbortController()
  let clientGone = false
  let release = null
  let bodyTimer = null
  let headersTimer = null
  const onClientGone = () => {
    if (!res.writableEnded) {
      clientGone = true
      controller.abort(Object.assign(new Error('client disconnected'), { code: 'CLIENT_GONE' }))
    }
  }
  req.once('aborted', onClientGone)
  res.on('close', onClientGone)

  try {
    // Reserve capacity before buffering/parsing the body. Waiting requests stay
    // paused in Node's bounded stream buffers rather than each holding 8 MiB.
    release = await acquireSlot(controller.signal)
    if (controller.signal.aborted) throw controller.signal.reason
    bodyTimer = setTimeout(() => {
      controller.abort(Object.assign(new Error('request body timeout'), { code: 'BODY_TIMEOUT', status: 408 }))
    }, BODY_TIMEOUT_MS)
    const raw = await readBody(req, MAX_BODY_BYTES, controller.signal)
    clearTimeout(bodyTimer)
    bodyTimer = null
    let parsed
    try {
      parsed = JSON.parse(raw.toString('utf8'))
    } catch {
      return json(res, 400, { error: { message: 'invalid JSON body' } })
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return json(res, 400, { error: { message: 'request body must be a JSON object' } })
    }
    for (const field of UNSUPPORTED) delete parsed[field]
    parsed.store = false
    parsed.stream = true

    headersTimer = setTimeout(() => {
      controller.abort(Object.assign(new Error('upstream headers timeout'), { code: 'HEADERS_TIMEOUT' }))
    }, UPSTREAM_HEADERS_TIMEOUT_MS)
    const upstream = await openUpstreamStream(parsed, req.headers, controller)
    clearTimeout(headersTimer)
    headersTimer = null
    log('info', 'upstream-opened', {
      reqId,
      model: parsed.model,
      status: upstream.status,
      connectMs: Date.now() - startedAt,
      bytesIn: raw.length,
    })

    if (!upstream.ok && upstream.status !== 200) {
      const text = await upstream.text().catch(() => '')
      let payload = text
      try {
        payload = JSON.parse(text)
      } catch {
        payload = { error: { message: text.slice(0, 2000) } }
      }
      const retryAfter = upstream.headers.get('retry-after')
      const headers = retryAfter ? { 'retry-after': retryAfter } : undefined
      return json(res, upstream.status, payload, headers)
    }

    res.writeHead(upstream.status, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      'x-accel-buffering': 'no',
    })
    if (typeof res.flushHeaders === 'function') res.flushHeaders()
    metrics.activeStreams += 1
    let bytes = 0
    let idleTimer = setTimeout(() => {
      controller.abort(Object.assign(new Error('stream idle timeout'), { code: 'IDLE_TIMEOUT' }))
    }, SSE_IDLE_TIMEOUT_MS)
    try {
      for await (const chunk of upstream.body) {
        bytes += chunk.length
        metrics.sseBytes += chunk.length
        clearTimeout(idleTimer)
        idleTimer = setTimeout(() => {
          controller.abort(Object.assign(new Error('stream idle timeout'), { code: 'IDLE_TIMEOUT' }))
        }, SSE_IDLE_TIMEOUT_MS)
        if (!res.write(chunk)) await once(res, 'drain', { signal: controller.signal })
      }
    } finally {
      clearTimeout(idleTimer)
      metrics.activeStreams = Math.max(0, metrics.activeStreams - 1)
    }
    res.end()
    log('info', 'stream-complete', { reqId, bytes, ms: Date.now() - startedAt })
  } catch (error) {
    const code = (error && error.code) || ''
    if (clientGone || code === 'CLIENT_GONE') {
      log('warn', 'client-disconnected', { reqId, ms: Date.now() - startedAt })
      return
    }
    log('error', 'responses-failed', {
      reqId,
      ms: Date.now() - startedAt,
      code,
      message: String((error && error.message) || error).slice(0, 400),
    })
    if (!res.headersSent && !res.destroyed) {
      const status = Number(error && error.status) || 502
      const headers = status === 429 ? { 'retry-after': '1', connection: 'close' } : (status === 413 || status === 408) ? { connection: 'close' } : undefined
      const message = (error && error.status ? '' : 'upstream failed: ') + String((error && error.message) || error).slice(0, 300)
      json(res, status, { error: { message } }, headers)
    } else res.destroy()
  } finally {
    clearTimeout(bodyTimer)
    clearTimeout(headersTimer)
    req.off('aborted', onClientGone)
    res.off('close', onClientGone)
    release?.()
  }
}

/* ---------------------------------------------------------------- metrics */

function metricsText() {
  const lines = []
  const counter = (name, help, value) => {
    lines.push('# HELP ' + name + ' ' + help)
    lines.push('# TYPE ' + name + ' counter')
    lines.push(name + ' ' + value)
  }
  const gauge = (name, help, value) => {
    lines.push('# HELP ' + name + ' ' + help)
    lines.push('# TYPE ' + name + ' gauge')
    lines.push(name + ' ' + value)
  }
  counter('siwc_gateway_requests_total', 'HTTP requests handled', metrics.models + metrics.responses + metrics.health)
  counter('siwc_gateway_models_requests_total', 'Catalog requests', metrics.models)
  counter('siwc_gateway_responses_requests_total', 'Responses requests', metrics.responses)
  counter('siwc_gateway_upstream_calls_total', 'Upstream API calls', metrics.upstreamCalls)
  counter('siwc_gateway_upstream_errors_total', 'Upstream API errors', metrics.upstreamErrors)
  counter('siwc_gateway_token_refreshes_total', 'Token refreshes', metrics.refreshes)
  counter('siwc_gateway_token_refresh_failures_total', 'Token refresh failures', metrics.refreshFailures)
  counter('siwc_gateway_models_cache_hits_total', 'Model catalog cache hits', metrics.modelsCacheHits)
  counter('siwc_gateway_models_cache_misses_total', 'Model catalog cache misses', metrics.modelsCacheMisses)
  counter('siwc_gateway_models_refresh_failures_total', 'Failed model catalog refreshes', metrics.modelsRefreshFailures)
  counter('siwc_gateway_models_stale_responses_total', 'Catalog responses using stale cache', metrics.modelsStaleResponses)
  counter('siwc_gateway_sse_bytes_total', 'SSE bytes relayed', metrics.sseBytes)
  gauge('siwc_gateway_inflight_requests', 'In-flight responses requests', metrics.inflight)
  gauge('siwc_gateway_queued_requests', 'Responses requests waiting for capacity', slotWaiters.length)
  gauge('siwc_gateway_active_streams', 'Active SSE streams', metrics.activeStreams)
  gauge('siwc_gateway_uptime_seconds', 'Uptime seconds', Math.round((Date.now() - metrics.startedAt) / 1000))
  const access = record ? Math.max(0, Math.round((record.expiresAt - Date.now()) / 1000)) : 0
  gauge('siwc_gateway_access_token_valid_seconds', 'Seconds until the access token expires', access)
  return lines.join('\n') + '\n'
}

/* ----------------------------------------------------------------- server */

const activeSockets = new Set()
const server = createServer((req, res) => {
  const startedAt = Date.now()
  const reqId = randomUUID().slice(0, 8)
  const ip = clientIp(req)
  let pathname = '/'
  try {
    pathname = new URL(req.url, 'http://localhost').pathname
  } catch {}
  const finish = () => {
    const status = res.statusCode
    metrics.byStatus.set(String(status), (metrics.byStatus.get(String(status)) ?? 0) + 1)
    if (pathname !== '/metrics') {
      log('info', 'request', { reqId, ip, method: req.method, path: pathname, status, ms: Date.now() - startedAt })
    }
  }
  res.on('finish', finish)
  res.on('close', () => {
    if (!res.writableFinished) finish()
  })

  const dispatch = async () => {
    if (pathname === '/healthz') {
      metrics.health += 1
      ensureRecord()
      return json(res, 200, {
        ok: true,
        version: VERSION,
        upstream: API_BASE,
        account: record ? String(record.email ?? '').replace(/^(.).*(@.*)$/, '$1***$2') : null,
        accessTokenExpiresAt: record ? new Date(record.expiresAt).toISOString() : null,
        modelsCached: modelsCache ? new Date(modelsCache.at).toISOString() : null,
        inflight,
      })
    }
    if (req.method === 'GET' && pathname === '/metrics') {
      const body = Buffer.from(metricsText())
      res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4; charset=utf-8', 'content-length': body.length })
      return res.end(body)
    }
    if (!authorized(req)) {
      log('warn', 'unauthorized', { reqId, ip, path: pathname })
      return json(res, 401, { error: { message: 'unauthorized' } })
    }
    if (!rateLimitOk(ip)) {
      log('warn', 'rate-limited', { reqId, ip, path: pathname })
      return json(res, 429, { error: { message: 'rate limit exceeded, retry later' } }, { 'retry-after': '60' })
    }
    if (req.method === 'GET' && (pathname === '/v1/models' || pathname === '/models')) {
      return handleModels(req, res)
    }
    if (req.method === 'POST' && (pathname === '/v1/responses' || pathname === '/responses')) {
      return handleResponses(req, res, ip, reqId)
    }
    return json(res, 404, { error: { message: 'not found: ' + req.method + ' ' + pathname } })
  }

  dispatch().catch((error) => {
    log('error', 'dispatch-failed', { reqId, path: pathname, message: String((error && error.message) || error).slice(0, 300) })
    if (!res.headersSent) {
      const status = Number(error && error.status) || 502
      json(res, status, { error: { message: String((error && error.message) || error).slice(0, 300) } })
    } else {
      res.destroy()
    }
  })
})

server.keepAliveTimeout = 70_000
server.headersTimeout = 75_000
server.on('connection', (socket) => {
  socket.setNoDelay(true)
  socket.setKeepAlive(true, 30_000)
  activeSockets.add(socket)
  socket.on('close', () => activeSockets.delete(socket))
})
server.on('clientError', (error, socket) => {
  log('warn', 'client-error', { message: String((error && error.message) || error).slice(0, 200) })
  if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n')
})

let shuttingDown = false
function shutdown(signal) {
  if (shuttingDown) return
  shuttingDown = true
  log('info', 'shutdown', { signal, inflight })
  server.close(() => {
    log('info', 'shutdown-complete', {})
    process.exit(0)
  })
  setTimeout(() => {
    log('warn', 'shutdown-forced', {})
    for (const socket of activeSockets) socket.destroy()
    process.exit(0)
  }, 15_000).unref()
}
process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))
process.on('uncaughtException', (error) => {
  log('error', 'uncaught-exception', { message: String((error && error.stack) || error).slice(0, 1200) })
  process.exit(1)
})
process.on('unhandledRejection', (reason) => {
  log('error', 'unhandled-rejection', { message: String((reason && reason.stack) || reason).slice(0, 1200) })
})

ensureRecord()
if (record) log('info', 'credential-loaded', { account: record.email, file: credFile, expiresAt: new Date(record.expiresAt).toISOString() })
else log('warn', 'no-credential', { dir: CRED_DIR })
mkdirSync(CRED_DIR, { recursive: true })
server.listen(PORT, HOST, () => {
  log('info', 'listening', {
    version: VERSION,
    url: 'http://' + HOST + ':' + server.address().port,
    upstream: API_BASE,
    auth: GATEWAY_TOKEN ? 'bearer' : 'DISABLED',
    maxConcurrency: MAX_CONCURRENCY,
    maxQueue: MAX_QUEUE,
    rateLimitPerMin: RATE_LIMIT_PER_MIN,
  })
})
