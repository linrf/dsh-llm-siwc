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
import { existsSync, readFileSync, readdirSync, writeFileSync, renameSync, mkdirSync, rmSync, lstatSync } from 'node:fs'
import { mkdir, mkdtemp, rm, statfs, open } from 'node:fs/promises'
import { Transform, Writable, Readable } from 'node:stream'
import { createStreamingNormalizer } from './streaming-json.mjs'
import { pipeline, finished } from 'node:stream/promises'
import * as zlib from 'node:zlib'
import { join } from 'node:path'
import { once } from 'node:events'

const VERSION = '2.3.0'
const STORE_DIR = process.env.SIWC_STORE ?? join(process.env.HOME ?? '/root', '.dsh', 'siwc')
const CRED_DIR = join(STORE_DIR, 'credentials')
const PORT = Number(process.env.PORT ?? 1457)
const HOST = process.env.HOST ?? '127.0.0.1'
const API_BASE = (process.env.API_BASE ?? 'https://api.openai.com/v1').replace(/\/+$/, '')
const GATEWAY_TOKEN = process.env.GATEWAY_TOKEN ?? ''
const MAX_CONCURRENCY = Number(process.env.MAX_RESPONSE_CONCURRENCY ?? process.env.MAX_CONCURRENCY ?? 8)
const MAX_UPLOAD_CONCURRENCY = Number(process.env.MAX_UPLOAD_CONCURRENCY ?? 2)
const MAX_UPLOAD_QUEUE = Number(process.env.MAX_UPLOAD_QUEUE ?? 8)
const UPLOAD_QUEUE_WAIT_MS = Number(process.env.UPLOAD_QUEUE_WAIT_MS ?? 30_000)
const UPSTREAM_UPLOAD_TIMEOUT_MS = Number(process.env.UPSTREAM_UPLOAD_TIMEOUT_MS ?? 300_000)
const MAX_QUEUE = Number(process.env.MAX_QUEUE ?? 8)
const QUEUE_WAIT_MS = Number(process.env.QUEUE_WAIT_MS ?? 30_000)
const RATE_LIMIT_PER_MIN = Number(process.env.RATE_LIMIT_PER_MIN ?? 240)
const MODELS_TTL_MS = Number(process.env.MODELS_TTL_MS ?? 300_000)
const MODELS_TIMEOUT_MS = Number(process.env.MODELS_TIMEOUT_MS ?? 30_000)
const MODELS_MAX_STALE_MS = Number(process.env.MODELS_MAX_STALE_MS ?? 30 * 60_000)
const BODY_IDLE_TIMEOUT_MS = Number(process.env.BODY_IDLE_TIMEOUT_MS ?? process.env.BODY_TIMEOUT_MS ?? 30_000)
const BODY_TOTAL_TIMEOUT_MS = Number(process.env.BODY_TOTAL_TIMEOUT_MS ?? 300_000)
const UPSTREAM_HEADERS_TIMEOUT_MS = Number(process.env.UPSTREAM_HEADERS_TIMEOUT_MS ?? 180_000)
const UPSTREAM_ERROR_BODY_TIMEOUT_MS = Number(process.env.UPSTREAM_ERROR_BODY_TIMEOUT_MS ?? 10_000)
const MAX_UPSTREAM_ERROR_BODY_BYTES = 64 * 1024
const SSE_IDLE_TIMEOUT_MS = Number(process.env.SSE_IDLE_TIMEOUT_MS ?? 300_000)
const MAX_WIRE_BODY_BYTES = Number(process.env.MAX_WIRE_BODY_BYTES ?? process.env.MAX_BODY_BYTES ?? 64 * 1024 * 1024)
const MAX_DECODED_BODY_BYTES = Number(process.env.MAX_DECODED_BODY_BYTES ?? process.env.MAX_BODY_BYTES ?? 64 * 1024 * 1024)
const MAX_NORMALIZED_BODY_BYTES = MAX_DECODED_BODY_BYTES + 64 * 1024
const MAX_SPOOL_BYTES = Number(process.env.MAX_SPOOL_BYTES ?? MAX_CONCURRENCY * MAX_NORMALIZED_BODY_BYTES)
const MIN_SPOOL_FREE_BYTES = Number(process.env.MIN_SPOOL_FREE_BYTES ?? 128 * 1024 * 1024)
// STORE_DIR is on the service's writable disk. Do not use /tmp: it may be tmpfs.
const SPOOL_DIR = process.env.SPOOL_DIR ?? join(STORE_DIR, 'request-spool')
const ZSTD_WINDOW_LOG_MAX = Number(process.env.ZSTD_WINDOW_LOG_MAX ?? 23)
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
  requestWireBytes: 0,
  requestDecodedBytes: 0,
  requestNormalizedBytes: 0,
  normalizerPeakRssBytes: 0,
  normalizersActive: 0,
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

function requestFailure(message, status, code) {
  return Object.assign(new Error(message), { status, code })
}

function requestEncoding(req) {
  const encoding = String(req.headers['content-encoding'] ?? 'identity').trim().toLowerCase()
  if (!['identity', 'gzip', 'zstd'].includes(encoding)) throw requestFailure('unsupported request content encoding', 415, 'CONTENT_ENCODING')
  if (encoding === 'zstd' && (typeof zlib.createZstdDecompress !== 'function' || zlib.constants.ZSTD_d_windowLogMax === undefined)) throw requestFailure('zstd decoding is unavailable in this Node runtime', 415, 'ZSTD_UNAVAILABLE')
  if (Number(req.headers['content-length']) > MAX_WIRE_BODY_BYTES) throw requestFailure('encoded request exceeds byte limit', 413, 'WIRE_LIMIT')
  return encoding
}

let spoolReservedBytes = 0
const activeSpools = new Set()
const activeControllers = new Set()
const activeUploads = new Set()

async function reserveSpool(signal) {
  await mkdir(SPOOL_DIR, { recursive: true, mode: 0o700 })
  const disk = await statfs(SPOOL_DIR)
  signal.throwIfAborted()
  const reservation = MAX_NORMALIZED_BODY_BYTES
  if (spoolReservedBytes + reservation > MAX_SPOOL_BYTES) throw requestFailure('request spool capacity is busy', 503, 'SPOOL_CAPACITY')
  if (disk.bavail * disk.bsize - spoolReservedBytes < MIN_SPOOL_FREE_BYTES + reservation) throw requestFailure('insufficient request spool disk space', 507, 'SPOOL_DISK')
  spoolReservedBytes += reservation
  let dir
  try {
    dir = await mkdtemp(join(SPOOL_DIR, 'request-'))
    writeFileSync(join(dir, 'owner.json'), JSON.stringify({ pid: process.pid }), { flag: 'wx', mode: 0o600 })
    activeSpools.add(dir)
    let cleaned = false
    let cleaning = null
    return {
      dir,
      decoded: join(dir, 'decoded.json'),
      normalized: join(dir, 'normalized.json'),
      cleanup() {
        if (cleaned) return Promise.resolve()
        if (!cleaning) cleaning = rm(dir, { recursive: true, force: true }).then(() => {
          cleaned = true
          activeSpools.delete(dir)
          spoolReservedBytes -= reservation
        }).finally(() => { cleaning = null })
        return cleaning
      },
    }
  } catch (error) {
    spoolReservedBytes -= reservation
    throw error
  }
}

async function cleanupSpoolSafely(spool, reqId) {
  try { await spool.cleanup(); return true } catch {
    log('error', 'request-spool-cleanup-failed', { reqId })
    return false
  }
}

function cleanupStaleSpool() {
  mkdirSync(SPOOL_DIR, { recursive: true, mode: 0o700 })
  let removed = 0
  for (const name of readdirSync(SPOOL_DIR)) {
    if (!/^request-[a-z0-9]{6}$/i.test(name)) continue
    const dir = join(SPOOL_DIR, name)
    const stat = lstatSync(dir)
    if (!stat.isDirectory() || (stat.mode & 0o077) !== 0 || (process.getuid && stat.uid !== process.getuid())) continue
    // Only our fixed files in an owned private directory may be removed.
    if (readdirSync(dir).some((entry) => !['owner.json', 'decoded.json', 'normalized.json'].includes(entry))) continue
    let owner
    try { owner = JSON.parse(readFileSync(join(dir, 'owner.json'), 'utf8')) } catch {}
    if (Number.isSafeInteger(owner?.pid) && owner.pid > 0) {
      try { process.kill(owner.pid, 0); continue } catch (error) { if (error.code !== 'ESRCH') continue }
    }
    rmSync(dir, { recursive: true, force: true })
    removed += 1
  }
  if (removed) log('info', 'stale-request-spool-removed', { directories: removed })
}

// One producer, independent readers per attempt. Only committed disk bytes are
// visible to a tail. Retain the final byte until *all* JSON/codec checks succeed.
// This permits overlap without submitting a complete invalid request upstream.
async function startBody(req, spool, encoding, controller) {
  const startedAt = Date.now()
  const file = await open(spool.normalized, 'wx', 0o600)
  const body = { path: spool.normalized, bytes: 0, wireBytes: 0, decodedBytes: 0, complete: false, error: null, uploads: new Set(), waiters: new Set() }
  const notify = () => { for (const wake of body.waiters) wake(); body.waiters.clear() }
  body.wait = (signal) => {
    signal.throwIfAborted()
    return new Promise((resolve, reject) => {
      const wake = () => { signal.removeEventListener('abort', abort); body.waiters.delete(wake); resolve() }
      const abort = () => { body.waiters.delete(wake); reject(signal.reason) }
      body.waiters.add(wake)
      signal.addEventListener('abort', abort, { once: true })
    })
  }
  let idleTimer
  let wireMeter
  const clearIdle = () => { clearTimeout(idleTimer); idleTimer = null }
  const armIdle = () => {
    clearIdle()
    // readableFlowing=false means our disk/decoder backpressure paused req.
    // Waiting on our own processing must not be blamed on the client.
    if (!req.readableEnded && req.readableFlowing === true) idleTimer = setTimeout(() => controller.abort(requestFailure('request upload idle timeout', 408, 'BODY_IDLE_TIMEOUT')), BODY_IDLE_TIMEOUT_MS)
  }
  req.on('pause', clearIdle)
  req.on('resume', armIdle)
  req.on('end', clearIdle)
  const totalTimer = setTimeout(() => controller.abort(requestFailure('request upload total timeout', 408, 'BODY_TOTAL_TIMEOUT')), BODY_TOTAL_TIMEOUT_MS)
  wireMeter = new Transform({ transform(chunk, _encoding, done) {
    armIdle()
    body.wireBytes += chunk.length
    done(body.wireBytes > MAX_WIRE_BODY_BYTES ? requestFailure('encoded request exceeds byte limit', 413, 'WIRE_LIMIT') : null, chunk)
  } })
  const decodedMeter = new Transform({ transform(chunk, _encoding, done) {
    body.decodedBytes += chunk.length
    done(body.decodedBytes > MAX_DECODED_BODY_BYTES ? requestFailure('decoded request exceeds byte limit', 413, 'DECODED_LIMIT') : null, chunk)
  } })
  const decoder = encoding === 'gzip' ? zlib.createGunzip() : encoding === 'zstd' ? zlib.createZstdDecompress({ params: { [zlib.constants.ZSTD_d_windowLogMax]: ZSTD_WINDOW_LOG_MAX } }) : null
  const normalizer = createStreamingNormalizer({ unsupported: UNSUPPORTED, forbidden: ['previous_response_id'], maxBytes: MAX_NORMALIZED_BODY_BYTES })
  const writer = new Writable({ write(chunk, _encoding, done) {
    ;(async () => {
      let offset = 0
      while (offset < chunk.length) {
        const { bytesWritten } = await file.write(chunk, offset, chunk.length - offset)
        if (!bytesWritten) throw new Error('short request spool write')
        offset += bytesWritten
      }
      body.bytes += chunk.length
      notify()
    })().then(() => done(), done)
  } })
  metrics.normalizersActive += 1
  body.done = (async () => {
    try {
      // Keep req outside pipeline to return controlled validation errors.
      const work = pipeline(wireMeter, ...(decoder ? [decoder] : []), decodedMeter, normalizer, writer, { signal: controller.signal })
      req.pipe(wireMeter)
      await work
      if (encoding === 'zstd' && decoder.bytesWritten !== body.wireBytes) throw requestFailure('zstd request contains trailing data or multiple frames', 400, 'ZSTD_TRAILING_DATA')
      await file.close()
      body.complete = true
      body.uploadMs = Date.now() - startedAt
      body.model = normalizer.model
      notify()
      return body
    } catch (error) {
      if (controller.signal.aborted) error = controller.signal.reason
      else if (!error.status) error = error.code === 'ENOSPC' || error.code === 'EDQUOT'
        ? requestFailure('request spool disk is full', 507, 'SPOOL_DISK')
        : requestFailure(encoding === 'identity' ? 'request body could not be received' : 'invalid compressed request body', 400, 'BODY_DECODE')
      body.error = error
      notify()
      controller.abort(error)
      throw error
    } finally {
      clearIdle()
      clearTimeout(totalTimer)
      req.off('pause', clearIdle); req.off('resume', armIdle); req.off('end', clearIdle)
      req.unpipe(wireMeter)
      req.pause()
      await file.close().catch(() => {})
      metrics.normalizersActive -= 1
      metrics.requestWireBytes += body.wireBytes
      metrics.requestDecodedBytes += body.decodedBytes
      metrics.requestNormalizedBytes += body.bytes
    }
  })()
  body.done.catch(() => {})
  return body
}

function tailBody(body, signal) {
  const stream = Readable.from((async function* () {
    const file = await open(body.path, 'r')
    let offset = 0
    try {
      while (true) {
        signal.throwIfAborted()
        if (body.error) throw body.error
        const frontier = body.complete ? body.bytes : Math.max(0, body.bytes - 1)
        if (offset < frontier) {
          const buffer = Buffer.allocUnsafe(Math.min(64 * 1024, frontier - offset))
          const { bytesRead } = await file.read(buffer, 0, buffer.length, offset)
          if (!bytesRead) throw new Error('request spool unexpectedly ended')
          offset += bytesRead
          yield buffer.subarray(0, bytesRead)
        } else if (body.complete) return
        else await body.wait(signal)
      }
    } finally { await file.close() }
  })(), { objectMode: false, highWaterMark: 64 * 1024, signal })
  const upload = { stream, done: finished(stream, { cleanup: true }) }
  upload.done.catch(() => {})
  body.uploads.add(upload)
  activeUploads.add(stream)
  stream.once('close', () => { body.uploads.delete(upload); activeUploads.delete(stream) })
  return upload
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
function createGate(maximum, maxWaiting, timeoutMs, changed = () => {}) {
  let active = 0
  const waiters = []
  const reserve = () => {
    active += 1
    changed(active)
    let released = false
    return () => {
      if (released) return
      released = true
      active -= 1
      changed(active)
      while (waiters.length && (!maximum || active < maximum)) {
        const waiter = waiters.shift()
        waiter.cleanup()
        if (waiter.signal.aborted) waiter.reject(waiter.signal.reason)
        else waiter.resolve(reserve())
      }
    }
  }
  return {
    get active() { return active },
    get queued() { return waiters.length },
    acquire(signal) {
      if (signal.aborted) return Promise.reject(signal.reason)
      if (waiters.length === 0 && (!maximum || active < maximum)) return Promise.resolve(reserve())
      if (waiters.length >= maxWaiting) return Promise.reject(requestFailure('gateway busy: request queue is full', 429, 'QUEUE_FULL'))
      return new Promise((resolve, reject) => {
        const waiter = { resolve, reject, signal, timer: null, cleanup: null }
        const remove = (error) => {
          const index = waiters.indexOf(waiter)
          if (index !== -1) waiters.splice(index, 1)
          waiter.cleanup()
          reject(error)
        }
        const onAbort = () => remove(signal.reason)
        waiter.cleanup = () => { clearTimeout(waiter.timer); signal.removeEventListener('abort', onAbort) }
        waiter.timer = setTimeout(() => remove(requestFailure('gateway busy: queue wait timeout', 429, 'QUEUE_TIMEOUT')), timeoutMs)
        signal.addEventListener('abort', onAbort, { once: true })
        waiters.push(waiter)
      })
    },
  }
}
const requestGate = createGate(MAX_CONCURRENCY, MAX_QUEUE, QUEUE_WAIT_MS, (active) => {
  inflight = active
  metrics.inflight = active
  metrics.peakInflight = Math.max(metrics.peakInflight, active)
})
const uploadGate = createGate(MAX_UPLOAD_CONCURRENCY, MAX_UPLOAD_QUEUE, UPLOAD_QUEUE_WAIT_MS)

/* --------------------------------------------------------------- upstream */

function upstreamHeaders(token, extra) {
  const headers = { Authorization: 'Bearer ' + token }
  if (extra) Object.assign(headers, extra)
  return headers
}

async function fetchUpstream(url, init) {
  metrics.upstreamCalls += 1
  try {
    const response = await fetch(url, init)
    metrics.upstreamStatus.set(String(response.status), (metrics.upstreamStatus.get(String(response.status)) ?? 0) + 1)
    if (!response.ok) metrics.upstreamErrors += 1
    return response
  } catch (error) {
    metrics.upstreamErrors += 1
    throw error
  }
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

async function readUpstreamErrorBody(upstream, controller, reqId) {
  if (!upstream.body) return ''
  const reader = upstream.body.getReader()
  const chunks = []
  let bytes = 0
  let timedOut = false
  const payload = (message) => JSON.stringify({ error: { message } })
  const timer = setTimeout(() => {
    timedOut = true
    controller.abort(requestFailure('upstream error body timed out', 502, 'UPSTREAM_ERROR_BODY_TIMEOUT'))
  }, UPSTREAM_ERROR_BODY_TIMEOUT_MS)
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) return Buffer.concat(chunks, bytes).toString('utf8')
      bytes += value.byteLength
      if (bytes > MAX_UPSTREAM_ERROR_BODY_BYTES) {
        log('warn', 'upstream-error-body-limited', { reqId, status: upstream.status, bytes })
        controller.abort(requestFailure('upstream error body exceeds byte limit', 502, 'UPSTREAM_ERROR_BODY_LIMIT'))
        await reader.cancel().catch(() => {})
        return payload('upstream error body exceeded the 64KiB safety limit')
      }
      chunks.push(value)
    }
  } catch (error) {
    if (timedOut) {
      log('warn', 'upstream-error-body-timeout', { reqId, status: upstream.status, bytes })
      return payload('upstream error body timed out')
    }
    if (controller.signal.aborted) throw controller.signal.reason
    return payload('upstream error body could not be read')
  } finally {
    clearTimeout(timer)
    reader.releaseLock()
  }
}

async function openUpstreamStream(body, originalHeaders, controller, firstRelease) {
  const headers = { 'Content-Type': 'application/json', Accept: 'text/event-stream' }
  for (const name of FORWARD_HEADERS) {
    const value = originalHeaders[name]
    if (typeof value === 'string' && value.length > 0) headers[name] = value
  }
  const token = await waitWithSignal(accessToken(false), controller.signal)
  const send = async (accessTokenValue, release, replay = false) => {
    const attempt = new AbortController()
    const signal = AbortSignal.any([controller.signal, attempt.signal])
    const upload = tailBody(body, signal)
    let headersReceived = false
    let headersTimer
    const uploadTimer = setTimeout(() => controller.abort(requestFailure('upstream upload timeout', 504, 'UPSTREAM_UPLOAD_TIMEOUT')), UPSTREAM_UPLOAD_TIMEOUT_MS)
    const clearTimers = () => { clearTimeout(uploadTimer); clearTimeout(headersTimer) }
    signal.addEventListener('abort', clearTimers, { once: true })
    const uploaded = Promise.allSettled([body.done, upload.done]).then((results) => {
      clearTimeout(uploadTimer)
      if (results.some((result) => result.status === 'rejected') || headersReceived || signal.aborted) return
      headersTimer = setTimeout(() => controller.abort(requestFailure('upstream headers timeout', 504, 'HEADERS_TIMEOUT')), UPSTREAM_HEADERS_TIMEOUT_MS)
    }).finally(release)
    uploaded.catch(() => {})
    let response
    try {
      response = await fetchUpstream(API_BASE + '/responses', {
        method: 'POST',
        headers: upstreamHeaders(accessTokenValue, replay ? { ...headers, 'Content-Length': String(body.bytes) } : headers),
        body: upload.stream,
        duplex: 'half',
        signal,
      })
      return { response, attempt, upload }
    } catch (error) {
      attempt.abort(error)
      upload.stream.destroy()
      if (controller.signal.aborted) throw controller.signal.reason
      if (signal.aborted && signal.reason?.status) throw signal.reason
      throw error
    } finally {
      headersReceived = true
      clearTimeout(headersTimer)
      // Do not destroy an upload when headers arrive. Node 22 can thereby
      // truncate the response. Caller consumes/cancels response first.
      // Prevent an EOF microtask from re-arming the headers timer afterwards.
      uploaded.finally(() => { clearTimers(); signal.removeEventListener('abort', clearTimers) }).catch(() => {})
    }
  }
  let current = await send(token, firstRelease)
  if (current.response.status === 401) {
    log('warn', 'upstream-401-refresh-retry', {})
    await current.response.body?.cancel().catch(() => {})
    current.attempt.abort()
    current.upload.stream.destroy()
    await current.upload.done.catch(() => {})
    // The failed attempt owns only its reader, never the shared producer.
    // Replay begins only once the complete, validated body is on disk.
    await body.done
    const fresh = await waitWithSignal(accessToken(true, token), controller.signal)
    const release = await uploadGate.acquire(controller.signal)
    current = await send(fresh, release, true)
  }
  return current.response
}

async function handleResponses(req, res, ip, reqId) {
  metrics.responses += 1
  const startedAt = Date.now()
  const controller = new AbortController()
  let clientGone = false
  let release = null
  let spool = null
  let normalized = null
  let uploadCleanup = null
  let releaseUpload = null
  const onClientGone = () => {
    if (!res.writableEnded) {
      clientGone = true
      controller.abort(Object.assign(new Error('client disconnected'), { code: 'CLIENT_GONE' }))
    }
  }
  activeControllers.add(controller)
  req.once('aborted', onClientGone)
  req.on('error', onClientGone)
  res.on('close', onClientGone)

  try {
    const encoding = requestEncoding(req)
    release = await requestGate.acquire(controller.signal)
    controller.signal.throwIfAborted()
    releaseUpload = await uploadGate.acquire(controller.signal)
    spool = await reserveSpool(controller.signal)
    normalized = await startBody(req, spool, encoding, controller)
    const firstRelease = releaseUpload
    normalized.done.then((body) => log('info', 'request-prepared', { reqId, encoding, wireBytes: body.wireBytes, decodedBytes: body.decodedBytes, normalizedBytes: body.bytes, uploadMs: body.uploadMs, streaming: true })).catch(() => {})
    const upstream = await openUpstreamStream(normalized, req.headers, controller, firstRelease)
    // Real inference consumes a complete request. Even an unusual early 200
    // must not bypass local JSON, codec integrity or size validation.
    if (upstream.ok) await normalized.done
    // A successful peer can start SSE before reading its complete request.
    // Close upload FDs before deleting files, without delaying early SSE. Error
    // responses keep their spool until their body is consumed in the branch below.
    if (upstream.ok) {
      const ownedSpool = spool
      uploadCleanup = Promise.all([...normalized.uploads].map((upload) => upload.done.catch(() => {}))).then(async () => {
        if (await cleanupSpoolSafely(ownedSpool, reqId) && spool === ownedSpool) spool = null
      })
      if (normalized.uploads.size === 0) await uploadCleanup
    }
    log('info', 'upstream-opened', {
      reqId,
      status: upstream.status,
      connectMs: Date.now() - startedAt,
      bytesIn: normalized.decodedBytes,
      normalizedBytes: normalized.bytes,
      model: normalized.model,
    })

    if (!upstream.ok && upstream.status !== 200) {
      const text = await readUpstreamErrorBody(upstream, controller, reqId)
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
    if (controller.signal.aborted && controller.signal.reason?.status) error = controller.signal.reason
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
      const headers = { connection: 'close', ...(status === 429 || status === 503 ? { 'retry-after': '1' } : {}) }
      const message = (error && error.status ? '' : 'upstream failed: ') + String((error && error.message) || error).slice(0, 300)
      json(res, status, { error: { message } }, headers)
    } else res.destroy()
  } finally {
    // A peer can finish an error or SSE response without reading our complete
    // request. Abort only after consuming that response, then await FD closure.
    controller.abort()
    if (normalized) await normalized.done.catch(() => {})
    if (normalized?.uploads) await Promise.all([...normalized.uploads].map((upload) => upload.done.catch(() => {})))
    if (uploadCleanup) await uploadCleanup
    activeControllers.delete(controller)
    req.off('aborted', onClientGone)
    req.off('error', onClientGone)
    res.off('close', onClientGone)
    try { if (spool) await cleanupSpoolSafely(spool, reqId) } finally { releaseUpload?.(); release?.() }
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
  gauge('siwc_gateway_queued_requests', 'Responses requests waiting for capacity', requestGate.queued)
  gauge('siwc_gateway_normalizer_active', 'Active streaming JSON validators', metrics.normalizersActive)
  gauge('siwc_gateway_upload_active', 'Active body receive or upstream upload stages', uploadGate.active)
  gauge('siwc_gateway_upload_queued', 'Requests waiting for an upload stage', uploadGate.queued)
  gauge('siwc_gateway_normalizer_queued', 'Streaming validators have no separate worker queue', 0)
  gauge('siwc_gateway_upstream_uploads_active', 'Open upstream request file streams', activeUploads.size)
  gauge('siwc_gateway_spool_reserved_bytes', 'Disk capacity reserved for requests', spoolReservedBytes)
  gauge('siwc_gateway_spool_directories', 'Active private request directories', activeSpools.size)
  gauge('siwc_gateway_normalizer_peak_rss_bytes', 'Deprecated: no JSON child workers since 2.3; always zero', metrics.normalizerPeakRssBytes)
  gauge('siwc_gateway_process_rss_bytes', 'Gateway process RSS', process.memoryUsage().rss)
  counter('siwc_gateway_request_wire_bytes_total', 'Encoded request bytes received', metrics.requestWireBytes)
  counter('siwc_gateway_request_decoded_bytes_total', 'Decoded request bytes received', metrics.requestDecodedBytes)
  counter('siwc_gateway_request_normalized_bytes_total', 'Normalized request bytes forwarded', metrics.requestNormalizedBytes)
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

// Queue, upload, normalization and upstream have their own explicit deadlines.
server.requestTimeout = 0
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
    for (const controller of activeControllers) controller.abort(requestFailure('gateway shutting down', 503, 'SHUTDOWN'))
    for (const socket of activeSockets) socket.destroy()
    for (const dir of activeSpools) { try { rmSync(dir, { recursive: true, force: true }) } catch {} }
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

cleanupStaleSpool()
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
    maxWireBodyBytes: MAX_WIRE_BODY_BYTES,
    maxDecodedBodyBytes: MAX_DECODED_BODY_BYTES,
    maxUploadConcurrency: MAX_UPLOAD_CONCURRENCY,
    rateLimitPerMin: RATE_LIMIT_PER_MIN,
  })
})
