import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createServer, request as httpRequest } from 'node:http'
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, writeFile, rm, readdir, stat } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { gzipSync, zstdCompressSync } from 'node:zlib'
import { Readable } from 'node:stream'
import { createServer as createTcpServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'

const gatewayFile = fileURLToPath(new URL('./siwc-gateway-v2.mjs', import.meta.url))
const gatewayToken = 'loopback-gateway-token'
const catalog = (slug) => JSON.stringify({ models: [{ slug, display_name: slug }] })
const sse = 'data: {"type":"response.output_text.delta","delta":"OK"}\n\ndata: [DONE]\n\n'

async function until(check, description, timeout = 2500) {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (await check()) return
    await delay(10)
  }
  assert.fail('timed out: ' + description)
}

async function fixture(t, { expired = false, env = {}, onToken, onApi, onApiRequest, prepareDir, extraOrigins = [] } = {}) {
  const dir = await mkdtemp(join(process.env.SIWC_TEST_TMPDIR ?? tmpdir(), 'siwc-http-test-'))
  let child
  let upstream
  let childExit
  t.after(async () => {
    upstream?.closeAllConnections()
    upstream?.close()
    if (child && child.exitCode === null) {
      child.kill('SIGTERM')
      const force = setTimeout(() => child.kill('SIGKILL'), 1000)
      await childExit
      clearTimeout(force)
    }
    await rm(dir, { recursive: true, force: true })
  })
  const calls = { token: 0, api: 0, models: 0, responses: 0 }
  upstream = createServer(async (req, res) => {
    try {
      if (req.url !== '/test-token' && onApiRequest) {
        calls.api += 1
        if (req.url === '/v1/models') calls.models += 1
        if (req.url === '/v1/responses') calls.responses += 1
        return await onApiRequest(req, res, calls)
      }
      const chunks = []
      for await (const chunk of req) chunks.push(chunk)
      const raw = Buffer.concat(chunks).toString()
      if (req.url === '/test-token') {
        calls.token += 1
        if (onToken) return await onToken(req, res, raw, calls)
        res.writeHead(200, { 'content-type': 'application/json' })
        return res.end(JSON.stringify({ access_token: 'new-access-token', refresh_token: 'rotated-refresh-token', expires_in: 3600 }))
      }
      calls.api += 1
      if (req.url === '/v1/models') calls.models += 1
      if (req.url === '/v1/responses') calls.responses += 1
      if (onApi) return await onApi(req, res, raw, calls)
      if (req.url === '/v1/models') {
        res.writeHead(200, { 'content-type': 'application/json' })
        return res.end(catalog('fixture-model'))
      }
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      res.end(sse)
    } catch (error) {
      if (!res.headersSent) res.writeHead(500)
      res.end(String(error))
    }
  })
  upstream.listen(0, '127.0.0.1')
  await new Promise((resolve) => upstream.once('listening', resolve))
  const stubBase = 'http://127.0.0.1:' + upstream.address().port
  await mkdir(join(dir, 'credentials'))
  await writeFile(join(dir, 'credentials', 'fixture.json'), JSON.stringify({
    clientId: 'loopback-client',
    accessToken: 'old-access-token',
    refreshToken: 'fixture-refresh-token',
    expiresAt: expired ? Date.now() - 1000 : Date.now() + 3600_000,
    savedAt: new Date().toISOString(),
    email: 'fixture@example.invalid',
  }), { mode: 0o600 })
  const preload = join(dir, 'loopback-only.mjs')
  await writeFile(preload, `const realFetch = globalThis.fetch;
globalThis.fetch = (input, init) => {
  const original = typeof input === 'string' ? input : input.url ?? String(input);
  const url = original === 'https://auth.openai.com/api/accounts/oauth/token' ? ${JSON.stringify(stubBase + '/test-token')} : original;
  if (!${JSON.stringify([stubBase, ...extraOrigins])}.some((origin) => url.startsWith(origin + '/'))) throw new Error('unexpected non-loopback fetch: ' + url);
  return realFetch(url, init);
};\n`)
  const childEnv = { ...process.env,
    SIWC_STORE: dir, API_BASE: stubBase + '/v1', HOST: '127.0.0.1', PORT: '0',
    GATEWAY_TOKEN: gatewayToken, RATE_LIMIT_PER_MIN: '0', MAX_CONCURRENCY: '8',
    MAX_QUEUE: '8', QUEUE_WAIT_MS: '1000', BODY_TIMEOUT_MS: '1000',
    MAX_BODY_BYTES: '8192', MAX_SPOOL_BYTES: String(16 * 1024 * 1024),
    UPSTREAM_HEADERS_TIMEOUT_MS: '2000', SSE_IDLE_TIMEOUT_MS: '2000',
    ...env,
  }
  delete childEnv.NODE_OPTIONS
  if (prepareDir) await prepareDir(dir)
  child = spawn(process.execPath, ['--import', preload, gatewayFile], { env: childEnv, stdio: ['ignore', 'pipe', 'pipe'] })
  childExit = new Promise((resolve) => child.once('exit', resolve))
  let stdout = ''
  let stderr = ''
  child.stdout.on('data', (chunk) => { stdout += chunk })
  child.stderr.on('data', (chunk) => { stderr += chunk })
  let base
  await until(() => {
    if (child.exitCode !== null) assert.fail('gateway exited: ' + stderr)
    for (const line of stdout.split('\n')) {
      try { const entry = JSON.parse(line); if (entry.event === 'listening') base = entry.url } catch {}
    }
    return base !== undefined
  }, 'gateway listening')
  const request = (path, init = {}) => fetch(base + path, { ...init, headers: { Authorization: 'Bearer ' + gatewayToken, ...init.headers } })
  const post = (model = 'fixture-model', init = {}) => request('/v1/responses', { method: 'POST', body: JSON.stringify({ model, input: 'loopback only' }), ...init })
  const metric = async (name) => {
    const text = await (await request('/metrics')).text()
    return Number(text.split('\n').find((line) => line.startsWith(name + ' '))?.split(' ')[1])
  }
  const clean = async () => {
    await until(async () => await metric('siwc_gateway_spool_reserved_bytes') === 0 && await metric('siwc_gateway_normalizer_active') === 0 && await metric('siwc_gateway_spool_directories') === 0 && await metric('siwc_gateway_upstream_uploads_active') === 0, 'request files, upload FDs and worker capacity released')
    assert.deepEqual(await readdir(join(dir, 'request-spool')), [])
  }
  return { base, calls, request, post, metric, clean, dir, pid: child.pid, logs: () => stdout + stderr }
}

test('a failed refresh does not prevent a later HTTP request from refreshing', { timeout: 8000 }, async (t) => {
  const f = await fixture(t, { expired: true, onToken: (_req, res, _body, calls) => {
    res.writeHead(calls.token === 1 ? 503 : 200, { 'content-type': 'application/json' })
    res.end(calls.token === 1 ? '{"error":"temporary"}' : JSON.stringify({ access_token: 'new-access-token', refresh_token: 'new-refresh-token', expires_in: 3600 }))
  } })
  const first = await f.post()
  assert.equal(first.status, 502)
  await first.text()
  const second = await f.post()
  assert.equal(second.status, 200)
  assert.match(await second.text(), /OK/)
  assert.equal(f.calls.token, 2)
  assert.equal(f.calls.responses, 1)
  await f.clean()
  await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'completed response cleanup released its slot')
})

test('concurrent expired credentials share one refresh', { timeout: 8000 }, async (t) => {
  const f = await fixture(t, { expired: true, onToken: async (_req, res) => {
    await delay(70)
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ access_token: 'new-access-token', refresh_token: 'new-refresh-token', expires_in: 3600 }))
  } })
  const results = await Promise.all(Array.from({ length: 8 }, async () => {
    const response = await f.post()
    return [response.status, await response.text()]
  }))
  assert.ok(results.every(([status, text]) => status === 200 && text.includes('OK')))
  assert.equal(f.calls.token, 1)
  assert.equal(f.calls.responses, 8)
})

test('concurrent 401s reuse one rotated token, including a late old-token 401', { timeout: 8000 }, async (t) => {
  let oldCalls = 0
  let releaseOld
  const allOld = new Promise((resolve) => { releaseOld = resolve })
  const f = await fixture(t, { env: { MAX_CONCURRENCY: '6' }, onApi: async (req, res, raw) => {
    if (req.headers.authorization === 'Bearer old-access-token') {
      oldCalls += 1
      if (oldCalls === 6) releaseOld()
      await allOld
      if (JSON.parse(raw).model === 'late') await delay(120)
      res.writeHead(401, { 'content-type': 'application/json' })
      return res.end('{"error":"expired"}')
    }
    assert.equal(req.headers.authorization, 'Bearer new-access-token')
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    res.end(sse)
  } })
  const responses = await Promise.all(Array.from({ length: 6 }, async (_, i) => {
    const response = await f.post(i === 5 ? 'late' : 'normal')
    const text = await response.text()
    return [response.status, text]
  }))
  assert.ok(responses.every(([status, text]) => status === 200 && text.includes('OK')))
  assert.equal(oldCalls, 6)
  assert.equal(f.calls.token, 1)
  assert.equal(f.calls.responses, 12)
})

test('waiting capacity is bounded and a disconnected waiter never opens upstream', { timeout: 8000 }, async (t) => {
  let held
  const opened = []
  const f = await fixture(t, { env: { MAX_CONCURRENCY: '1', MAX_QUEUE: '1' }, onApi: (_req, res, raw) => {
    const model = JSON.parse(raw).model
    opened.push(model)
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    res.write('data: {"delta":"start"}\n\n')
    if (model === 'hold') held = res
    else res.end(sse)
  } })
  const first = await f.post('hold')
  const cancel = new AbortController()
  const waiting = f.post('cancelled', { signal: cancel.signal }).then(() => 'unexpected success', () => 'aborted')
  await until(async () => await f.metric('siwc_gateway_queued_requests') === 1, 'one queued request')
  const rejected = await f.post('over-capacity')
  assert.equal(rejected.status, 429)
  assert.equal(rejected.headers.get('retry-after'), '1')
  await rejected.text()
  cancel.abort()
  assert.equal(await waiting, 'aborted')
  await until(async () => await f.metric('siwc_gateway_queued_requests') === 0, 'cancelled waiter removed')
  const next = f.post('after-cancel')
  await until(async () => await f.metric('siwc_gateway_queued_requests') === 1, 'replacement waiter accepted')
  held.end(sse)
  await first.text()
  const response = await next
  assert.equal(response.status, 200)
  await response.text()
  assert.deepEqual(opened, ['hold', 'after-cancel'])
  await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'all slots released')
})

test('queue timeout and body rejection release capacity for the next request', { timeout: 8000 }, async (t) => {
  let held
  const f = await fixture(t, { env: { MAX_CONCURRENCY: '1', MAX_QUEUE: '1', QUEUE_WAIT_MS: '80', MAX_BODY_BYTES: '128' }, onApi: (_req, res, raw) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    if (JSON.parse(raw).model === 'hold') { res.write('data: {}\n\n'); held = res }
    else res.end(sse)
  } })
  const first = await f.post('hold')
  const timeout = await f.post('queued-timeout')
  assert.equal(timeout.status, 429)
  await timeout.text()
  assert.equal(await f.metric('siwc_gateway_queued_requests'), 0)
  held.end(sse)
  await first.text()
  const invalid = await f.request('/v1/responses', { method: 'POST', body: '{' })
  assert.equal(invalid.status, 400)
  await invalid.text()
  const oversized = await f.request('/v1/responses', { method: 'POST', body: JSON.stringify({ model: 'too-big', input: 'x'.repeat(200) }) })
  assert.equal(oversized.status, 413)
  await oversized.text()
  const final = await f.post('after-errors')
  assert.equal(final.status, 200)
  await final.text()
  assert.equal(f.calls.responses, 2)
  await f.clean()
  await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'completed response cleanup released its slot')
})

test('disconnects during upload and streaming release their occupied slot', { timeout: 8000 }, async (t) => {
  let heldClosed = false
  const f = await fixture(t, { env: { MAX_CONCURRENCY: '1' }, onApi: (_req, res, raw) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    if (JSON.parse(raw).model === 'hold') {
      res.on('close', () => { heldClosed = true })
      res.write('data: {"delta":"start"}\n\n')
    } else res.end(sse)
  } })
  const uploading = httpRequest(f.base + '/v1/responses', { method: 'POST', headers: { Authorization: 'Bearer ' + gatewayToken } })
  uploading.on('error', () => {})
  uploading.write('{"model":')
  await until(async () => await f.metric('siwc_gateway_inflight_requests') === 1, 'upload occupies one slot')
  uploading.destroy()
  await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'cancelled upload released')
  assert.equal(f.calls.responses, 0)
  const cancel = new AbortController()
  const streaming = await f.post('hold', { signal: cancel.signal })
  cancel.abort()
  await streaming.text().catch(() => {})
  await until(async () => heldClosed && await f.metric('siwc_gateway_inflight_requests') === 0, 'cancelled upstream closed and released')
  const final = await f.post('after-disconnects')
  assert.equal(final.status, 200)
  await final.text()
})

test('model refresh is shared, serves bounded stale data on failure, then recovers', { timeout: 8000 }, async (t) => {
  let failure = false
  let slug = 'first-catalog'
  const f = await fixture(t, { env: { MODELS_TTL_MS: '80', MODELS_MAX_STALE_MS: '350' }, onApi: async (_req, res) => {
    await delay(60)
    res.writeHead(failure ? 503 : 200, { 'content-type': 'application/json' })
    res.end(failure ? '{"error":"temporary"}' : catalog(slug))
  } })
  const batch = () => Promise.all(Array.from({ length: 8 }, async () => {
    const response = await f.request('/v1/models')
    return { status: response.status, state: response.headers.get('x-siwc-models-cache'), text: await response.text() }
  }))
  const initial = await batch()
  assert.equal(f.calls.models, 1)
  assert.ok(initial.every((r) => r.status === 200 && r.state === 'fresh' && r.text.includes('first-catalog')))
  const at = (await (await f.request('/healthz')).json()).modelsCached
  await delay(100)
  failure = true
  const stale = await batch()
  assert.equal(f.calls.models, 2)
  assert.ok(stale.every((r) => r.status === 200 && r.state === 'stale' && r.text.includes('first-catalog')))
  assert.equal((await (await f.request('/healthz')).json()).modelsCached, at)
  await delay(360)
  const expired = await f.request('/v1/models')
  assert.equal(expired.status, 502)
  await expired.text()
  failure = false
  slug = 'recovered-catalog'
  const recovered = await f.request('/v1/models')
  assert.equal(recovered.status, 200)
  assert.equal(recovered.headers.get('x-siwc-models-cache'), 'fresh')
  assert.match(await recovered.text(), /recovered-catalog/)
  assert.equal(f.calls.models, 4)
})

const MiB = 1024 * 1024
const largeEnv = { MAX_CONCURRENCY: '2', MAX_QUEUE: '8', MAX_BODY_BYTES: String(64 * MiB), MAX_SPOOL_BYTES: String(2 * (128 * MiB + 65536)), BODY_IDLE_TIMEOUT_MS: '30000', BODY_TOTAL_TIMEOUT_MS: '300000', UPSTREAM_HEADERS_TIMEOUT_MS: '10000' }
function largePayload(sizeMiB, mixed = false) {
  const prefix = Buffer.from('{"model":"large-fixture","input":"')
  const suffix = Buffer.from('","store":true,"stream":false,"temperature":0.2}')
  const fill = Buffer.alloc(sizeMiB * MiB - prefix.length - suffix.length, 'a')
  if (mixed) Buffer.from('中文😀').copy(fill, Math.floor(fill.length / 2))
  const normalizedSuffix = Buffer.from('","store":false,"stream":true}')
  return {
    body: Buffer.concat([prefix, fill, suffix]),
    hash: createHash('sha256').update(prefix).update(fill).update(normalizedSuffix).digest('hex'),
    normalizedBytes: prefix.length + fill.length + normalizedSuffix.length,
  }
}

async function hashUpstream(req, res, received) {
  const digest = createHash('sha256')
  let bytes = 0
  for await (const chunk of req) { digest.update(chunk); bytes += chunk.length }
  received.push({ hash: digest.digest('hex'), bytes, contentEncoding: req.headers['content-encoding'] })
  res.writeHead(200, { 'content-type': 'text/event-stream' })
  res.end(sse)
}

for (const [size, encoding, mixed] of [[20, 'identity', false], [30, 'gzip', false], [50, 'zstd', false], [50, 'identity', true]]) {
  test(`${size}MiB ${encoding}${mixed ? ' mixed Unicode' : ''} request is normalized without changing its content`, { timeout: 30000 }, async (t) => {
    const received = []
    const f = await fixture(t, { env: largeEnv, onApiRequest: (req, res) => hashUpstream(req, res, received) })
    const payload = largePayload(size, mixed)
    const wire = encoding === 'gzip' ? gzipSync(payload.body) : encoding === 'zstd' ? zstdCompressSync(payload.body) : payload.body
    let parentPeakRss = 0
    const sampler = setInterval(async () => { parentPeakRss = Math.max(parentPeakRss, await f.metric('siwc_gateway_process_rss_bytes').catch(() => 0)) }, 20)
    const response = await f.request('/v1/responses', { method: 'POST', body: wire, headers: { 'content-encoding': encoding } })
    const text = await response.text()
    clearInterval(sampler)
    assert.equal(response.status, 200, text)
    assert.deepEqual(received, [{ hash: payload.hash, bytes: payload.normalizedBytes, contentEncoding: undefined }])
    await f.clean()
    t.diagnostic(JSON.stringify({ sizeMiB: size, encoding, mixed, wireBytes: wire.length, parentPeakRss, workerPeakRss: await f.metric('siwc_gateway_normalizer_peak_rss_bytes') }))
  })
}

test('two concurrent 50MiB bodies use one normalizer and leave no spool files during SSE', { timeout: 30000 }, async (t) => {
  const received = []
  const held = []
  const f = await fixture(t, { env: largeEnv, onApiRequest: async (req, res) => {
    const hash = createHash('sha256')
    let bytes = 0
    for await (const chunk of req) { hash.update(chunk); bytes += chunk.length }
    received.push({ hash: hash.digest('hex'), bytes })
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    res.write('data: {"delta":"started"}\n\n')
    held.push(res)
  } })
  const payload = largePayload(50, true)
  let parentPeakRss = 0
  let maximumWorkers = 0
  const sampler = setInterval(async () => {
    parentPeakRss = Math.max(parentPeakRss, await f.metric('siwc_gateway_process_rss_bytes').catch(() => 0))
    maximumWorkers = Math.max(maximumWorkers, await f.metric('siwc_gateway_normalizer_active').catch(() => 0))
  }, 10)
  const responses = await Promise.all([1, 2].map(() => f.request('/v1/responses', { method: 'POST', body: payload.body })))
  clearInterval(sampler)
  assert.ok(responses.every((response) => response.status === 200))
  assert.deepEqual(received, [{ hash: payload.hash, bytes: payload.normalizedBytes }, { hash: payload.hash, bytes: payload.normalizedBytes }])
  assert.equal(maximumWorkers, 1)
  await f.clean()
  assert.equal(await f.metric('siwc_gateway_active_streams'), 2)
  for (const res of held) res.end(sse)
  await Promise.all(responses.map((response) => response.text()))
  await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'both response slots released')
  t.diagnostic(JSON.stringify({ sizeMiB: '2x50 mixed Unicode', parentPeakRss, workerPeakRss: await f.metric('siwc_gateway_normalizer_peak_rss_bytes'), maximumWorkers }))
})

test('string chunks preserve escapes, surrogate pairs, long keys, and JSON number semantics', { timeout: 8000 }, async (t) => {
  const longKey = 'k'.repeat(70000) + '😀'
  const input = 'a'.repeat(65535) + '😀' + '\\"\n\t\u0000' + '\ud800' + '\udc00' + '中'
  const body = JSON.stringify({ model: 'fixture', input, [longKey]: input, store: true, stream: false })
  let forwarded
  const f = await fixture(t, { env: { ...largeEnv, MAX_BODY_BYTES: String(MiB) }, onApi: (_req, res, raw) => {
    forwarded = raw
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end(sse)
  } })
  const response = await f.request('/v1/responses', { method: 'POST', body })
  assert.equal(response.status, 200)
  await response.text()
  assert.equal(forwarded, JSON.stringify({ model: 'fixture', input, [longKey]: input, store: false, stream: true }))
  await f.clean()
})

test('unknown encodings, corrupt compression, zstd trailing frames and decoded bombs are rejected and cleaned', { timeout: 12000 }, async (t) => {
  const f = await fixture(t, { env: { ...largeEnv, MAX_DECODED_BODY_BYTES: '32768' } })
  const small = Buffer.from('{"model":"fixture","input":"ok"}')
  const cases = [
    ['br', small, 415],
    ['gzip', Buffer.from('not gzip'), 400],
    ['zstd', Buffer.from('not zstd'), 400],
    ['zstd', Buffer.concat([zstdCompressSync(small), zstdCompressSync(small)]), 400],
    ['zstd', Buffer.concat([zstdCompressSync(small), Buffer.from('ignored junk')]), 400],
    ['gzip', gzipSync(Buffer.alloc(MiB, 'a')), 413],
    ['zstd', zstdCompressSync(Buffer.alloc(MiB, 'a')), 413],
  ]
  for (const [encoding, body, status] of cases) {
    const response = await f.request('/v1/responses', { method: 'POST', body, headers: { 'content-encoding': encoding } })
    assert.equal(response.status, status, await response.text())
    await f.clean()
  }
  assert.equal(f.calls.responses, 0)
  const valid = await f.post()
  assert.equal(valid.status, 200)
  await valid.text()
  await f.clean()
})

test('wire quota, disk reservation and processing timeout fail without leaking capacity', { timeout: 12000 }, async (t) => {
  const limited = await fixture(t, { env: { ...largeEnv, MAX_WIRE_BODY_BYTES: '128' } })
  const response = await limited.request('/v1/responses', { method: 'POST', body: Readable.from([Buffer.alloc(256, 'a')]), duplex: 'half' })
  assert.equal(response.status, 413)
  await response.text(); await limited.clean()
  assert.equal(limited.calls.responses, 0)
  const disk = await fixture(t, { env: { MAX_SPOOL_BYTES: '1' } })
  const busy = await disk.post()
  assert.equal(busy.status, 503)
  await busy.text(); await disk.clean()
  const workerTimeout = await fixture(t, { env: { NORMALIZE_TIMEOUT_MS: '1' } })
  for (let i = 0; i < 2; i++) {
    const timedOut = await workerTimeout.post()
    assert.equal(timedOut.status, 503)
    await timedOut.text(); await workerTimeout.clean()
  }
  assert.equal(workerTimeout.calls.responses, 0)
})

test('upload idle and total deadlines are separate and always clean disk and slots', { timeout: 12000 }, async (t) => {
  for (const total of [false, true]) {
    const f = await fixture(t, { env: { BODY_IDLE_TIMEOUT_MS: total ? '150' : '50', BODY_TOTAL_TIMEOUT_MS: total ? '180' : '2000' } })
    const result = new Promise((resolve, reject) => {
      const req = httpRequest(f.base + '/v1/responses', { method: 'POST', headers: { Authorization: 'Bearer ' + gatewayToken } }, async (res) => {
        let text = ''; for await (const chunk of res) text += chunk
        resolve({ status: res.statusCode, text })
      })
      req.on('error', reject)
      req.write('{"input":"')
      if (total) {
        const interval = setInterval(() => req.write('a'), 25)
        req.on('close', () => clearInterval(interval))
      }
    })
    const response = await result
    assert.equal(response.status, 408)
    assert.match(response.text, total ? /total timeout/ : /idle timeout/)
    await f.clean()
    await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'completed response cleanup released its slot')
  }
})

test('cancelling a large request during normalization kills the worker and frees resources', { timeout: 20000 }, async (t) => {
  const f = await fixture(t, { env: largeEnv })
  const payload = largePayload(30, true)
  const abort = new AbortController()
  const pending = f.request('/v1/responses', { method: 'POST', body: payload.body, signal: abort.signal }).then(() => 'unexpected success', () => 'aborted')
  await until(async () => await f.metric('siwc_gateway_normalizer_active') === 1, 'normalizer active', 8000)
  abort.abort()
  assert.equal(await pending, 'aborted')
  await f.clean()
  const next = await f.post()
  assert.equal(next.status, 200)
  await next.text(); await f.clean()
})

test('startup removes abandoned private request files and preserves unrelated spool entries', { timeout: 8000 }, async (t) => {
  const f = await fixture(t, { prepareDir: async (dir) => {
    const spool = join(dir, 'request-spool')
    await mkdir(join(spool, 'request-ABC123'), { recursive: true, mode: 0o700 })
    await writeFile(join(spool, 'request-ABC123', 'owner.json'), '{"pid":2147483647}', { mode: 0o600 })
    await writeFile(join(spool, 'request-ABC123', 'decoded.json'), 'fixture payload', { mode: 0o600 })
    await writeFile(join(spool, 'unrelated.txt'), 'keep this')
  } })
  assert.deepEqual(await readdir(join(f.dir, 'request-spool')), ['unrelated.txt'])
  assert.match(f.logs(), /stale-request-spool-removed/)
})

test('an early upstream 401 replays a large file with the new token', { timeout: 15000 }, async (t) => {
  const payload = largePayload(10)
  let oldCalls = 0
  let freshCalls = 0
  const received = []
  const f = await fixture(t, { env: largeEnv, onApiRequest: async (req, res) => {
    if (req.headers.authorization === 'Bearer old-access-token') {
      oldCalls += 1
      res.writeHead(401, { 'content-type': 'application/json' })
      return res.end('{"error":"expired before body upload"}')
    }
    freshCalls += 1
    return hashUpstream(req, res, received)
  } })
  const response = await f.request('/v1/responses', { method: 'POST', body: payload.body })
  assert.equal(response.status, 200)
  const text = await response.text().catch((error) => { t.diagnostic(f.logs()); throw error })
  assert.match(text, /DONE/)
  assert.equal(oldCalls, 1)
  assert.equal(freshCalls, 1)
  assert.equal(f.calls.token, 1)
  assert.deepEqual(received, [{ hash: payload.hash, bytes: payload.normalizedBytes, contentEncoding: undefined }])
  await f.clean()
  await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'completed response cleanup released its slot')
})

test('gzip members decode together while zstd decoder window remains bounded', { timeout: 8000 }, async (t) => {
  let normalized
  const gzip = await fixture(t, { onApi: (_req, res, raw) => {
    normalized = JSON.parse(raw)
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end(sse)
  } })
  const body = Buffer.from('{"model":"fixture","input":"two gzip members"}')
  const encoded = Buffer.concat([gzipSync(body.subarray(0, 20)), gzipSync(body.subarray(20))])
  const response = await gzip.request('/v1/responses', { method: 'POST', body: encoded, headers: { 'content-encoding': 'gzip' } })
  assert.equal(response.status, 200)
  await response.text(); await gzip.clean()
  assert.deepEqual(normalized, { model: 'fixture', input: 'two gzip members', store: false, stream: true })
  const zstd = await fixture(t, { env: { ...largeEnv, ZSTD_WINDOW_LOG_MAX: '10' } })
  const oversizedWindow = zstdCompressSync(Buffer.from(JSON.stringify({ model: 'fixture', input: 'a'.repeat(100000) })))
  const rejected = await zstd.request('/v1/responses', { method: 'POST', body: oversizedWindow, headers: { 'content-encoding': 'zstd' } })
  assert.equal(rejected.status, 400)
  await rejected.text(); await zstd.clean()
  assert.equal(zstd.calls.responses, 0)
})

test('spooled data is private and normalized bytes are removed before SSE streaming', { timeout: 8000 }, async (t) => {
  let release
  let entered
  const began = new Promise((resolve) => { entered = resolve })
  const hold = new Promise((resolve) => { release = resolve })
  const f = await fixture(t, { onApiRequest: async (req, res) => {
    entered()
    await hold
    for await (const _chunk of req) {}
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end(sse)
  } })
  const pending = f.post()
  await began
  const dirs = await readdir(join(f.dir, 'request-spool'))
  assert.equal(dirs.length, 1)
  const dir = join(f.dir, 'request-spool', dirs[0])
  assert.equal((await stat(dir)).mode & 0o777, 0o700)
  assert.deepEqual((await readdir(dir)).sort(), ['normalized.json', 'owner.json'])
  assert.equal((await stat(join(dir, 'normalized.json'))).mode & 0o777, 0o600)
  assert.equal((await stat(join(dir, 'owner.json'))).mode & 0o777, 0o600)
  release()
  const response = await pending
  assert.equal(response.status, 200)
  await response.text(); await f.clean()
})

async function pausedEarlyResponse(t, status, body) {
  const sockets = new Set()
  let calls = 0
  const server = createTcpServer((socket) => {
    sockets.add(socket)
    socket.on('error', () => {})
    socket.on('close', () => sockets.delete(socket))
    let header = Buffer.alloc(0)
    const readHeader = (chunk) => {
      header = Buffer.concat([header, chunk])
      if (!header.includes('\r\n\r\n')) return
      socket.off('data', readHeader)
      socket.pause() // Deliberately never drain the large upload after rejection.
      calls += 1
      const type = status === 200 ? 'text/event-stream' : 'application/json'
      socket.write(`HTTP/1.1 ${status} ${status === 200 ? 'OK' : 'Bad Request'}\r\nContent-Type: ${type}\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: keep-alive\r\n\r\n${body}`)
      header = null
    }
    socket.on('data', readHeader)
  })
  server.listen(0, '127.0.0.1')
  await new Promise((resolve) => server.once('listening', resolve))
  t.after(() => { for (const socket of sockets) socket.destroy(); server.close() })
  return { base: 'http://127.0.0.1:' + server.address().port, calls: () => calls }
}

for (const status of [400, 200]) {
  test(`early upstream ${status} is delivered completely and its unread upload is closed`, { timeout: 15000 }, async (t) => {
    const body = status === 200 ? sse : JSON.stringify({ error: { message: 'rejected before reading the upload' } })
    const upstream = await pausedEarlyResponse(t, status, body)
    const f = await fixture(t, { env: { ...largeEnv, API_BASE: upstream.base + '/v1' }, extraOrigins: [upstream.base] })
    const payload = largePayload(30)
    const response = await f.request('/v1/responses', { method: 'POST', body: payload.body })
    const text = await response.text().catch((error) => { t.diagnostic(f.logs()); throw error })
    assert.equal(response.status, status, text)
    assert.equal(text, body)
    assert.equal(upstream.calls(), 1)
    await f.clean()
    await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'completed early response released its slot')
  })
}

for (const slow of [false, true]) {
  test(`upstream error body ${slow ? 'deadline' : 'byte limit'} preserves status and Retry-After without retaining body data`, { timeout: 8000 }, async (t) => {
    const f = await fixture(t, { env: { UPSTREAM_ERROR_BODY_TIMEOUT_MS: '60' }, onApiRequest: (_req, res) => {
      res.writeHead(429, { 'content-type': 'application/json', 'retry-after': '17' })
      if (slow) res.write('{"error":')
      else res.end(Buffer.alloc(2 * MiB, 'x'))
    } })
    const response = await f.post()
    assert.equal(response.status, 429)
    assert.equal(response.headers.get('retry-after'), '17')
    const text = await response.text()
    assert.match(text, slow ? /timed out/ : /64KiB safety limit/)
    assert.ok(text.length < 200)
    await f.clean()
    await until(async () => await f.metric('siwc_gateway_inflight_requests') === 0, 'completed response cleanup released its slot')
  })
}
