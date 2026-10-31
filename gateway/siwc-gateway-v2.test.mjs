import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createServer, request as httpRequest } from 'node:http'
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
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

async function fixture(t, { expired = false, env = {}, onToken, onApi } = {}) {
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
  if (!url.startsWith(${JSON.stringify(stubBase + '/')})) throw new Error('unexpected non-loopback fetch: ' + url);
  return realFetch(url, init);
};\n`)
  const childEnv = { ...process.env,
    SIWC_STORE: dir, API_BASE: stubBase + '/v1', HOST: '127.0.0.1', PORT: '0',
    GATEWAY_TOKEN: gatewayToken, RATE_LIMIT_PER_MIN: '0', MAX_CONCURRENCY: '8',
    MAX_QUEUE: '8', QUEUE_WAIT_MS: '1000', BODY_TIMEOUT_MS: '1000',
    UPSTREAM_HEADERS_TIMEOUT_MS: '2000', SSE_IDLE_TIMEOUT_MS: '2000',
    ...env,
  }
  delete childEnv.NODE_OPTIONS
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
  return { base, calls, request, post, metric, logs: () => stdout + stderr }
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
  assert.equal(await f.metric('siwc_gateway_inflight_requests'), 0)
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
  assert.equal(await f.metric('siwc_gateway_inflight_requests'), 0)
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
