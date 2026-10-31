import { createServer, request } from 'node:http'
import { createHash } from 'node:crypto'
import { mkdir, writeFile, readFile, rm, chown } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'
import { setTimeout as delay } from 'node:timers/promises'
import { once } from 'node:events'

// All traffic and credentials in this probe are synthetic and loopback-only.
// The source imports streaming-json.mjs, which must exist beside it.
const source = process.argv[2]
if (!source) throw new Error('gateway source path required')
const unit = 'siwc-body-acceptance'
const dir = '/var/lib/siwc-gateway/body-acceptance-' + process.pid
const cg = '/sys/fs/cgroup/system.slice/' + unit + '.service'
const uid = Number(execFileSync('id', ['-u', 'siwc-gw'], { encoding: 'utf8' }).trim())
const gid = Number(execFileSync('id', ['-g', 'siwc-gw'], { encoding: 'utf8' }).trim())
const prefix = Buffer.from('{"model":"fixture-model","input":"')
const chunk = Buffer.alloc(64 * 1024, 'x')
const inputTail = Buffer.from('","max_output_tokens":20,"store":true,"stream":false}')
const outputTail = Buffer.from('","store":false,"stream":true}')
const terminalSse = 'data: {"type":"response.completed"}\n\ndata: [DONE]\n\n'
const expected = new Map()
const observed = []
const heldResponses = new Map()
const checks = []
let heldStreamClosed = false
let prefixReached
const prefixObserved = new Promise(resolve => { prefixReached = resolve })
let peak = 0
let events = ''
let timer

async function deadline(promise, description, timeout = 30_000) {
  let expiry
  try {
    return await Promise.race([promise, new Promise((_, reject) => { expiry = setTimeout(() => reject(new Error(description)), timeout) })])
  } finally { clearTimeout(expiry) }
}
const stub = createServer(async (req, res) => {
  try {
    if (req.url !== '/v1/responses') { res.writeHead(500); res.end('unexpected route'); return }
    const key = req.headers['x-client-request-id']
    let bytes = 0
    const hash = createHash('sha256')
    for await (const part of req) {
      bytes += part.length
      hash.update(part)
      if (key === 'stream-before-complete') prefixReached()
    }
    const digest = hash.digest('hex')
    if (key === 'cancelled-sse') {
      res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write('data: {"delta":"started"}\n\n')
      const force = setTimeout(() => res.end(terminalSse), 30_000)
      res.once('close', () => { heldStreamClosed = true; clearTimeout(force) })
      return
    }
    observed.push({ key, bytes, valid: digest === expected.get(key) })
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    if (key === 'held-sse-a' || key === 'held-sse-b') {
      heldResponses.set(key, res)
      res.write('data: {"delta":"started"}\n\n')
      return
    }
    res.end(terminalSse)
  } catch {
    // A cancellation probe can close a deliberately partial request.
    res.destroy()
  }
})
await mkdir(dir + '/credentials', { recursive: true, mode: 0o700 })
await writeFile(dir + '/credentials/fake.json', JSON.stringify({ clientId: 'fixture', accessToken: 'fixture-only', refreshToken: 'unused-fixture', expiresAt: Date.now() + 3600_000, savedAt: new Date().toISOString(), email: 'fixture@example.invalid' }), { mode: 0o600 })
await chown(dir, uid, gid); await chown(dir + '/credentials', uid, gid); await chown(dir + '/credentials/fake.json', uid, gid)
stub.listen(0, '127.0.0.1'); await once(stub, 'listening')
const port = Number(process.env.BENCH_GATEWAY_PORT ?? 21457)
const ingressPort = Number(process.env.BENCH_INGRESS_PORT ?? port)
const options = (key, length) => ({ hostname: '127.0.0.1', port: ingressPort, path: '/v1/responses', method: 'POST', headers: { authorization: 'Bearer synthetic-test-token', 'content-type': 'application/json', ...(length === undefined ? {} : { 'content-length': length }), 'x-client-request-id': key } })
const completion = (req, key, mib, started) => new Promise((resolve, reject) => {
  req.once('error', reject)
  req.once('response', async res => {
    try { let body = ''; for await (const part of res) body += part; resolve({ key, mib, status: res.statusCode, sseComplete: body.includes('[DONE]'), ms: Date.now() - started }) }
    catch (error) { reject(error) }
  })
})
const post = async (mib, key, mixed = false) => {
  const extra = mixed ? Buffer.from('中') : Buffer.alloc(0)
  const count = mib * 1024 * 1024 - prefix.length - inputTail.length - extra.length
  const hash = createHash('sha256').update(prefix).update(extra)
  let left = count
  while (left) { const part = chunk.subarray(0, Math.min(left, chunk.length)); hash.update(part); left -= part.length }
  hash.update(outputTail); expected.set(key, hash.digest('hex'))
  const started = Date.now()
  const req = request(options(key, prefix.length + extra.length + count + inputTail.length))
  const done = completion(req, key, mib, started)
  done.catch(() => {})
  req.setTimeout(120_000, () => req.destroy(new Error('benchmark deadline')))
  if (!req.write(prefix)) await once(req, 'drain')
  if (extra.length && !req.write(extra)) await once(req, 'drain')
  left = count
  while (left) { const part = chunk.subarray(0, Math.min(left, chunk.length)); if (!req.write(part)) await once(req, 'drain'); left -= part.length }
  req.end(inputTail)
  return done
}
const resourceNames = ['inflight_requests', 'queued_requests', 'upload_active', 'upload_queued', 'normalizer_active', 'normalizer_queued', 'upstream_uploads_active', 'spool_reserved_bytes', 'spool_directories', 'active_streams']
const metricsSnapshot = async () => {
  const text = await (await fetch('http://127.0.0.1:' + port + '/metrics')).text()
  const values = Object.fromEntries(text.split('\n').filter(line => line.startsWith('siwc_gateway_')).map(line => { const [name, value] = line.split(' '); return [name.slice('siwc_gateway_'.length), Number(value)] }))
  return { text, values }
}
const awaitResources = async (predicate, description) => {
  for (let i = 0; i < 200; i++) {
    const snapshot = await metricsSnapshot()
    if (predicate(snapshot.values)) return snapshot
    await delay(25)
  }
  throw new Error(description)
}
try {
  execFileSync('systemd-run', ['--unit=' + unit, '--collect', '--property=MemoryMax=512M', '--property=MemorySwapMax=0', '--property=LimitCORE=0', '--property=OOMPolicy=continue', '--property=CPUQuota=150%', '--property=User=siwc-gw', '--property=Group=siwc-gw', '--setenv=SIWC_STORE=' + dir, '--setenv=API_BASE=http://127.0.0.1:' + stub.address().port + '/v1', '--setenv=GATEWAY_TOKEN=synthetic-test-token', '--setenv=PORT=' + port, '--setenv=HOST=127.0.0.1', '--setenv=RATE_LIMIT_PER_MIN=0', '--setenv=MAX_RESPONSE_CONCURRENCY=8', '--setenv=MAX_UPLOAD_CONCURRENCY=2', '/usr/local/bin/node', source], { encoding: 'utf8' })
  for (let i = 0; i < 100; i++) { try { if ((await fetch('http://127.0.0.1:' + port + '/healthz')).ok) break } catch {} if (i === 99) throw new Error('gateway did not start'); await delay(50) }
  timer = setInterval(async () => { try { peak = Math.max(peak, Number(await readFile(cg + '/memory.peak', 'utf8'))); events = await readFile(cg + '/memory.events', 'utf8') } catch {} }, 25)

  // This is an ordering assertion, not a latency threshold: the suffix is sent
  // only after the stub has already received bytes of this incomplete body.
  const orderingReq = request(options('stream-before-complete'))
  const orderingDone = completion(orderingReq, 'stream-before-complete', undefined, Date.now())
  orderingDone.catch(() => {})
  expected.set('stream-before-complete', createHash('sha256').update(prefix).update(chunk).update(outputTail).digest('hex'))
  orderingReq.write(prefix); orderingReq.write(chunk)
  try { await deadline(prefixObserved, 'upstream did not receive prefix before client EOF') }
  catch (error) { orderingReq.destroy(); throw error }
  orderingReq.end(inputTail)
  checks.push(await orderingDone)

  for (const mib of [20, 30, 50]) checks.push(await post(mib, 'single-' + mib))
  checks.push(await post(50, 'mixed-unicode-50', true))
  checks.push(...await Promise.all([post(50, 'parallel-50-a', true), post(50, 'parallel-50-b', true)]))

  // Two open SSE responses must leave both upload slots available. Keep their
  // readers active so response backpressure does not accidentally close them.
  const held = await Promise.all(['held-sse-a', 'held-sse-b'].map(async key => {
    const fill = Buffer.from('held')
    expected.set(key, createHash('sha256').update(prefix).update(fill).update(outputTail).digest('hex'))
    const started = Date.now()
    const response = await fetch('http://127.0.0.1:' + ingressPort + '/v1/responses', { method: 'POST', headers: options(key).headers, body: Buffer.concat([prefix, fill, inputTail]), signal: AbortSignal.timeout(120_000) })
    const reader = response.body.getReader()
    const first = await deadline(reader.read(), 'held SSE did not start')
    if (response.status !== 200 || first.done) throw new Error('held SSE did not open')
    return { key, response, reader, first: Buffer.from(first.value).toString(), started }
  }))
  const heldSnapshot = await awaitResources(v => v.active_streams === 2 && v.upload_active === 0 && v.normalizer_active === 0 && v.upstream_uploads_active === 0 && v.spool_reserved_bytes === 0 && v.spool_directories === 0, 'held SSE retained upload resources')
  checks.push(await post(50, 'large-while-two-sse', true))
  await awaitResources(v => v.active_streams === 2 && v.upload_active === 0 && v.normalizer_active === 0 && v.upstream_uploads_active === 0 && v.spool_reserved_bytes === 0 && v.spool_directories === 0, 'large upload did not release its resources while SSE remained open')
  for (const response of heldResponses.values()) response.end(terminalSse)
  for (const item of held) {
    let body = item.first
    for (;;) { const next = await item.reader.read(); if (next.done) break; body += Buffer.from(next.value).toString() }
    checks.push({ key: item.key, status: item.response.status, sseComplete: body.includes('[DONE]'), ms: Date.now() - item.started })
  }

  const cancelled = await new Promise((resolve, reject) => {
    const req = request(options('cancelled-sse'), res => {
      res.once('data', () => { res.destroy(); resolve(true) })
      res.once('error', () => {})
    })
    req.once('error', reject); req.end('{"model":"fixture-model","input":"cancel probe"}')
  })
  for (let i = 0; i < 100 && !heldStreamClosed; i++) await delay(25)
  if (!cancelled || !heldStreamClosed) throw new Error('SSE cancellation did not reach stub upstream')
  const final = await awaitResources(v => resourceNames.every(name => v[name] === 0), 'request, upload, validator, file and spool resources did not return to zero')
  peak = Math.max(peak, Number(await readFile(cg + '/memory.peak', 'utf8')))
  events = await readFile(cg + '/memory.events', 'utf8')
  console.log(JSON.stringify({ node: process.version, memoryMax: 512 * 1024 * 1024, memorySwapMax: 0, cgroupPeakBytes: peak, events, ingressPort, prefixReachedBeforeClientEof: true, largeUploadWhileTwoSseOpen: true, heldSseResourceMetrics: heldSnapshot.values, sseCancellationReachedUpstream: heldStreamClosed, checks, observed, resourceMetrics: final.text.split('\n').filter(s => /(?:memory|spool|normaliz|processing|inflight|queued|body_|upload|active_streams)/.test(s) && !s.startsWith('#')) }, null, 2))
  if (checks.some(x => x.status !== 200 || !x.sseComplete) || observed.some(x => !x.valid) || observed.length !== checks.length || /(?:^|\n)(?:max|oom|oom_kill) [1-9]/.test(events)) process.exitCode = 1
} finally {
  clearInterval(timer)
  try { execFileSync('systemctl', ['stop', unit], { stdio: 'ignore' }) } catch {}
  stub.closeAllConnections(); stub.close()
  await rm(dir, { recursive: true, force: true })
}
