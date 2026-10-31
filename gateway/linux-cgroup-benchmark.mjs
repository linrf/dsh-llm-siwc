import { createServer, request } from 'node:http'
import { createHash } from 'node:crypto'
import { mkdir, writeFile, readFile, rm, chown } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'
import { setTimeout as delay } from 'node:timers/promises'
import { once } from 'node:events'

// All traffic and credentials in this probe are synthetic and loopback-only.
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
const expected = new Map()
const observed = []
let heldStreamClosed = false
const checks = []
let peak = 0
let events = ''
let timer
const stub = createServer(async (req, res) => {
  if (req.url !== '/v1/responses') { res.writeHead(500); res.end('unexpected route'); return }
  let bytes = 0
  const hash = createHash('sha256')
  for await (const part of req) { bytes += part.length; hash.update(part) }
  const digest = hash.digest('hex')
  if (req.headers['x-client-request-id'] === 'cancelled-sse') {
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write('data: {"delta":"started"}\n\n')
    const force = setTimeout(() => res.end('data: [DONE]\n\n'), 30_000)
    res.once('close', () => { heldStreamClosed = true; clearTimeout(force) })
    return
  }
  const key = req.headers['x-client-request-id']
  observed.push({ key, bytes, valid: digest === expected.get(key) })
  res.writeHead(200, { 'content-type': 'text/event-stream' })
  res.end('data: {"type":"response.completed"}\n\ndata: [DONE]\n\n')
})
await mkdir(dir + '/credentials', { recursive: true, mode: 0o700 })
await writeFile(dir + '/credentials/fake.json', JSON.stringify({ clientId: 'fixture', accessToken: 'fixture-only', refreshToken: 'unused-fixture', expiresAt: Date.now() + 3600_000, savedAt: new Date().toISOString(), email: 'fixture@example.invalid' }), { mode: 0o600 })
await chown(dir, uid, gid); await chown(dir + '/credentials', uid, gid); await chown(dir + '/credentials/fake.json', uid, gid)
stub.listen(0, '127.0.0.1'); await once(stub, 'listening')
const port = Number(process.env.BENCH_GATEWAY_PORT ?? 21457)
const ingressPort = Number(process.env.BENCH_INGRESS_PORT ?? port)
const post = async (mib, key, mixed = false) => {
  const extra = mixed ? Buffer.from("中") : Buffer.alloc(0)
  const count = mib * 1024 * 1024 - prefix.length - inputTail.length - extra.length
  const hash = createHash('sha256').update(prefix).update(extra)
  let left = count
  while (left) { const part = chunk.subarray(0, Math.min(left, chunk.length)); hash.update(part); left -= part.length }
  hash.update(outputTail); expected.set(key, hash.digest('hex'))
  const started = Date.now()
  const req = request({ hostname: '127.0.0.1', port: ingressPort, path: '/v1/responses', method: 'POST', headers: { authorization: 'Bearer synthetic-test-token', 'content-type': 'application/json', 'content-length': prefix.length + extra.length + count + inputTail.length, 'x-client-request-id': key } })
  const completion = new Promise((resolve, reject) => { req.once('error', reject); req.once('response', async res => { let body = ''; for await (const part of res) body += part; resolve({ key, mib, status: res.statusCode, sseComplete: body.includes('[DONE]'), ms: Date.now() - started }) }) })
  req.setTimeout(120_000, () => req.destroy(new Error('benchmark deadline')))
  if (!req.write(prefix)) await once(req, 'drain')
  if (extra.length && !req.write(extra)) await once(req, 'drain')
  left = count
  while (left) { const part = chunk.subarray(0, Math.min(left, chunk.length)); if (!req.write(part)) await once(req, 'drain'); left -= part.length }
  req.end(inputTail)
  return completion
}
try {
  execFileSync('systemd-run', ['--unit=' + unit, '--collect', '--property=MemoryMax=512M', '--property=MemorySwapMax=0', '--property=LimitCORE=0', '--property=OOMPolicy=continue', '--property=CPUQuota=150%', '--property=User=siwc-gw', '--property=Group=siwc-gw', '--setenv=SIWC_STORE=' + dir, '--setenv=API_BASE=http://127.0.0.1:' + stub.address().port + '/v1', '--setenv=GATEWAY_TOKEN=synthetic-test-token', '--setenv=PORT=' + port, '--setenv=HOST=127.0.0.1', '--setenv=RATE_LIMIT_PER_MIN=0', '/usr/local/bin/node', source], { encoding: 'utf8' })
  for (let i = 0; i < 100; i++) { try { if ((await fetch('http://127.0.0.1:' + port + '/healthz')).ok) break } catch {} if (i === 99) throw new Error('gateway did not start'); await delay(50) }
  timer = setInterval(async () => { try { peak = Math.max(peak, Number(await readFile(cg + '/memory.peak', 'utf8'))); events = await readFile(cg + '/memory.events', 'utf8') } catch {} }, 25)
  for (const mib of [20, 30, 50]) checks.push(await post(mib, 'single-' + mib))
  checks.push(await post(50, 'mixed-unicode-50', true))
  checks.push(...await Promise.all([post(50, 'parallel-50-a', true), post(50, 'parallel-50-b', true)]))
  const cancelled = await new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port: ingressPort, path: '/v1/responses', method: 'POST', headers: { authorization: 'Bearer synthetic-test-token', 'content-type': 'application/json', 'x-client-request-id': 'cancelled-sse' } }, res => {
      res.once('data', () => { res.destroy(); resolve(true) })
      res.once('error', () => {})
    })
    req.once('error', reject); req.end('{"model":"fixture-model","input":"cancel probe"}')
  })
  for (let i = 0; i < 100 && !heldStreamClosed; i++) await delay(25)
  if (!cancelled || !heldStreamClosed) throw new Error('SSE cancellation did not reach stub upstream')
  await delay(150)
  peak = Math.max(peak, Number(await readFile(cg + '/memory.peak', 'utf8')))
  events = await readFile(cg + '/memory.events', 'utf8')
  const metrics = await (await fetch('http://127.0.0.1:' + port + '/metrics')).text()
  console.log(JSON.stringify({ node: process.version, memoryMax: 512 * 1024 * 1024, memorySwapMax: 0, cgroupPeakBytes: peak, events, ingressPort, sseCancellationReachedUpstream: heldStreamClosed, checks, observed, resourceMetrics: metrics.split('\n').filter(s => /(?:memory|spool|normaliz|processing|inflight|queued|body_|upload)/.test(s) && !s.startsWith('#')) }, null, 2))
  if (checks.some(x => x.status !== 200 || !x.sseComplete) || observed.some(x => !x.valid) || observed.length !== checks.length || /oom_kill [1-9]/.test(events)) process.exitCode = 1
} finally {
  clearInterval(timer)
  try { execFileSync('systemctl', ['stop', unit], { stdio: 'ignore' }) } catch {}
  stub.closeAllConnections(); stub.close()
  await rm(dir, { recursive: true, force: true })
}
