// Transport probe only: a top-level JSON array must be rejected before inference.
import { Readable } from 'node:stream'
import * as zlib from 'node:zlib'
import { setTimeout as delay } from 'node:timers/promises'
import { fetch, ProxyAgent } from 'undici'
const base = process.env.SIWC_BASE_URL?.replace(/\/+$/, '')
const token = process.env.SIWC_GATEWAY_TOKEN
if (!base || !token) throw new Error('SIWC_BASE_URL and SIWC_GATEWAY_TOKEN are required')
const proxy = process.env.SIWC_PROXY_URL ? new ProxyAgent(process.env.SIWC_PROXY_URL) : undefined
const chunk = Buffer.alloc(64 * 1024, 'x')
async function* body(bytes, durationMs) {
  const started = Date.now()
  yield Buffer.from('["')
  let sent = 0
  const count = bytes - 4
  while (sent < count) {
    const part = chunk.subarray(0, Math.min(chunk.length, count - sent))
    yield part
    sent += part.length
    if (durationMs) await delay(Math.max(0, started + durationMs * sent / count - Date.now()))
  }
  yield Buffer.from('"]')
}
const results = []
try {
  for (const [encoding, mib, duration] of (process.env.SIWC_SKIP_LARGE_PROBES === '1' ? [] : [['identity', 20, 40_000], ['gzip', 30, 0], ['zstd', 50, 0]])) {
    const stream = Readable.from(body(mib * 1024 * 1024, duration))
    const payload = encoding === 'gzip' ? stream.pipe(zlib.createGzip()) : encoding === 'zstd' ? stream.pipe(zlib.createZstdCompress({ params: { [zlib.constants.ZSTD_c_compressionLevel]: 3 } })) : stream
    const started = Date.now()
    let response
    try {
      response = await fetch(base + '/responses', { method: 'POST', dispatcher: proxy, headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', 'content-encoding': encoding, ...(encoding === 'identity' ? { 'content-length': String(mib * 1024 * 1024) } : {}) }, body: payload, duplex: 'half', signal: AbortSignal.timeout(150_000) })
    } catch (error) {
      results.push({ encoding, decodedMiB: mib, transportError: error.cause?.code ?? error.code ?? 'FETCH_FAILED', ms: Date.now() - started })
      throw error
    }
    const json = await response.json()
    const valid = response.status === 400 && json.error?.message === 'request body must be a JSON object'
    results.push({ encoding, decodedMiB: mib, uploadTargetMs: duration, status: response.status, validationRejectedBeforeInference: valid, ms: Date.now() - started })
    if (!valid) throw new Error('unexpected result for ' + encoding + ': HTTP ' + response.status)
  }
  if (process.env.SIWC_MODEL_SMOKE === '1') {
    const auth = { authorization: 'Bearer ' + token }
    const models = await fetch(base + '/models', { dispatcher: proxy, headers: auth, signal: AbortSignal.timeout(45_000) })
    const directory = await models.json()
    results.push({ kind: 'model-catalog', status: models.status, models: directory.models?.length ?? directory.data?.length ?? 0, cache: models.headers.get('x-siwc-models-cache') })
    if (!models.ok) throw new Error('model catalog failed')
    const started = Date.now()
    const reply = await fetch(base + '/responses', { method: 'POST', dispatcher: proxy, headers: { ...auth, 'content-type': 'application/json' }, body: JSON.stringify({ model: 'gpt-6.1-sol', input: [{ role: 'user', content: [{ type: 'input_text', text: 'Reply exactly GATEWAY_V220_OK' }] }], instructions: '', reasoning: { effort: 'low' }, stream: true }), signal: AbortSignal.timeout(120_000) })
    const data = await reply.text()
    let text = ''; let completed = false
    for (const line of data.split('\n')) {
      if (!line.startsWith('data: ')) continue
      let event; try { event = JSON.parse(line.slice(6)) } catch { continue }
      if (event.type === 'response.output_text.delta') text += event.delta ?? ''
      if (event.type === 'response.completed') completed = true
    }
    const valid = reply.status === 200 && completed && text.trim() === 'GATEWAY_V220_OK'
    let diagnostic
    if (!valid) { try { const err = JSON.parse(data); diagnostic = String(err.error?.message ?? err.message ?? err.detail ?? JSON.stringify(err)).slice(0, 200) } catch { diagnostic = text.slice(0, 200) } }
    results.push({ kind: 'model-smoke', status: reply.status, completed, expectedReply: valid, diagnostic, ms: Date.now() - started })
    if (!valid) throw new Error('model smoke did not complete as expected')
  }
} finally { console.log(JSON.stringify(results, null, 2)); await proxy?.close() }
