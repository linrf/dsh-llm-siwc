import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Readable, Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { createStreamingNormalizer } from './streaming-json.mjs'

const unsupported = ['metadata', 'temperature', 'background', 'max_output_tokens']
async function normalize(input, chunkSize = 65536, options = {}) {
  const bytes = Buffer.isBuffer(input) ? input : Buffer.from(input)
  const normalizer = createStreamingNormalizer({ unsupported, ...options })
  const chunks = []
  async function* source() { for (let at = 0; at < bytes.length; at += chunkSize) yield bytes.subarray(at, at + chunkSize) }
  await pipeline(Readable.from(source()), normalizer, new Writable({ write(chunk, _encoding, done) { chunks.push(chunk); done() } }))
  return { text: Buffer.concat(chunks).toString('utf8'), normalizer }
}
const expected = (value) => {
  const result = { ...value }
  for (const field of unsupported) delete result[field]
  result.store = false
  result.stream = true
  return result
}

test('rewrites only top-level fields and preserves all retained item IDs and lexical bytes', async () => {
  const source = '{ "model" : "fixture", "metadata":{"keep":"no"}, "input":[{"id":"msg_123","call_id":"call_1","store":true,"metadata":"keep","content":"x"}], "store": true, "stream": false, "prompt_cache_key":"session-1" }'
  const result = await normalize(source, 1)
  assert.deepEqual(JSON.parse(result.text), expected(JSON.parse(source)))
  assert.equal(result.normalizer.model, 'fixture')
  assert.equal(result.normalizer.bytes, Buffer.byteLength(result.text))
  const lexical = '{"big":90071992547409931234567890,"negativeZero":-0,"exp":1e999,"escaped":"\\u0061\\/","model":"fixture"}'
  const actual = (await normalize(lexical, 2)).text
  assert.equal(actual, lexical.slice(0, -1) + ',"store":false,"stream":true}')
})

test('empty objects, duplicate keys and escaped omitted names have JSON last-value semantics', async () => {
  for (const source of ['{}', ' \n { }\r\t', '{"sto\\u0072e":true,"stream":false}', '{"x":1,"x":2,"store":true,"store":null,"model":"old","model":"new"}']) {
    const actual = await normalize(source, 1)
    assert.deepEqual(JSON.parse(actual.text), expected(JSON.parse(source)))
  }
  assert.equal((await normalize('{"model":"old","model":null}')).normalizer.model, undefined)
})

test('chunk boundaries preserve UTF-8, escape sequences, unpaired escaped surrogates and long keys', async () => {
  const key = 'k'.repeat(70000) + '😀'
  const value = 'a'.repeat(65535) + '😀' + '\\"\n\t\u0000' + '\ud800' + '\udc00' + '中'
  const source = JSON.stringify({ model: 'fixture', input: value, [key]: value, store: true, stream: false })
  for (const chunkSize of [1, 7, 65536]) {
    assert.equal((await normalize(source, chunkSize)).text, JSON.stringify(expected(JSON.parse(source))))
  }
})

test('all nested values including omitted fields are strictly validated', async () => {
  const invalid = ['', '[]', 'null', 'true', '1', '"x"', '{', '{}{}', '{"x":}', '{,}', '{"x":1,}', '{"x":[1,]}', '{"x":01}', '{"x":-}', '{"x":1.}', '{"x":1e}', '{"x":1e+}', '{"x":+1}', '{"x":NaN}', '{"x":Infinity}', '{"x":tru}', '{"x":truefalse}', '{"x":"\\q"}', '{"x":"\\u0x00"}', '{"x":"\n"}', '{"x":"unterminated}', '{"metadata":[1,]}', '{"metadata":"\\q"}', '{"x":1 "y":2}', '{"x" 1}', '{"x":[]]}', '{"x":null}:', '{"metadata":{[}}']
  for (const source of invalid) {
    if (!['[]', 'null', 'true', '1', '"x"'].includes(source)) assert.throws(() => JSON.parse(source))
    for (const chunkSize of [1, 17]) await assert.rejects(normalize(source, chunkSize), { status: 400 }, source)
  }
})

test('malformed UTF-8 is rejected including split sequences and discarded fields', async () => {
  for (const bytes of [[0xc0, 0xaf], [0xed, 0xa0, 0x80], [0xf4, 0x90, 0x80, 0x80], [0xc2], [0x80]]) {
    const body = Buffer.concat([Buffer.from('{"metadata":"'), Buffer.from(bytes), Buffer.from('"}')])
    for (const chunkSize of [1, 65536]) await assert.rejects(normalize(body, chunkSize), { status: 400 })
  }
  assert.equal(JSON.parse((await normalize('{"input":"é中😀"}', 1)).text).input, 'é中😀')
})

test('depth and normalized byte limits fail with 413', async () => {
  await assert.rejects(normalize('{"input":"some text"}', 1, { maxBytes: 15 }), { status: 413 })
  await assert.rejects(normalize('{"x":[[[0]]]}', 1, { maxDepth: 3 }), { status: 413 })
  assert.deepEqual(JSON.parse((await normalize('{"x":[[0]]}', 1, { maxDepth: 3 })).text), { x: [[0]], store: false, stream: true })
  await assert.rejects(normalize('{"metadata":[[[0]]]}', 1, { maxDepth: 3 }), { status: 413 })
})

test('forwards a prefix immediately but withholds a complete root until validated EOF', async () => {
  const normalizer = createStreamingNormalizer({ unsupported })
  const chunks = []
  normalizer.on('data', chunk => chunks.push(chunk))
  normalizer.write('{"input":"prefix')
  assert.equal(Buffer.concat(chunks).toString(), '{"input":"prefix')
  normalizer.write(' suffix"}')
  assert.equal(Buffer.concat(chunks).toString(), '{"input":"prefix suffix"')
  normalizer.end(' \n')
  await new Promise((resolve, reject) => { normalizer.once('end', resolve); normalizer.once('error', reject) })
  assert.equal(Buffer.concat(chunks).toString(), '{"input":"prefix suffix","store":false,"stream":true}')
})

test('a large value and huge omitted string are processed chunk by chunk with bounded buffering', async () => {
  const normalizer = createStreamingNormalizer({ unsupported })
  let maximumBuffered = 0
  let total = 0
  const sink = new Writable({ highWaterMark: 1024, write(chunk, _encoding, done) {
    total += chunk.length
    maximumBuffered = Math.max(maximumBuffered, normalizer.readableLength, normalizer.writableLength)
    setImmediate(done)
  } })
  async function* source() {
    yield Buffer.from('{"input":"')
    const chunk = Buffer.alloc(65536, 'x')
    for (let i = 0; i < 800; i++) yield chunk
    yield Buffer.from('","metadata":"')
    for (let i = 0; i < 100; i++) yield chunk
    yield Buffer.from('"}')
  }
  await pipeline(Readable.from(source()), normalizer, sink)
  assert.ok(total > 50 * 1024 * 1024)
  assert.ok(total < 50 * 1024 * 1024 + 100)
  assert.ok(maximumBuffered <= 2 * 65536 + 256, String(maximumBuffered))
  assert.equal(normalizer.keyParts.length, 0)
  assert.equal(normalizer.capture, null)
  assert.equal(normalizer.stack.length, 0)
})

test('random valid JSON and deterministic malformed mutations agree with JSON.parse', async () => {
  let seed = 0xcafebabe
  const random = () => { seed = (1664525 * seed + 1013904223) >>> 0; return seed / 2 ** 32 }
  const strings = ['plain', '\\"\n\u0000', '中😀', '\ud800', 'metadata', 'store', '__proto__', '']
  const generate = (depth = 0) => {
    const type = depth > 3 ? Math.floor(random() * 4) : Math.floor(random() * 6)
    if (type === 0) return null
    if (type === 1) return random() < 0.5
    if (type === 2) return (random() - 0.5) * 1e10
    if (type === 3) return strings[Math.floor(random() * strings.length)]
    if (type === 4) return Array.from({ length: Math.floor(random() * 5) }, () => generate(depth + 1))
    return Object.fromEntries(Array.from({ length: Math.floor(random() * 5) }, (_, i) => [strings[Math.floor(random() * strings.length)] + i, generate(depth + 1)]))
  }
  for (let i = 0; i < 250; i++) {
    const value = { input: generate(), metadata: generate(), store: generate(), model: i % 3 ? 'fixture' : generate() }
    const source = JSON.stringify(value)
    const chunkSize = 1 + Math.floor(random() * 31)
    assert.deepEqual(JSON.parse((await normalize(source, chunkSize)).text), expected(value))
    const position = Math.floor(random() * source.length)
    const mutated = source.slice(0, position) + source.slice(position + 1)
    let parsed
    try { parsed = JSON.parse(mutated) } catch { await assert.rejects(normalize(mutated, chunkSize), { status: 400 }); continue }
    assert.deepEqual(JSON.parse((await normalize(mutated, chunkSize)).text), expected(parsed))
  }
})
