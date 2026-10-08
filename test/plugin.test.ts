import { test } from 'node:test'
import assert from 'node:assert/strict'
import { convertMessages, convertTools } from '../src/convert.ts'
import { parseSseStream } from '../src/sse.ts'
import { SiwcResponsesAdapter } from '../src/adapter.ts'

// ---------- message conversion ----------

test('a leading system message becomes instructions, never an input item', () => {
  const result = convertMessages([
    { role: 'system', content: 'You are terse.' },
    { role: 'user', content: 'hi' },
  ])
  assert.equal(result.instructions, 'You are terse.')
  assert.equal(result.input.length, 1)
  assert.equal(result.input[0]?.role, 'user')
  // The rejected shape must never appear.
  assert.ok(!result.input.some((item) => item.role === 'system'))
})

test('a one-shot system string merges with system messages', () => {
  const result = convertMessages([{ role: 'system', content: 'A' }], 'B')
  assert.equal(result.instructions, 'B\n\nA')
})

test('developer messages are lifted like system messages', () => {
  const result = convertMessages([
    { role: 'developer', content: 'dev rules' },
    { role: 'user', content: 'x' },
  ])
  assert.equal(result.instructions, 'dev rules')
  assert.equal(result.input.length, 1)
})

test('user text becomes an input_text part', () => {
  const result = convertMessages([{ role: 'user', content: 'hello' }])
  assert.deepEqual(result.input[0], {
    role: 'user',
    content: [{ type: 'input_text', text: 'hello' }],
  })
})

test('assistant tool calls become function_call items', () => {
  const result = convertMessages([
    {
      role: 'assistant',
      content: '',
      toolCalls: [{ id: 'call_1', name: 'bash', arguments: '{"cmd":"ls"}' }],
    },
  ])
  assert.deepEqual(result.input[0], {
    type: 'function_call',
    call_id: 'call_1',
    name: 'bash',
    arguments: '{"cmd":"ls"}',
  })
})

test('tool results become function_call_output items keyed by call id', () => {
  const result = convertMessages([
    { role: 'tool', toolCallId: 'call_1', content: 'file list' },
  ])
  assert.deepEqual(result.input[0], {
    type: 'function_call_output',
    call_id: 'call_1',
    output: 'file list',
  })
})

test('a full assistant-tool-user round trip keeps order', () => {
  const result = convertMessages([
    { role: 'system', content: 'sys' },
    { role: 'user', content: 'do it' },
    { role: 'assistant', content: '', toolCalls: [{ id: 'c1', name: 'f', arguments: '{}' }] },
    { role: 'tool', toolCallId: 'c1', content: 'ok' },
  ])
  assert.deepEqual(
    result.input.map((item) => item.type ?? item.role),
    ['user', 'function_call', 'function_call_output'],
  )
})

test('empty content produces no empty input items', () => {
  const result = convertMessages([
    { role: 'user', content: '' },
    { role: 'assistant', content: '' },
  ])
  assert.equal(result.input.length, 0)
})

test('tool schemas convert to Responses function tools', () => {
  const tools = convertTools([
    { name: 'bash', description: 'run', parameters: { type: 'object', properties: {} } },
  ])
  assert.deepEqual(tools, [
    {
      type: 'function',
      name: 'bash',
      description: 'run',
      parameters: { type: 'object', properties: {} },
    },
  ])
})

test('nameless tools are dropped and an empty list is undefined', () => {
  assert.equal(convertTools([]), undefined)
  assert.equal(convertTools(undefined), undefined)
  const tools = convertTools([{ description: 'no name' }])
  assert.equal(tools, undefined)
})

// ---------- SSE parsing ----------

function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
      controller.close()
    },
  })
}

test('SSE data frames are parsed and [DONE] dropped', async () => {
  const events: Record<string, unknown>[] = []
  for await (const event of parseSseStream(streamOf([
    'data: {"type":"response.created"}\n\n',
    'data: {"type":"response.completed"}\n\n',
    'data: [DONE]\n\n',
  ]))) events.push(event)
  assert.deepEqual(events.map((event) => event.type), ['response.created', 'response.completed'])
})

test('a frame split across chunks is reassembled', async () => {
  const events: Record<string, unknown>[] = []
  for await (const event of parseSseStream(streamOf([
    'data: {"type":"resp',
    'onse.output_text.delta","delta":"hi"}\n\n',
  ]))) events.push(event)
  assert.equal(events.length, 1)
  assert.equal(events[0]?.delta, 'hi')
})

test('a malformed frame does not abort the stream', async () => {
  const events: Record<string, unknown>[] = []
  for await (const event of parseSseStream(streamOf([
    'data: {not json}\n\n',
    'data: {"type":"response.completed"}\n\n',
  ]))) events.push(event)
  assert.deepEqual(events.map((event) => event.type), ['response.completed'])
})

// ---------- adapter stream mapping ----------

function fakeResponse(frames: string[], status = 200): Response {
  return new Response(streamOf(frames), { status })
}

test('text deltas map to block-start + text-delta + block-end + finish', async () => {
  const frames = [
    'data: {"type":"response.output_text.delta","output_index":0,"delta":"Hel"}\n\n',
    'data: {"type":"response.output_text.delta","output_index":0,"delta":"lo"}\n\n',
    'data: {"type":"response.output_text.done","output_index":0,"text":"Hello"}\n\n',
    'data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":2}}}\n\n',
  ]
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    fetchImpl: (async () => fakeResponse(frames)) as unknown as typeof fetch,
  })

  const chunks = []
  for await (const chunk of adapter.stream({
    provider: 'chatgpt',
    model: 'm',
    messages: [{ role: 'user', content: 'hi' }],
  })) chunks.push(chunk)

  assert.deepEqual(chunks.map((chunk) => chunk.type), [
    'block-start', 'text-delta', 'text-delta', 'block-end', 'usage', 'finish',
  ])
  assert.equal(chunks[1]?.type === 'text-delta' ? chunks[1].text : '', 'Hel')
  assert.equal(chunks[5]?.type === 'finish' ? chunks[5].reason : '', 'stop')
})

test('a function call maps to a tool-call block with streamed arguments', async () => {
  const frames = [
    'data: {"type":"response.output_item.added","item":{"type":"function_call","call_id":"c1","id":"i1","name":"bash"}}\n\n',
    'data: {"type":"response.function_call_arguments.delta","item_id":"c1","delta":"{\\"cmd\\":"}\n\n',
    'data: {"type":"response.function_call_arguments.delta","item_id":"c1","delta":"\\"ls\\"}"}\n\n',
    'data: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"c1","id":"i1","name":"bash","arguments":"{\\"cmd\\":\\"ls\\"}"}}\n\n',
    'data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":5,"output_tokens":4}}}\n\n',
  ]
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    fetchImpl: (async () => fakeResponse(frames)) as unknown as typeof fetch,
  })

  const chunks = []
  for await (const chunk of adapter.stream({
    provider: 'chatgpt',
    model: 'm',
    messages: [{ role: 'user', content: 'list files' }],
    tools: [{ name: 'bash', parameters: { type: 'object', properties: {} } }],
  })) chunks.push(chunk)

  assert.deepEqual(chunks.map((chunk) => chunk.type), [
    'block-start', 'tool-call-delta', 'tool-call-delta', 'tool-call-delta', 'block-end', 'usage', 'finish',
  ])
  const end = chunks.find((chunk) => chunk.type === 'block-end')
  assert.ok(end && end.type === 'block-end')
  assert.deepEqual(end.block, {
    type: 'tool-call',
    id: 'c1',
    name: 'bash',
    arguments: '{"cmd":"ls"}',
  })
})

test('a stream without a terminal event is an error, not a silent success', async () => {
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    fetchImpl: (async () => fakeResponse([
      'data: {"type":"response.output_text.delta","output_index":0,"delta":"x"}\n\n',
    ])) as unknown as typeof fetch,
  })

  await assert.rejects(async () => {
    for await (const _chunk of adapter.stream({
      provider: 'chatgpt',
      model: 'm',
      messages: [{ role: 'user', content: 'hi' }],
    })) { /* drain */ }
  }, /terminal event/)
})

test('the request body carries store:false, stream:true and no rejected fields', async () => {
  let captured: Record<string, unknown> | undefined
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    fetchImpl: (async (_url: string, init: { body: string }) => {
      captured = JSON.parse(init.body) as Record<string, unknown>
      return fakeResponse([
        'data: {"type":"response.completed","response":{"status":"completed"}}\n\n',
      ])
    }) as unknown as typeof fetch,
  })

  for await (const _chunk of adapter.stream({
    provider: 'chatgpt',
    model: 'm',
    messages: [{ role: 'user', content: 'hi' }],
    temperature: 0.5,
    maxTokens: 10,
  })) { /* drain */ }

  assert.equal(captured?.store, false)
  assert.equal(captured?.stream, true)
  assert.equal(captured?.temperature, undefined)
  assert.equal(captured?.max_output_tokens, undefined)
})

test('a 429 with a usage-limit code surfaces a classified pause action', async () => {
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    fetchImpl: (async () => new Response(
      JSON.stringify({ error: { code: 'subscription_sharing_usage_limit_exceeded', message: 'limit' } }),
      { status: 429 },
    )) as unknown as typeof fetch,
  })

  await assert.rejects(async () => {
    for await (const _chunk of adapter.stream({
      provider: 'chatgpt',
      model: 'm',
      messages: [{ role: 'user', content: 'hi' }],
    })) { /* drain */ }
  }, (error: Error & { classified?: { action?: string } }) => {
    assert.equal(error.classified?.action, 'pause-account')
    return true
  })
})
