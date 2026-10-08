import { test } from 'node:test'
import assert from 'node:assert/strict'
import { convertMessages, convertTools } from '../src/convert.ts'
import { parseSseStream } from '../src/sse.ts'
import { SiwcResponsesAdapter } from '../src/adapter.ts'
import { ModelCatalog, FALLBACK_CATALOG, orderLevels } from '../src/catalog.ts'

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
      content: [{ type: 'tool-call', id: 'call_1', name: 'bash', arguments: '{"cmd":"ls"}' }],
    },
  ])
  assert.deepEqual(result.input[0], {
    type: 'function_call',
    call_id: 'call_1',
    name: 'bash',
    arguments: '{"cmd":"ls"}',
  })
})

test('an assistant tool call paired with its result stays ordered', () => {
  const result = convertMessages([
    { role: 'assistant', content: [{ type: 'tool-call', id: 'c9', name: 'read', arguments: '{}' }] },
    { role: 'tool', toolCallId: 'c9', content: [{ type: 'text', text: 'contents' }] },
  ])
  assert.deepEqual(result.input, [
    { type: 'function_call', call_id: 'c9', name: 'read', arguments: '{}' },
    { type: 'function_call_output', call_id: 'c9', output: 'contents' },
  ])
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
    { role: 'system', content: [{ type: 'text', text: 'sys' }] },
    { role: 'user', content: [{ type: 'text', text: 'do it' }] },
    { role: 'assistant', content: [{ type: 'tool-call', id: 'c1', name: 'f', arguments: '{}' }] },
    { role: 'tool', toolCallId: 'c1', content: [{ type: 'text', text: 'ok' }] },
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

test('session cache routing stays stable across requests and separate across sessions', async () => {
  const captured: { headers: Headers; body: Record<string, unknown> }[] = []
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    fetchImpl: async (_url, init) => {
      captured.push({
        headers: new Headers(init?.headers),
        body: JSON.parse(String(init?.body)) as Record<string, unknown>,
      })
      return fakeResponse([
        'data: {"type":"response.completed","response":{"status":"completed"}}\n\n',
      ])
    },
  })

  const sessionIds = ['session-alpha', 'session-alpha', 'session-beta', undefined, '']
  for (const sessionId of sessionIds) {
    for await (const _chunk of adapter.stream({
      provider: 'chatgpt',
      model: 'm',
      messages: [{ role: 'user', content: 'hi' }],
      sessionId,
    })) { /* drain */ }
  }

  assert.deepEqual(
    captured.map(({ headers }) => headers.get('session-id')),
    ['session-alpha', 'session-alpha', 'session-beta', null, null],
  )
  for (const { headers, body } of captured) {
    assert.equal(headers.get('authorization'), 'Bearer token')
    assert.equal(headers.get('content-type'), 'application/json')
    assert.equal(body.store, false)
    assert.equal(body.stream, true)
    assert.equal('sessionId' in body, false)
    assert.equal('session-id' in body, false)
  }
  assert.ok(captured.every(({ body }) => JSON.stringify(body) === JSON.stringify(captured[0]?.body)))
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

test('usage is converted to the harness shape (camelCase, disjoint input)', async () => {
  const frames = [
    'data: {"type":"response.completed","response":{"status":"completed","usage":' +
      '{"input_tokens":100,"input_tokens_details":{"cached_tokens":40,"cache_write_tokens":5},' +
      '"output_tokens":20,"output_tokens_details":{"reasoning_tokens":7},"total_tokens":120}}}\n\n',
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

  const usageChunk = chunks.find((chunk) => chunk.type === 'usage')
  assert.ok(usageChunk && usageChunk.type === 'usage', 'a usage chunk must be emitted')
  const usage = usageChunk.usage as Record<string, unknown>

  // Provider snake_case must not leak; the harness reads camelCase and would
  // otherwise store NaN and fail session projection.
  assert.equal(usage.input_tokens, undefined)
  assert.equal(usage.output_tokens, undefined)

  assert.equal(usage.inputTokens, 60) // 100 aggregate - 40 cached
  assert.equal(usage.outputTokens, 20)
  assert.equal(usage.totalTokens, 120)
  assert.equal(usage.cacheReadTokens, 40)
  assert.equal(usage.cacheWriteTokens, 5)
  assert.equal(usage.reasoningTokens, 7)
})

test('a usage object with missing counts still yields finite numbers', async () => {
  const frames = [
    'data: {"type":"response.completed","response":{"status":"completed","usage":{}}}\n\n',
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

  const usageChunk = chunks.find((chunk) => chunk.type === 'usage')
  assert.ok(usageChunk && usageChunk.type === 'usage')
  const usage = usageChunk.usage as Record<string, number>
  assert.equal(usage.inputTokens, 0)
  assert.equal(usage.outputTokens, 0)
  assert.ok(Number.isFinite(usage.inputTokens) && Number.isFinite(usage.outputTokens))
})

test('an image block with a resolved data URL becomes input_image', () => {
  const result = convertMessages([
    {
      role: 'user',
      content: [
        { type: 'text', text: 'what is this?' },
        {
          type: 'image',
          attachment: { attachmentId: 'a1', width: 10, height: 10 },
          dataUrl: 'data:image/png;base64,AAAA',
        },
      ],
    },
  ])
  assert.deepEqual(result.input[0], {
    role: 'user',
    content: [
      { type: 'input_text', text: 'what is this?' },
      { type: 'input_image', image_url: 'data:image/png;base64,AAAA' },
    ],
  })
})

test('an unresolved image block degrades to a placeholder, not a dropped turn', () => {
  const result = convertMessages([
    {
      role: 'user',
      content: [{ type: 'image', attachment: { attachmentId: 'a1', width: 10, height: 10 } }],
    },
  ])
  assert.deepEqual(result.input[0], {
    role: 'user',
    content: [{ type: 'input_text', text: '[image unavailable]' }],
  })
})

test('the selected reasoning effort reaches the request body', async () => {
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
    reasoningEffort: 'high',
  })) { /* drain */ }

  assert.deepEqual(captured?.reasoning, { effort: 'high' })
})

test('reasoning effort "off" omits the field entirely', async () => {
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
    reasoningEffort: 'off',
  })) { /* drain */ }

  assert.equal(captured?.reasoning, undefined)
})

test('the route refuses to retry a usage-limit 429', () => {
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
  })
  const policy = adapter.providerRetryPolicy()
  assert.equal(policy.retryableCodes.includes('RATE_LIMIT'), false)
  assert.ok(policy.retryableCodes.includes('SERVER'))
})

test('resolved model metadata advertises reasoning, context, and max tokens', async () => {
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
  })
  const info = await adapter.resolveModel('chatgpt', 'gpt-6-luna')
  assert.equal(info.context?.contextWindow, 272_000)
  assert.equal(info.defaultMaxTokens, 128_000)
  assert.deepEqual(info.inputModalities, ['text', 'image'])
  assert.ok((info.reasoning?.efforts.length ?? 0) > 0)
  assert.equal(info.reasoning?.defaultEffort, 'high')
})

// ---------- live model catalog ----------

/** One entry in the shape the route actually returns. */
const liveEntry = (slug: string, extra: Record<string, unknown> = {}) => ({
  slug,
  display_name: slug === 'gpt-6-luna' ? 'GPT-6-Luna' : slug,
  description: `${slug} description`,
  context_window: 272000,
  max_context_window: 872000,
  input_modalities: ['text', 'image'],
  default_reasoning_level: 'low',
  supported_reasoning_levels: [{ effort: 'low' }, { effort: 'xhigh' }, { effort: 'ultra' }],
  ...extra,
})

const catalogWith = (models: unknown[], opts: Record<string, unknown> = {}) =>
  new ModelCatalog({
    resolveAccessToken: async () => 'token',
    fetchImpl: (async () =>
      new Response(JSON.stringify({ models }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })) as unknown as typeof fetch,
    ...opts,
  })

test('the catalog reads the route\'s real fields, not a hardcoded list', async () => {
  const catalog = catalogWith([liveEntry('gpt-6-luna')])
  const models = await catalog.load()
  assert.equal(models.length, 1)
  const [model] = models
  assert.equal(model?.slug, 'gpt-6-luna')
  assert.equal(model?.displayName, 'GPT-6-Luna')
  assert.equal(model?.description, 'gpt-6-luna description')
  assert.equal(model?.contextWindow, 272000)
  assert.equal(model?.maxContextWindow, 872000)
  assert.deepEqual(model?.inputModalities, ['text', 'image'])
})

test('per-model reasoning levels survive, including xhigh/max/ultra', async () => {
  const catalog = catalogWith([liveEntry('gpt-6-luna')])
  const [model] = await catalog.load()
  // The route lists these; a shared hardcoded set would drop them.
  assert.deepEqual(model?.reasoningLevels, ['low', 'xhigh', 'ultra'])
  // Reordered into the route's natural progression.
  assert.deepEqual(orderLevels(['ultra', 'low', 'high', 'max']), ['low', 'high', 'max', 'ultra'])
})

test('resolveModel publishes exactly the levels that model accepts', async () => {
  const catalog = catalogWith([
    liveEntry('gpt-6.1-sol'),
    liveEntry('gpt-5.5', { supported_reasoning_levels: [{ effort: 'low' }, { effort: 'xhigh' }] }),
  ])
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: [],
    resolveAccessToken: async () => 'token',
    catalog,
  })
  const wide = await adapter.resolveModel('chatgpt', 'gpt-6.1-sol')
  assert.deepEqual(wide.reasoning?.efforts.map((e) => e.id), ['low', 'xhigh', 'ultra'])
  const narrow = await adapter.resolveModel('chatgpt', 'gpt-5.5')
  assert.deepEqual(narrow.reasoning?.efforts.map((e) => e.id), ['low', 'xhigh'])
  // A model the route does not serve must not inherit another model's levels.
  assert.equal(narrow.name, 'gpt-5.5')
})

test('listModels advertises the live catalog with display names', async () => {
  const catalog = catalogWith([liveEntry('gpt-6-luna'), liveEntry('gpt-reserve')])
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: [],
    resolveAccessToken: async () => 'token',
    catalog,
  })
  const models = await adapter.listModels('chatgpt')
  assert.deepEqual(models.map((m) => m.id), ['gpt-6-luna', 'gpt-reserve'])
  assert.equal(models[0]?.provider, 'chatgpt')
  assert.equal(models[0]?.name, 'GPT-6-Luna')
  assert.deepEqual(models[0]?.inputModalities, ['text', 'image'])
})

test('a catalog read failure keeps the picker usable instead of emptying it', async () => {
  const catalog = new ModelCatalog({
    resolveAccessToken: async () => {
      throw new Error('no credential')
    },
    seed: FALLBACK_CATALOG,
  })
  const models = await catalog.load()
  assert.equal(models.length, FALLBACK_CATALOG.length)
  // The seed is deliberately conservative: text+image and common levels only.
  assert.deepEqual(catalog.snapshot()[0]?.reasoningLevels, ['low', 'medium', 'high'])
})

test('a stale catalog is not refetched while its TTL holds', async () => {
  let calls = 0
  const catalog = new ModelCatalog({
    resolveAccessToken: async () => 'token',
    ttlMs: 60_000,
    fetchImpl: (async () => {
      calls += 1
      return new Response(JSON.stringify({ models: [liveEntry('gpt-6-luna')] }), { status: 200 })
    }) as unknown as typeof fetch,
  })
  await catalog.load()
  await catalog.load()
  assert.equal(calls, 1)
})

test('a reasoning summary is requested, so the thinking stream has content', async () => {
  let captured: Record<string, unknown> | undefined
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    reasoningSummary: 'auto',
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
    reasoningEffort: 'high',
  })) { /* drain */ }
  assert.deepEqual(captured?.reasoning, { effort: 'high', summary: 'auto' })
})

test('reasoningSummary "none" keeps the request to the bare effort', async () => {
  let captured: Record<string, unknown> | undefined
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    reasoningSummary: 'none',
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
    reasoningEffort: 'high',
  })) { /* drain */ }
  assert.deepEqual(captured?.reasoning, { effort: 'high' })
})

test('a failed refresh does not become an unhandled rejection', async () => {
  // The harness turns any unhandled rejection into a fatal load failure that
  // exits the application, so a dead credential must stay contained here.
  const rejections: unknown[] = []
  const onRejection = (reason: unknown) => rejections.push(reason)
  process.on('unhandledRejection', onRejection)

  const { ensureFreshCredential } = await import('../src/authorization.ts')
  const expired = {
    clientId: 'oaiapp_test',
    subject: 's',
    email: 'e@example.com',
    scopes: [],
    accessToken: 'old',
    refreshToken: 'dead',
    idToken: '',
    expiresAt: Date.now() - 60_000,
    createdAt: Date.now() - 3_600_000,
    extAgentHostId: 'urn:uuid:test',
  }
  const store = {
    get: async () => expired,
    list: async () => [expired],
    save: async () => {},
    remove: async () => {},
  }

  await assert.rejects(
    ensureFreshCredential('oaiapp_test', {
      store: store as never,
      config: { storeDir: '/tmp', callbackHost: '127.0.0.1', callbackPort: 1455, refreshLeadMs: 300_000, provider: 'chatgpt' } as never,
      fetchImpl: (async () =>
        new Response(JSON.stringify({ error: 'invalid_grant' }), { status: 400 })) as unknown as typeof fetch,
    }),
  )

  // Let microtasks and the rejection tracker settle.
  await new Promise((resolve) => setTimeout(resolve, 50))
  process.off('unhandledRejection', onRejection)
  assert.deepEqual(rejections, [])
})

test('a fresh sign-in wins even when the stale credential comes first', async () => {
  // Both credentials stay on disk after a re-registration, and readdir order is
  // not a contract — picking the stale one looks like an auth failure right
  // after a successful sign-in.
  const mk = (clientId: string, createdAt: number) => ({
    clientId,
    subject: clientId,
    email: `${clientId}@example.com`,
    scopes: ['chatgpt.tokens.use.direct'],
    accessToken: 'a',
    refreshToken: 'r',
    idToken: '',
    expiresAt: createdAt + 3_600_000,
    createdAt,
    extAgentHostId: 'urn:uuid:x',
  })
  const stale = mk('oaiapp_stale', 1_000)
  const fresh = mk('oaiapp_fresh', 2_000)

  const { newestCredential } = await import('../src/credentials.ts')

  // Stale first, as the directory happened to return it.
  assert.equal(newestCredential([stale, fresh])?.clientId, 'oaiapp_fresh')
  // And the reverse order must not change the answer.
  assert.equal(newestCredential([fresh, stale])?.clientId, 'oaiapp_fresh')
  // Records that are not SIWC registrations are never selected.
  assert.equal(newestCredential([{ clientId: 'other', createdAt: 9_999 }]), null)
})

test('Fast mode sends service_tier=priority', async () => {
  let captured: Record<string, unknown> | undefined
  const adapter = new SiwcResponsesAdapter({
    providers: ['chatgpt'],
    models: ['m'],
    resolveAccessToken: async () => 'token',
    serviceTier: 'priority',
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
  })) { /* drain */ }
  assert.equal(captured?.service_tier, 'priority')
})

test('Fast mode is off by default, so the field is absent', async () => {
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
  })) { /* drain */ }
  assert.equal(captured?.service_tier, undefined)
})
