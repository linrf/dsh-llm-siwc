/**
 * `LlmAdapter` implementation for the ChatGPT-plan Responses route.
 *
 * Maps the harness stream vocabulary onto Responses SSE events:
 *
 *   response.output_text.delta            -> text-delta
 *   response.output_item.added(function)  -> block-start(tool-call)
 *   response.function_call_arguments.delta-> tool-call-delta
 *   response.output_item.done(function)   -> block-end(tool-call)
 *   response.completed                    -> usage, finish
 *
 * Note `previous_response_id` is NOT used: this route rejects it over HTTP,
 * so the loop's derived history is replayed in full on every call.
 */

import { convertMessages, convertTools, type HarnessMessage } from './convert.ts'
import { streamResponses, ResponsesHttpError, type ResponsesEvent } from './client.ts'
import { classifyError, type ClassifiedError } from './errors.ts'

/** Structural view of the harness stream chunk vocabulary. */
export type StreamChunk =
  | { type: 'block-start'; index: number; blockType: string }
  | { type: 'text-delta'; index: number; text: string }
  | { type: 'reasoning-delta'; index: number; text: string }
  | { type: 'tool-call-delta'; index: number; id: string; name?: string; argumentsDelta: string }
  | { type: 'block-end'; index: number; block: unknown }
  | { type: 'usage'; usage: unknown }
  | { type: 'finish'; reason: string; replayState?: unknown }

/** Structural view of `GenerateOptions`. */
export interface GenerateOptionsLike {
  provider: string
  model: string
  reasoningEffort?: string
  messages: readonly HarnessMessage[]
  system?: string
  tools?: readonly { name?: string; description?: string; parameters?: unknown; inputSchema?: unknown }[]
  temperature?: number
  maxTokens?: number
  stop?: string[]
  signal?: AbortSignal
  sessionId?: string
  purpose?: string
}

export interface AdapterOptions {
  /** Provider route names this adapter owns. */
  providers: readonly string[]
  /** Resolve a currently valid OAuth access token. */
  resolveAccessToken: (provider: string) => Promise<string>
  /** Model ids to advertise in pickers. */
  models: readonly string[]
  /** Override for tests / self-hosted gateways. */
  baseUrl?: string
  fetchImpl?: typeof fetch
}

interface ToolBlock {
  id: string
  name: string
  arguments: string
}

/**
 * Adapter for one or more ChatGPT-plan provider routes.
 *
 * Deliberately not extending the harness base class here: the plugin wraps
 * this with the real `LlmAdapter` subclass at the composition boundary, which
 * keeps this module free of DSH compile-time types.
 */
export class SiwcResponsesAdapter {
  readonly #options: AdapterOptions

  constructor(options: AdapterOptions) {
    this.#options = options
  }

  providerInfo(provider: string): { id: string; name: string } {
    return { id: provider, name: 'ChatGPT' }
  }

  /**
   * Advertised models for one route.
   *
   * `provider` is a REQUIRED field of `LlmModelInfo`; omitting it makes the
   * model directory reject the whole catalog with
   * "adapter returned invalid or duplicate model metadata".
   */
  listModels(provider: string): readonly { provider: string; id: string; name: string }[] {
    return this.#options.models.map((id) => ({ provider, id, name: id }))
  }

  async resolveModel(
    provider: string,
    model: string,
  ): Promise<{ provider: string; id: string; name: string }> {
    return { provider, id: model, name: model }
  }

  /**
   * Stream one call, translating Responses SSE events to harness chunks.
   *
   * @param options - the assembled harness request.
   * @yields {StreamChunk} chunks in the order the harness expects.
   */
  async *stream(options: GenerateOptionsLike): AsyncGenerator<StreamChunk> {
    const accessToken = await this.#options.resolveAccessToken(options.provider)
    const converted = convertMessages(options.messages, options.system)
    const tools = convertTools(options.tools)

    let index = 0
    const toolBlocks = new Map<string, { index: number; block: ToolBlock }>()
    const textBlocks = new Set<number>()
    let finished = false

    const events = streamResponses(
      {
        model: options.model,
        apiKey: accessToken,
        instructions: converted.instructions,
        input: converted.input,
        tools,
        signal: options.signal,
      },
      { baseUrl: this.#options.baseUrl, fetchImpl: this.#options.fetchImpl },
    )

    try {
      for await (const event of events) {
        for (const chunk of this.#translate(event, {
          textBlocks,
          toolBlocks,
          nextIndex: () => index++,
        })) {
          if (chunk.type === 'finish') finished = true
          yield chunk
        }
      }
    } catch (error) {
      if (error instanceof ResponsesHttpError) {
        const classified: ClassifiedError = classifyError({
          status: error.status,
          body: safeJson(error.body),
          rawBody: error.body,
          requestId: error.requestId,
        })
        throw new ChatGptPlanError(classified)
      }
      throw error
    }

    if (!finished) {
      // The stream ended without a terminal event; surface it rather than
      // silently reporting success.
      throw new Error('responses stream ended without a terminal event')
    }
  }

  /** Translate one Responses event into zero or more harness chunks. */
  *#translate(
    event: ResponsesEvent,
    state: {
      textBlocks: Set<number>
      toolBlocks: Map<string, { index: number; block: ToolBlock }>
      nextIndex: () => number
    },
  ): Generator<StreamChunk> {
    switch (event.type) {
      case 'response.output_text.delta': {
        const text = typeof event.delta === 'string' ? event.delta : ''
        if (text === '') return
        const itemIndex = typeof event.output_index === 'number' ? event.output_index : 0
        if (!state.textBlocks.has(itemIndex)) {
          state.textBlocks.add(itemIndex)
          yield { type: 'block-start', index: itemIndex, blockType: 'text' }
        }
        yield { type: 'text-delta', index: itemIndex, text }
        return
      }

      case 'response.reasoning_summary_text.delta':
      case 'response.reasoning_text.delta': {
        const text = typeof event.delta === 'string' ? event.delta : ''
        if (text === '') return
        yield { type: 'reasoning-delta', index: this.#reasoningIndex(state), text }
        return
      }

      case 'response.output_item.added': {
        const item = event.item as Record<string, unknown> | undefined
        if (item?.type !== 'function_call') return
        const callId = String(item.call_id ?? item.id ?? '')
        const name = typeof item.name === 'string' ? item.name : ''
        const blockIndex = state.nextIndex()
        const block: ToolBlock = { id: callId, name, arguments: '' }
        state.toolBlocks.set(callId, { index: blockIndex, block })
        yield { type: 'block-start', index: blockIndex, blockType: 'tool-call' }
        yield {
          type: 'tool-call-delta',
          index: blockIndex,
          id: callId,
          name,
          argumentsDelta: '',
        }
        return
      }

      case 'response.function_call_arguments.delta': {
        const callId = typeof event.item_id === 'string' ? event.item_id : ''
        const entry = state.toolBlocks.get(callId)
        if (!entry) return
        const delta = typeof event.delta === 'string' ? event.delta : ''
        entry.block.arguments += delta
        if (delta !== '') {
          yield {
            type: 'tool-call-delta',
            index: entry.index,
            id: entry.block.id,
            argumentsDelta: delta,
          }
        }
        return
      }

      case 'response.output_item.done': {
        const item = event.item as Record<string, unknown> | undefined
        if (item?.type !== 'function_call') return
        const callId = String(item.call_id ?? item.id ?? '')
        const entry = state.toolBlocks.get(callId)
        if (!entry) return
        const finalArguments =
          typeof item.arguments === 'string' && item.arguments !== ''
            ? item.arguments
            : entry.block.arguments
        yield {
          type: 'block-end',
          index: entry.index,
          block: {
            type: 'tool-call',
            id: entry.block.id,
            name: typeof item.name === 'string' ? item.name : entry.block.name,
            arguments: finalArguments,
          },
        }
        state.toolBlocks.delete(callId)
        return
      }

      case 'response.output_text.done': {
        const itemIndex = typeof event.output_index === 'number' ? event.output_index : 0
        if (!state.textBlocks.has(itemIndex)) return
        const text = typeof event.text === 'string' ? event.text : ''
        yield { type: 'block-end', index: itemIndex, block: { type: 'text', text } }
        state.textBlocks.delete(itemIndex)
        return
      }

      case 'response.completed': {
        const response = event.response as Record<string, unknown> | undefined
        const usage = toTokenUsage(response?.usage)
        if (usage !== undefined) yield { type: 'usage', usage }
        yield { type: 'finish', reason: this.#finishReason(response) }
        return
      }

      case 'response.failed':
      case 'response.incomplete': {
        const response = event.response as Record<string, unknown> | undefined
        const error = response?.error ?? event
        const classified = classifyError({ status: 200, body: { error } })
        throw new ChatGptPlanError(classified)
      }

      case 'error': {
        const classified = classifyError({ status: 200, body: event })
        throw new ChatGptPlanError(classified)
      }

      default:
        return
    }
  }

  #reasoningIndex(state: { textBlocks: Set<number> }): number {
    // Reasoning precedes text; keep it on its own stable index.
    return state.textBlocks.size === 0 ? 0 : -1
  }

  #finishReason(response: Record<string, unknown> | undefined): string {
    const status = typeof response?.status === 'string' ? response.status : ''
    const reasons = response?.incomplete_details as Record<string, unknown> | undefined
    const reason = typeof reasons?.reason === 'string' ? reasons.reason : undefined
    if (status === 'incomplete' || reason === 'max_output_tokens') return 'length'
    return 'stop'
  }
}

/** A classified ChatGPT-plan failure, carrying the recovery action. */
export class ChatGptPlanError extends Error {
  readonly classified: ClassifiedError

  constructor(classified: ClassifiedError) {
    super(classified.message)
    this.name = 'ChatGptPlanError'
    this.classified = classified
  }
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return undefined
  }
}

/**
 * Convert a Responses usage object into the harness `TokenUsage` shape.
 *
 * The harness uses camelCase and requires `inputTokens`/`outputTokens`. Passing
 * the provider's snake_case object straight through leaves both undefined, and
 * the stored session then fails projection with:
 *
 *   {"path":["uncachedInputTokens"],"received":"NaN"},
 *   {"path":["outputTokens"],"received":"NaN"}
 *
 * Counts are DISJOINT in the harness: `inputTokens` is UNCACHED input, with
 * cache reads/writes reported separately.
 */
function toTokenUsage(raw: unknown): Record<string, number> | undefined {
  if (raw === null || typeof raw !== 'object') return undefined
  const usage = raw as Record<string, unknown>
  const inputDetails = (usage.input_tokens_details ?? {}) as Record<string, unknown>
  const outputDetails = (usage.output_tokens_details ?? {}) as Record<string, unknown>
  const num = (value: unknown): number =>
    typeof value === 'number' && Number.isFinite(value) ? value : 0

  const aggregateInput = num(usage.input_tokens)
  const cacheRead = num(inputDetails.cached_tokens)
  const cacheWrite = num(inputDetails.cache_write_tokens)
  const output = num(usage.output_tokens)
  const reasoning = num(outputDetails.reasoning_tokens)

  const result: Record<string, number> = {
    // Uncached input only; cached input is reported separately.
    inputTokens: Math.max(0, aggregateInput - cacheRead),
    outputTokens: output,
  }
  const total = num(usage.total_tokens)
  if (total > 0) result.totalTokens = total
  if (cacheRead > 0) result.cacheReadTokens = cacheRead
  if (cacheWrite > 0) result.cacheWriteTokens = cacheWrite
  if (reasoning > 0) result.reasoningTokens = reasoning
  return result
}
