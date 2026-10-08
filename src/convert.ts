/**
 * Translate harness messages into Responses API `input` items.
 *
 * The harness message shape (verified against dsh-llm) is:
 *
 *   { role, content: ContentBlock[], ... }
 *
 * where a block is one of
 *   { type: 'text',       text }
 *   { type: 'reasoning',  text }
 *   { type: 'tool-call',  id, name, arguments }
 *   { type: 'image' | 'file', attachment }
 *
 * Tool invocations live INSIDE the content array. There is no message-level
 * `toolCalls` field; reading one yields nothing, which leaves the route with a
 * `function_call_output` and no matching `function_call`:
 *
 *     No tool call found for function call output with call_id call_…
 *
 * Tool results arrive as role:'tool' messages carrying `toolCallId`.
 *
 * Preview constraint: an explicit `{type:'message', role:'system'}` item is
 * REJECTED, so system/developer text is lifted into top-level `instructions`.
 */

/** Minimal structural views of the harness message vocabulary. */
export interface ContentBlockLike {
  type: string
  text?: string
  id?: string
  name?: string
  arguments?: string
  attachment?: unknown
  [key: string]: unknown
}

export interface HarnessMessage {
  role: string
  content?: string | ContentBlockLike[]
  /** Present on role:'tool' messages. */
  toolCallId?: string
  isError?: boolean
  [key: string]: unknown
}

export interface ResponsesInputItem {
  type?: string
  role?: string
  content?: unknown
  call_id?: string
  name?: string
  arguments?: string
  output?: string
}

export interface ConvertedRequest {
  instructions?: string
  input: ResponsesInputItem[]
}

function blocksOf(message: HarnessMessage): ContentBlockLike[] {
  if (Array.isArray(message.content)) return message.content as ContentBlockLike[]
  if (typeof message.content === 'string' && message.content !== '') {
    return [{ type: 'text', text: message.content }]
  }
  return []
}

function textOf(blocks: readonly ContentBlockLike[]): string {
  return blocks
    .filter((block) => block.type === 'text' && typeof block.text === 'string')
    .map((block) => block.text as string)
    .join('')
}

/**
 * Convert harness messages into a Responses request fragment.
 *
 * @param messages - ordered messages as the harness assembled them.
 * @param system - optional one-shot system prompt.
 * @returns `instructions` plus the `input` item list.
 */
export function convertMessages(
  messages: readonly HarnessMessage[],
  system?: string,
): ConvertedRequest {
  const instructionParts: string[] = []
  if (typeof system === 'string' && system.trim() !== '') instructionParts.push(system)

  const input: ResponsesInputItem[] = []

  for (const message of messages) {
    const role = message.role
    const blocks = blocksOf(message)

    if (role === 'system' || role === 'developer') {
      // MUST NOT become an input item: explicit system items are rejected.
      const text = textOf(blocks)
      if (text.trim() !== '') instructionParts.push(text)
      continue
    }

    if (role === 'tool') {
      input.push({
        type: 'function_call_output',
        call_id: String(message.toolCallId ?? ''),
        output: textOf(blocks),
      })
      continue
    }

    if (role === 'assistant') {
      // Tool invocations are content blocks, not a message-level field.
      for (const block of blocks) {
        if (block.type !== 'tool-call') continue
        input.push({
          type: 'function_call',
          call_id: String(block.id ?? ''),
          name: String(block.name ?? ''),
          arguments:
            typeof block.arguments === 'string' && block.arguments !== ''
              ? block.arguments
              : '{}',
        })
      }
      const text = textOf(blocks)
      if (text.trim() !== '') {
        input.push({ role: 'assistant', content: [{ type: 'output_text', text }] })
      }
      continue
    }

    // user (and anything else the loop passes through) becomes an input message.
    const parts: unknown[] = []
    for (const block of blocks) {
      if (block.type === 'text' && typeof block.text === 'string') {
        parts.push({ type: 'input_text', text: block.text })
      } else if (block.type === 'image') {
        // An image block carries an attachment reference that this route
        // cannot resolve without the attachment service.
        parts.push({ type: 'input_text', text: '[image]' })
      }
    }
    if (parts.length > 0) input.push({ role: 'user', content: parts })
  }

  const instructions = instructionParts.join('\n\n')
  return instructions === '' ? { input } : { instructions, input }
}

/** Convert harness tool schemas into Responses `tools`. */
export function convertTools(
  tools: readonly { name?: string; description?: string; parameters?: unknown; inputSchema?: unknown }[] | undefined,
): unknown[] | undefined {
  if (!tools || tools.length === 0) return undefined
  const converted = tools
    .filter((tool) => typeof tool.name === 'string' && tool.name !== '')
    .map((tool) => ({
      type: 'function',
      name: tool.name,
      ...(typeof tool.description === 'string' && tool.description !== ''
        ? { description: tool.description }
        : {}),
      parameters: tool.parameters ?? tool.inputSchema ?? { type: 'object', properties: {} },
    }))
  return converted.length > 0 ? converted : undefined
}
