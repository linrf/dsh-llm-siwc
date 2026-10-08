/**
 * Translate harness request messages into Responses API `input` items.
 *
 * Preview constraint (verified live): an explicit
 * `{type:"message", role:"system"}` item is REJECTED. The leading system
 * message must be lifted into top-level `instructions` instead.
 *
 * The same applies to tool results: they travel as `function_call_output`
 * items correlated by `call_id`, and assistant tool calls as `function_call`.
 */

/** Minimal structural views of the harness message vocabulary. */
export interface TextPart {
  type: string
  text?: string
  image?: unknown
  [key: string]: unknown
}

export interface HarnessMessage {
  role: string
  content?: string | TextPart[]
  toolCalls?: { id: string; name: string; arguments: string }[]
  toolCallId?: string
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

function textOf(content: string | TextPart[] | undefined): string {
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  return content
    .filter((part) => part.type === 'text' && typeof part.text === 'string')
    .map((part) => part.text as string)
    .join('')
}

/** Convert one content array into Responses content parts. */
function contentParts(content: string | TextPart[] | undefined): unknown[] {
  if (typeof content === 'string') {
    return content === '' ? [] : [{ type: 'input_text', text: content }]
  }
  if (!Array.isArray(content)) return []
  const parts: unknown[] = []
  for (const part of content) {
    if (part.type === 'text' && typeof part.text === 'string') {
      parts.push({ type: 'input_text', text: part.text })
    } else if (part.type === 'image' && part.image !== undefined) {
      // Images are passed through when the selected model accepts them.
      const image = part.image as Record<string, unknown>
      const url = typeof image.url === 'string' ? image.url : undefined
      const data = typeof image.data === 'string' ? image.data : undefined
      if (url) parts.push({ type: 'input_image', image_url: url })
      else if (data) parts.push({ type: 'input_image', image_url: data })
    }
  }
  return parts
}

/**
 * Convert harness messages into a Responses request body fragment.
 *
 * @param messages - ordered messages as the harness assembled them.
 * @param system - optional one-shot system prompt (already lifted by the loop).
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

    if (role === 'system' || role === 'developer') {
      // MUST NOT become an input item: explicit system items are rejected.
      const text = textOf(message.content)
      if (text.trim() !== '') instructionParts.push(text)
      continue
    }

    if (role === 'tool') {
      input.push({
        type: 'function_call_output',
        call_id: String(message.toolCallId ?? ''),
        output:
          typeof message.content === 'string'
            ? message.content
            : textOf(message.content),
      })
      continue
    }

    if (role === 'assistant') {
      if (Array.isArray(message.toolCalls)) {
        for (const call of message.toolCalls) {
          input.push({
            type: 'function_call',
            call_id: call.id,
            name: call.name,
            arguments: call.arguments || '{}',
          })
        }
      }
      const text = textOf(message.content)
      if (text.trim() !== '') {
        input.push({ role: 'assistant', content: [{ type: 'output_text', text }] })
      }
      continue
    }

    // user (and anything else the loop passes through) becomes an input message.
    const parts = contentParts(message.content)
    if (parts.length > 0) {
      input.push({ role: 'user', content: parts })
    }
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
