/**
 * Server-Sent Events parser for a fetch response body.
 *
 * The Responses route emits `data: {...}` frames separated by blank lines.
 * A frame may also carry a `[DONE]` sentinel, which is dropped here.
 */

function isReadableStream(body: unknown): body is ReadableStream<Uint8Array> {
  return typeof (body as ReadableStream<Uint8Array>)?.getReader === 'function'
}

/**
 * Parse an SSE byte stream into JSON event objects.
 *
 * @param body - the response body stream.
 * @yields each parsed event payload in arrival order.
 */
export async function* parseSseStream(
  body: ReadableStream<Uint8Array> | AsyncIterable<Uint8Array>,
): AsyncGenerator<Record<string, unknown>> {
  const decoder = new TextDecoder()
  let buffer = ''

  const frames = function* (chunk: string): Generator<string> {
    buffer += chunk
    const lines = buffer.split('\n')
    buffer = lines.pop() ?? ''
    for (const line of lines) {
      const trimmed = line.trimEnd()
      if (!trimmed.startsWith('data:')) continue
      const payload = trimmed.slice(5).trim()
      if (payload === '' || payload === '[DONE]') continue
      yield payload
    }
  }

  const source: AsyncIterable<Uint8Array> = isReadableStream(body)
    ? (async function* () {
        const reader = body.getReader()
        try {
          for (;;) {
            const { done, value } = await reader.read()
            if (done) break
            if (value) yield value
          }
        } finally {
          reader.releaseLock()
        }
      })()
    : body

  for await (const chunk of source) {
    for (const payload of frames(decoder.decode(chunk, { stream: true }))) {
      try {
        const parsed = JSON.parse(payload) as Record<string, unknown>
        if (parsed && typeof parsed === 'object') yield parsed
      } catch {
        // A malformed frame must not abort the whole stream.
      }
    }
  }

  // Flush any trailing frame that lacked a newline.
  for (const payload of frames('\n')) {
    try {
      const parsed = JSON.parse(payload) as Record<string, unknown>
      if (parsed && typeof parsed === 'object') yield parsed
    } catch {
      // ignore
    }
  }
}
