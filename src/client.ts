/**
 * Streaming client for the ChatGPT-plan Responses route.
 *
 * Enforces the preview requirements on every request:
 *   - `store: false` and `stream: true` are mandatory
 *   - fields the route rejects are stripped before sending
 *
 * Using the public `api.openai.com/v1` endpoint (never `chatgpt.com/backend-api`).
 */

import { API_BASE_URL, RESPONSES_PATH } from './config.ts'
import { parseSseStream } from './sse.ts'
import { stripUnsupportedFields } from './errors.ts'

export interface ResponsesRequest {
  model: string
  /** OAuth access token for this call. */
  apiKey: string
  instructions?: string
  input: unknown[]
  tools?: unknown[]
  signal?: AbortSignal
}

export interface ResponsesEvent {
  type: string
  [key: string]: unknown
}

/** A non-2xx response, retaining both the status and the parsed body. */
export class ResponsesHttpError extends Error {
  readonly status: number
  readonly body: string
  readonly requestId: string | undefined

  constructor(status: number, body: string, requestId?: string) {
    super(`responses request failed with HTTP ${status}`)
    this.name = 'ResponsesHttpError'
    this.status = status
    this.body = body
    this.requestId = requestId
  }
}

function requestBody(request: ResponsesRequest): Record<string, unknown> {
  const body: Record<string, unknown> = {
    model: request.model,
    input: request.input,
    // Mandatory for this flow.
    store: false,
    stream: true,
  }
  if (request.instructions !== undefined && request.instructions !== '') {
    body.instructions = request.instructions
  }
  if (request.tools !== undefined && request.tools.length > 0) {
    body.tools = request.tools
  }
  // Defensive: nothing the route rejects may reach the wire.
  return stripUnsupportedFields(body)
}

/**
 * Open a streaming Responses request and yield its SSE events.
 *
 * @throws {ResponsesHttpError} when the response is not OK.
 */
export async function* streamResponses(
  request: ResponsesRequest,
  options: { baseUrl?: string; fetchImpl?: typeof fetch } = {},
): AsyncGenerator<ResponsesEvent> {
  const fetchImpl = options.fetchImpl ?? fetch
  const url = `${options.baseUrl ?? API_BASE_URL}${RESPONSES_PATH}`

  const response = await fetchImpl(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${request.apiKey ?? ''}`,
    },
    body: JSON.stringify(requestBody(request)),
    signal: request.signal,
  })

  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new ResponsesHttpError(
      response.status,
      body,
      response.headers.get('x-request-id') ?? undefined,
    )
  }
  if (!response.body) {
    throw new ResponsesHttpError(response.status, 'response carried no body')
  }

  for await (const event of parseSseStream(response.body)) {
    yield event as ResponsesEvent
  }
}
