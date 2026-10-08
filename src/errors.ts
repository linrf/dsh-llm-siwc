/**
 * Error classification for the SIWC Responses route.
 *
 * Two distinct failure surfaces (from the official error docs):
 *  1. Direct-route admission BEFORE a stream opens. It may return
 *     `{"detail":"..."}` — diagnostic text, NOT a stable machine code.
 *  2. A structured Responses error object with `error.code`.
 *
 * OpenAI does not silently fall back to another billing path; the caller
 * must decide what to do.
 */

import { PLAN_USAGE_SCOPE } from './config.ts'

/** How the caller should react to a failure. */
export type ErrorAction =
  | 'retry-backoff'
  | 'pause-account'
  | 'ineligible'
  | 'fix-request'
  | 'reauth'
  | 'fix-client'
  | 'fatal'

export interface ClassifiedError {
  action: ErrorAction
  /** HTTP status when known. */
  status?: number
  /** Stable machine code when the server supplied one. */
  code?: string
  /** Value of `error.param` when supplied (unsupported_capability). */
  param?: string
  /** Human-facing message. */
  message: string
  /** Preserved for diagnostics; OpenAI asks that request ids be kept. */
  requestId?: string
  /** Where the user can review usage, when relevant. */
  usageUrl?: string
}

const USAGE_URL = 'https://chatgpt.com/settings/usage'

/** Structured Responses error codes and their required recovery. */
const RESPONSES_CODES: Record<string, { action: ErrorAction; status: number }> = {
  subscription_sharing_user_not_eligible: { action: 'ineligible', status: 403 },
  subscription_sharing_usage_limit_exceeded: { action: 'pause-account', status: 429 },
  subscription_sharing_usage_unavailable: { action: 'retry-backoff', status: 503 },
  subscription_sharing_unsupported_capability: { action: 'fix-request', status: 400 },
  subscription_sharing_route_not_supported: { action: 'fix-request', status: 403 },
  subscription_sharing_invalid_user: { action: 'reauth', status: 401 },
  chatpass_v2_scope_not_authorized: { action: 'fix-client', status: 403 },
  chatpass_v2_invalid_authorization_context: { action: 'fix-client', status: 403 },
  subscription_sharing_user_unavailable: { action: 'retry-backoff', status: 503 },
}

/** Direct-route admission statuses, before a stream opens. */
const ADMISSION_STATUS: Record<number, ErrorAction> = {
  401: 'reauth',
  403: 'ineligible',
  503: 'retry-backoff',
}

export interface ClassifyInput {
  status: number
  /** Parsed body when it was JSON. */
  body?: unknown
  /** Raw body text, for the `{"detail":...}` admission shape. */
  rawBody?: string
  /** Response request id header, when present. */
  requestId?: string
}

/**
 * Classify a failure into a recovery action.
 *
 * Never assumes every failure carries a standard `error` object.
 */
export function classifyError(input: ClassifyInput): ClassifiedError {
  const base: ClassifiedError = {
    action: 'fatal',
    status: input.status,
    message: `request failed with HTTP ${input.status}`,
    requestId: input.requestId,
  }

  // Structured Responses error.
  const structured = extractStructuredError(input.body)
  if (structured) {
    const known = structured.code ? RESPONSES_CODES[structured.code] : undefined
    const action = known?.action ?? statusToAction(input.status)
    return {
      ...base,
      action,
      code: structured.code,
      param: structured.param,
      message: structured.message ?? base.message,
      usageUrl: structured.code === 'subscription_sharing_usage_limit_exceeded' ? USAGE_URL : undefined,
    }
  }

  // Direct-route admission: `{"detail":"..."}` is diagnostic text.
  const detail = extractDetail(input.body, input.rawBody)
  if (detail) {
    return {
      ...base,
      action: statusToAction(input.status),
      message: detail,
    }
  }

  return { ...base, action: statusToAction(input.status) }
}

function statusToAction(status: number): ErrorAction {
  return ADMISSION_STATUS[status] ?? (status >= 500 ? 'retry-backoff' : 'fatal')
}

interface StructuredError {
  code?: string
  param?: string
  message?: string
}

/** Pull `error.code` / `error.param` from a Responses error body. */
function extractStructuredError(body: unknown): StructuredError | null {
  if (!body || typeof body !== 'object') return null
  const record = body as Record<string, unknown>
  const error = record.error
  if (error && typeof error === 'object') {
    const e = error as Record<string, unknown>
    const code = typeof e.code === 'string' ? e.code : undefined
    const param = typeof e.param === 'string' ? e.param : undefined
    const message = typeof e.message === 'string' ? e.message : undefined
    if (code || param || message) return { code, param, message }
  }
  // A `response.failed` stream event embeds the error under `response.error`.
  const response = record.response
  if (response && typeof response === 'object') {
    const r = response as Record<string, unknown>
    if (r.error) return extractStructuredError({ error: r.error })
  }
  return null
}

function extractDetail(body: unknown, rawBody?: string): string | null {
  if (body && typeof body === 'object') {
    const detail = (body as Record<string, unknown>).detail
    if (typeof detail === 'string') return detail
  }
  if (rawBody && rawBody.trim() && !rawBody.trimStart().startsWith('{')) {
    return rawBody.trim().slice(0, 500)
  }
  return null
}

/**
 * Whether a granted scope set actually permits ChatGPT plan usage.
 *
 * A valid ID token alone does NOT authorize plan usage — the scope must be
 * present. When absent, sign-in is retained but plan usage is disabled.
 */
export function canUsePlan(scopes: readonly string[]): boolean {
  return scopes.includes(PLAN_USAGE_SCOPE)
}

/**
 * Fields the SIWC Responses route rejects. Sending any of them fails with
 * `subscription_sharing_unsupported_capability`.
 */
export const UNSUPPORTED_RESPONSE_FIELDS = [
  'background',
  'conversation',
  'max_output_tokens',
  'max_tool_calls',
  'metadata',
  'moderation',
  'multi_agent',
  'prompt',
  'prompt_cache_retention',
  'safety_identifier',
  'temperature',
  'top_logprobs',
  'top_p',
  'truncation',
  'user',
] as const

/** Strip unsupported fields from a Responses request body. */
export function stripUnsupportedFields<T extends Record<string, unknown>>(body: T): T {
  const out = { ...body }
  for (const field of UNSUPPORTED_RESPONSE_FIELDS) delete out[field]
  return out
}
