/**
 * OAuth protocol layer for SIWC.
 *
 * Verified live 2026-10-08, including:
 *  - `dynamic_agent_client` is accepted by the authorize endpoint for
 *    first-time registration, with no client secret.
 *  - the callback returns the issued client id.
 *  - token exchange requires the ISSUED client id (not the dynamic one).
 *  - refresh omits `scope` to retain the existing grant.
 */

import {
  AUTHORIZE_URL,
  TOKEN_URL,
  OIDC_DISCOVERY_URL,
  API_BASE_URL,
  DYNAMIC_REGISTRATION_CLIENT_ID,
  REQUIRED_SCOPES,
} from './config.ts'
import type { TokenResponse } from './types.ts'

export interface AuthorizeParams {
  /** Omit to trigger first-time dynamic registration. */
  clientId?: string
  /** Stable per-host id. */
  extAgentHostId: string
  /** App name; only sent on initial dynamic registration. */
  agentNameHint?: string
  /** Id token from a previous sign-in; identifies the account. */
  idTokenHint?: string
  /** Optional email hint for returning sign-ins. */
  loginHint?: string
  redirectUri: string
  state: string
  nonce: string
  codeChallenge: string
  scope?: readonly string[]
  resource?: string
}

/** Build the authorize URL. Caller opens it in the system browser. */
export function buildAuthorizeUrl(params: AuthorizeParams): string {
  const isRegistration = params.clientId === undefined
  const url = new URL(AUTHORIZE_URL)
  const set = (key: string, value: string | undefined): void => {
    if (value !== undefined) url.searchParams.set(key, value)
  }

  set('client_id', isRegistration ? DYNAMIC_REGISTRATION_CLIENT_ID : params.clientId)
  // agent_name_hint only on initial dynamic registration.
  if (isRegistration) set('agent_name_hint', params.agentNameHint)
  set('ext_agent_host_id', params.extAgentHostId)
  set('id_token_hint', params.idTokenHint)
  set('login_hint', params.loginHint)
  set('response_type', 'code')
  set('redirect_uri', params.redirectUri)
  set('scope', (params.scope ?? REQUIRED_SCOPES).join(' '))
  set('resource', params.resource ?? API_BASE_URL)
  set('state', params.state)
  set('nonce', params.nonce)
  set('code_challenge_method', 'S256')
  set('code_challenge', params.codeChallenge)
  return url.toString()
}

export interface ExchangeParams {
  /** The ISSUED client id from the callback. */
  clientId: string
  code: string
  codeVerifier: string
  redirectUri: string
  resource?: string
}

interface FetchOptions {
  fetchImpl?: typeof fetch
  timeoutMs?: number
}

async function timedFetch(
  input: string,
  init: RequestInit,
  options: FetchOptions,
): Promise<Response> {
  const fetchImpl = options.fetchImpl ?? fetch
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), options.timeoutMs ?? 60_000)
  try {
    return await fetchImpl(input, { ...init, signal: controller.signal })
  } finally {
    clearTimeout(timer)
  }
}

/** Exchange an authorization code for tokens. */
export async function exchangeCode(
  params: ExchangeParams,
  options: FetchOptions = {},
): Promise<TokenResponse> {
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: params.clientId,
    code: params.code,
    code_verifier: params.codeVerifier,
    redirect_uri: params.redirectUri,
    resource: params.resource ?? API_BASE_URL,
  })
  const res = await timedFetch(
    TOKEN_URL,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
    },
    options,
  )
  const text = await res.text()
  if (!res.ok) {
    throw new OAuthError(`token exchange failed: HTTP ${res.status}`, text, res.status)
  }
  return parseTokenResponse(text)
}

export interface RefreshParams {
  /** The issued client id saved with this token set. */
  clientId: string
  refreshToken: string
  resource?: string
}

/** Refresh an access token. Omits `scope` so the existing grant is retained. */
export async function refreshToken(
  params: RefreshParams,
  options: FetchOptions = {},
): Promise<TokenResponse> {
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    client_id: params.clientId,
    refresh_token: params.refreshToken,
    resource: params.resource ?? API_BASE_URL,
  })
  const res = await timedFetch(
    TOKEN_URL,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
    },
    options,
  )
  const text = await res.text()
  if (!res.ok) {
    const code = extractErrorCode(text)
    throw new OAuthError(`token refresh failed: HTTP ${res.status}`, text, res.status, code)
  }
  return parseTokenResponse(text)
}

export interface RevokeParams {
  clientId: string
  refreshToken: string
}

/**
 * Revoke the renewable session via the discovery-documented endpoint.
 * An empty HTTP 200 is success, including for an already-invalid token.
 */
export async function revokeSession(
  params: RevokeParams,
  options: FetchOptions = {},
): Promise<void> {
  const endpoint = await resolveRevocationEndpoint(options)
  const body = new URLSearchParams({
    token: params.refreshToken,
    token_type_hint: 'refresh_token',
    client_id: params.clientId,
  })
  const res = await timedFetch(
    endpoint,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
    },
    options,
  )
  if (!res.ok) {
    throw new OAuthError(`revocation failed: HTTP ${res.status}`, await res.text(), res.status)
  }
}

/** Discover the revocation endpoint. */
export async function resolveRevocationEndpoint(options: FetchOptions = {}): Promise<string> {
  const res = await timedFetch(OIDC_DISCOVERY_URL, { method: 'GET' }, options)
  if (!res.ok) throw new OAuthError(`OIDC discovery failed: HTTP ${res.status}`, '', res.status)
  const doc = (await res.json()) as { revocation_endpoint?: string }
  if (!doc.revocation_endpoint) {
    throw new OAuthError('OIDC discovery returned no revocation_endpoint', '', 0)
  }
  return doc.revocation_endpoint
}

/** Machine-readable OAuth failure. */
export class OAuthError extends Error {
  readonly body: string
  readonly status: number
  readonly code: string | undefined

  constructor(message: string, body: string, status: number, code?: string) {
    super(message)
    this.name = 'OAuthError'
    this.body = body
    this.status = status
    this.code = code
  }
}

function parseTokenResponse(text: string): TokenResponse {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    throw new OAuthError('token response was not JSON', text, 0)
  }
  const response = parsed as TokenResponse
  if (!response.access_token) {
    throw new OAuthError('token response has no access_token', text, 0)
  }
  return response
}

/** Best-effort extraction of an OAuth error code from a body. */
export function extractErrorCode(body: string): string | undefined {
  try {
    const parsed = JSON.parse(body) as { error?: unknown }
    const error = parsed.error
    if (typeof error === 'string') return error
    if (error && typeof error === 'object' && 'code' in error) {
      return String((error as { code: unknown }).code)
    }
  } catch {
    // not JSON
  }
  return undefined
}

/**
 * Refresh-token failures that mean the token set is unusable and OAuth must
 * be repeated with the saved issued client id.
 */
export const UNUSABLE_REFRESH_CODES = new Set([
  'invalid_grant',
  'invalid_refresh_token',
  'token_expired',
  'refresh_token_expired',
  'refresh_token_invalidated',
  'refresh_token_reused',
])

export function isUnusableRefreshError(error: unknown): boolean {
  return (
    error instanceof OAuthError &&
    error.code !== undefined &&
    UNUSABLE_REFRESH_CODES.has(error.code)
  )
}
