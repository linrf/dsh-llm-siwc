/**
 * Credential and flow-result types.
 *
 * The record shape mirrors the official SIWC credential example, extended
 * with fields this implementation needs (earliestRefreshAt, grantedAt).
 */

/** One issued-client registration plus its live token set. */
export interface SiwcCredential {
  /** Display only — NOT an identity key. May repeat across registrations. */
  email: string | null
  /** ID-token issuer, e.g. https://auth.openai.com */
  issuer: string
  /** Validated `id_token.sub`; the account identity for this registration. */
  subject: string
  /** Issued OAuth client id (`oaiapp_…`). NEVER `dynamic_agent_client`. */
  clientId: string
  /** Stable per-host identifier. Opaque, not a credential. */
  extAgentHostId: string
  /** Retained so later sign-ins can pass `id_token_hint`. */
  idToken: string
  accessToken: string
  refreshToken: string
  tokenType: string
  /** Granted scopes, from the space-separated `scope` response field. */
  scopes: string[]
  /** Epoch ms when the access token expires. */
  expiresAt: number
  /** Epoch ms from `earliest_refresh_at` when provided. */
  earliestRefreshAt: number | null
  savedAt: string
}

/** Raw token-endpoint response. */
export interface TokenResponse {
  access_token: string
  refresh_token?: string
  id_token?: string
  token_type: string
  expires_in: number
  scope?: string
  earliest_refresh_at?: string
}

/** Result of a completed authorization attempt. */
export interface AuthorizationResult {
  status: 'authorized'
  credential: SiwcCredential
  /** True when the granted scopes permit ChatGPT plan usage. */
  planUsageEnabled: boolean
}

export type AuthorizationFailure = {
  status: 'cancelled' | 'failed'
  reason: string
  code?: string
}

/** Validated identity extracted from an ID token. */
export interface IdTokenClaims {
  iss: string
  sub: string
  aud: string | string[]
  email?: string
  exp: number
  iat: number
  nonce?: string
}
