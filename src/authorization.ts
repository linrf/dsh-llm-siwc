/**
 * Authorization orchestration — the P0 core.
 *
 * Implements the full happy path verified live on 2026-10-08:
 *   dynamic registration -> browser consent -> loopback callback ->
 *   state check -> code exchange -> ID-token verification -> scope check ->
 *   credential persistence.
 *
 * Also implements refresh (serialized per registration) and sign-out with
 * remote revocation.
 */

import { loadOrCreateHostId } from './host-id.ts'
import { createPkce, randomToken, decodeJwtPayload } from './crypto.ts'
import { startCallbackServer } from './callback.ts'
import { buildAuthorizeUrl, exchangeCode, refreshToken, revokeSession, isUnusableRefreshError } from './oauth.ts'
import { verifyIdToken } from './verify.ts'
import { PLAN_USAGE_SCOPE, ACCESS_TOKEN_TTL_MS, API_BASE_URL, resolveConfig, type SiwcConfig } from './config.ts'
import type { CredentialStore } from './store.ts'
import { SystemBrowserLauncher, type BrowserLauncher } from './browser.ts'
import type {
  AuthorizationResult,
  SiwcCredential,
  TokenResponse,
} from './types.ts'
import { join } from 'node:path'

export interface AuthorizeDeps {
  store: CredentialStore
  browser?: BrowserLauncher
  config?: Partial<SiwcConfig>
  fetchImpl?: typeof fetch
  /** Test seam: the live authorize URL is passed here before opening. */
  onAuthorizeUrl?: (url: string, isRegistration: boolean) => void
  signal?: AbortSignal
  /** Test seam: skip actually opening a browser. */
  openBrowser?: boolean
}

export interface AuthorizeRequest {
  /** Existing registration to reauthorize. Omit to register a new account. */
  existingClientId?: string
  /** Retained id token for a returning sign-in. */
  idTokenHint?: string
  /** Optional email hint for a returning sign-in. */
  loginHint?: string
}

/**
 * Run one authorization attempt.
 *
 * One attempt per registration at a time is the caller's responsibility
 * (mirrors `AuthorizationService.begin` semantics).
 */
export async function authorize(
  request: AuthorizeRequest,
  deps: AuthorizeDeps,
): Promise<AuthorizationResult> {
  const config = resolveConfig(deps.config)
  const browser = deps.browser ?? new SystemBrowserLauncher()
  const hostId = await loadOrCreateHostId(join(config.storeDir, 'host_id'))

  // Reuse the stored id token when reauthorizing a known registration.
  let idTokenHint = request.idTokenHint
  if (request.existingClientId && !idTokenHint) {
    const existing = await deps.store.get(request.existingClientId)
    idTokenHint = existing?.idToken
  }

  const { verifier, challenge } = createPkce()
  const state = randomToken(16)
  const nonce = randomToken(16)

  const callback = await startCallbackServer({
    host: config.callbackHost,
    port: config.callbackPort,
    signal: deps.signal,
  })

  try {
    const authorizeUrl = buildAuthorizeUrl({
      clientId: request.existingClientId,
      extAgentHostId: hostId,
      agentNameHint: request.existingClientId ? undefined : config.agentName,
      idTokenHint,
      loginHint: request.loginHint,
      redirectUri: callback.redirectUri,
      state,
      nonce,
      codeChallenge: challenge,
      resource: API_BASE_URL,
    })

    const isRegistration = request.existingClientId === undefined
    deps.onAuthorizeUrl?.(authorizeUrl, isRegistration)
    if (deps.openBrowser !== false) await browser.open(authorizeUrl)

    const result = await callback.waitForResult

    // CSRF check — must happen before any use of the code.
    if (result.state !== state) {
      throw new Error('authorization state mismatch; the callback was not bound to this attempt')
    }

    // The issued client id wins; a mismatched one is a hard failure.
    const issuedClientId = result.clientId ?? request.existingClientId
    if (!issuedClientId) {
      throw new Error('registration incomplete: the callback carried no issued client_id')
    }
    if (result.clientId && request.existingClientId && result.clientId !== request.existingClientId) {
      throw new Error(
        'callback returned a different client_id than the pending registration; refusing to replace it',
      )
    }

    const tokens = await exchangeCode(
      {
        clientId: issuedClientId,
        code: result.code,
        codeVerifier: verifier,
        redirectUri: callback.redirectUri, // exact same URI as the authorize request
        resource: API_BASE_URL,
      },
      { fetchImpl: deps.fetchImpl, timeoutMs: config.timeoutMs },
    )

    if (!tokens.id_token) throw new Error('token response carried no id_token')

    // Signature, issuer, audience (array!), expiry, and nonce.
    const claims = await verifyIdToken(tokens.id_token, {
      clientId: issuedClientId,
      nonce,
      fetchImpl: deps.fetchImpl,
    })

    const credential = toCredential({
      tokens,
      claims: { sub: claims.sub, email: claims.email ?? null, iss: claims.iss },
      clientId: issuedClientId,
      hostId,
    })

    await deps.store.save(credential)

    return {
      status: 'authorized',
      credential,
      planUsageEnabled: credential.scopes.includes(PLAN_USAGE_SCOPE),
    }
  } finally {
    callback.close()
  }
}

interface CredentialInput {
  tokens: TokenResponse
  claims: { sub: string; email: string | null; iss: string }
  clientId: string
  hostId: string
}

function toCredential(input: CredentialInput): SiwcCredential {
  const { tokens } = input
  const scopes = (tokens.scope ?? '').split(' ').filter(Boolean)
  const earliest = tokens.earliest_refresh_at ? Date.parse(tokens.earliest_refresh_at) : null
  return {
    email: input.claims.email,
    issuer: input.claims.iss,
    subject: input.claims.sub,
    clientId: input.clientId,
    extAgentHostId: input.hostId,
    idToken: tokens.id_token ?? '',
    accessToken: tokens.access_token,
    refreshToken: tokens.refresh_token ?? '',
    tokenType: tokens.token_type,
    scopes,
    expiresAt: Date.now() + (tokens.expires_in ?? 0) * 1000,
    earliestRefreshAt: Number.isFinite(earliest) ? earliest : null,
    savedAt: new Date().toISOString(),
  }
}

/**
 * Serialize refreshes per registration.
 *
 * Refresh tokens rotate: two concurrent refreshes race and one of them
 * invalidates the other. Every refresh for a client id goes through one chain.
 */
const refreshChains = new Map<string, Promise<SiwcCredential>>()

export interface RefreshDeps {
  store: CredentialStore
  config?: Partial<SiwcConfig>
  fetchImpl?: typeof fetch
}

/**
 * Return a usable access token, refreshing when the current one is at or
 * past its refresh point.
 */
export async function ensureFreshCredential(
  clientId: string,
  deps: RefreshDeps,
): Promise<SiwcCredential> {
  const credential = await deps.store.get(clientId)
  if (!credential) throw new Error(`no stored credential for client ${clientId}`)

  const config = resolveConfig(deps.config)
  const refreshAt =
    credential.earliestRefreshAt ?? credential.expiresAt - config.refreshLeadMs
  if (Date.now() < refreshAt) return credential

  const previous = refreshChains.get(clientId) ?? Promise.resolve(credential)
  const next = previous
    .catch(() => credential) // a failed predecessor must not poison the chain
    .then(() => performRefresh(credential, deps))
  refreshChains.set(
    clientId,
    next.finally(() => {
      if (refreshChains.get(clientId) === next) refreshChains.delete(clientId)
    }),
  )
  return next
}

async function performRefresh(
  current: SiwcCredential,
  deps: RefreshDeps,
): Promise<SiwcCredential> {
  // Re-read: another process may have rotated the token already.
  const latest = (await deps.store.get(current.clientId)) ?? current
  const config = resolveConfig(deps.config)

  try {
    const tokens = await refreshToken(
      {
        clientId: latest.clientId,
        refreshToken: latest.refreshToken,
        resource: API_BASE_URL,
      },
      { fetchImpl: deps.fetchImpl, timeoutMs: config.timeoutMs },
    )

    // Identity must not silently change across a refresh.
    let claims: { sub: string; email: string | null; iss: string } = {
      sub: latest.subject,
      email: latest.email,
      iss: latest.issuer,
    }
    if (tokens.id_token) {
      const verified = await verifyIdToken(tokens.id_token, {
        clientId: latest.clientId,
        fetchImpl: deps.fetchImpl,
      })
      if (verified.sub !== latest.subject) {
        throw new Error('refreshed ID token belongs to a different account; refusing to replace')
      }
      claims = { sub: verified.sub, email: verified.email ?? latest.email, iss: verified.iss }
    }

    const updated = toCredential({
      tokens: { ...tokens, refresh_token: tokens.refresh_token ?? latest.refreshToken },
      claims,
      clientId: latest.clientId,
      hostId: latest.extAgentHostId,
    })
    await deps.store.save(updated)
    return updated
  } catch (error) {
    if (isUnusableRefreshError(error)) {
      // The token set is dead; the caller must re-run authorize() with the
      // saved client id. Keep the record so reauth can reuse the client id.
      throw new UnusableCredentialError(latest.clientId, error)
    }
    throw error
  }
}

/** The stored token set can no longer be refreshed; reauthorize. */
export class UnusableCredentialError extends Error {
  readonly clientId: string
  readonly cause: unknown

  constructor(clientId: string, cause: unknown) {
    super(`credential for ${clientId} is no longer refreshable; sign in again`)
    this.name = 'UnusableCredentialError'
    this.clientId = clientId
    this.cause = cause
  }
}

export interface SignOutDeps {
  store: CredentialStore
  config?: Partial<SiwcConfig>
  fetchImpl?: typeof fetch
}

/**
 * Sign out: revoke the renewable session, then clear local tokens.
 *
 * Revocation does NOT delete the registered client — the account/client
 * mapping and the host id are retained for a later sign-in.
 */
export async function signOut(clientId: string, deps: SignOutDeps): Promise<void> {
  const credential = await deps.store.get(clientId)
  if (!credential) return
  const config = resolveConfig(deps.config)

  try {
    await revokeSession(
      { clientId: credential.clientId, refreshToken: credential.refreshToken },
      { fetchImpl: deps.fetchImpl, timeoutMs: config.timeoutMs },
    )
    await deps.store.remove(clientId)
  } catch (error) {
    // Local sign-out proceeds regardless; surface that revocation is unconfirmed.
    await deps.store.remove(clientId)
    throw new Error(
      `signed out locally, but remote revocation was not confirmed: ${String(error)}`,
    )
  }
}

/** True when the credential may be used for ChatGPT plan inference. */
export function planUsageEnabled(credential: SiwcCredential): boolean {
  return credential.scopes.includes(PLAN_USAGE_SCOPE)
}

export { ACCESS_TOKEN_TTL_MS, decodeJwtPayload }
