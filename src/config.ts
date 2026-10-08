/**
 * Protocol constants and configuration schema.
 *
 * Every value here was verified against the live SIWC OSS flow on 2026-10-08.
 */

/** Loopback callback. Docs: "Do not substitute with `localhost`". */
export const DEFAULT_CALLBACK_HOST = '127.0.0.1'
export const DEFAULT_CALLBACK_PORT = 1455
export const CALLBACK_PATH = '/auth/callback'

/** OAuth endpoints. */
export const AUTHORIZE_URL = 'https://auth.openai.com/api/accounts/authorize'
export const TOKEN_URL = 'https://auth.openai.com/api/accounts/oauth/token'
export const OIDC_DISCOVERY_URL = 'https://auth.openai.com/.well-known/openid-configuration'
/** JWKS is resolved from discovery; this is only a fallback. */
export const JWKS_URL_FALLBACK = 'https://auth.openai.com/.well-known/jwks.json'

/** Public API surface (NOT the chatgpt.com/backend-api route). */
export const API_BASE_URL = 'https://api.openai.com/v1'
export const RESPONSES_PATH = '/responses'
export const MODELS_PATH = '/models'

/**
 * The first-time registration entrypoint. This is NOT a client id to save
 * or to use for token exchange.
 */
export const DYNAMIC_REGISTRATION_CLIENT_ID = 'dynamic_agent_client'

/** Default agent name shown to the user during registration. */
export const DEFAULT_AGENT_NAME = 'DeepSeek Harness'

/**
 * Full requested scope set. `chatgpt.tokens.use.direct` is the one that
 * actually grants ChatGPT plan usage — without it, sign-in still succeeds
 * but inference through the plan is not permitted.
 */
export const REQUIRED_SCOPES = [
  'openid',
  'profile',
  'email',
  'offline_access',
  'resource.invoke',
  'chatgpt.tokens.use.direct',
] as const

/** Scope that gates ChatGPT plan usage. */
export const PLAN_USAGE_SCOPE = 'chatgpt.tokens.use.direct'

/** Access token lifetime observed live. */
export const ACCESS_TOKEN_TTL_MS = 3_600_000

export interface SiwcConfig {
  /** App name shown to the user at registration. */
  agentName: string
  /** Loopback host; keep 127.0.0.1. */
  callbackHost: string
  /** Preferred loopback port. */
  callbackPort: number
  /** Where credentials are persisted. */
  storeDir: string
  /**
   * Refresh this many ms before expiry. The token response also carries
   * `earliest_refresh_at`, which takes precedence when present.
   */
  refreshLeadMs: number
  /** Request timeout for OAuth/API calls. */
  timeoutMs: number
}

export function resolveConfig(partial: Partial<SiwcConfig> = {}): SiwcConfig {
  return {
    agentName: partial.agentName ?? DEFAULT_AGENT_NAME,
    callbackHost: partial.callbackHost ?? DEFAULT_CALLBACK_HOST,
    callbackPort: partial.callbackPort ?? DEFAULT_CALLBACK_PORT,
    storeDir: partial.storeDir ?? defaultStoreDir(),
    refreshLeadMs: partial.refreshLeadMs ?? 300_000,
    timeoutMs: partial.timeoutMs ?? 60_000,
  }
}

function defaultStoreDir(): string {
  const home = process.env.DSH_HOME ?? `${process.env.HOME ?? ''}/.dsh`
  return `${home}/siwc`
}

/** Build the redirect URI. Only the port may vary between deployments. */
export function redirectUri(config: Pick<SiwcConfig, 'callbackHost' | 'callbackPort'>): string {
  return `http://${config.callbackHost}:${config.callbackPort}${CALLBACK_PATH}`
}
