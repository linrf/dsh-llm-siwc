/**
 * ID-token verification.
 *
 * Docs require: verify the signature against OpenAI's published JWKS, then
 * check issuer, audience (against the issued client id), expiration, and the
 * nonce saved for the attempt.
 *
 * Verified live detail: the ID token's `aud` is an ARRAY (["oaiapp_…"]),
 * while the access token's `aud` is the STRING "https://api.openai.com/v1".
 * Comparing either one as the wrong shape fails validation.
 */

import { createPublicKey, verify as cryptoVerify, type KeyObject } from 'node:crypto'
import { OIDC_DISCOVERY_URL, JWKS_URL_FALLBACK } from './config.ts'
import { decodeJwtPayload } from './crypto.ts'
import type { IdTokenClaims } from './types.ts'

export const ISSUER = 'https://auth.openai.com'

interface Jwk {
  kty: string
  kid?: string
  use?: string
  alg?: string
  n?: string
  e?: string
  crv?: string
  x?: string
  y?: string
}

export interface VerifyIdTokenOptions {
  /** Expected audience: the issued client id. */
  clientId: string
  /** The nonce generated for this attempt. */
  nonce?: string
  /** Clock skew allowance in seconds. */
  clockToleranceSec?: number
  /** Injectable for tests. */
  fetchImpl?: typeof fetch
  /** Injectable for tests. */
  now?: () => number
}

/** JWKS cache with a short TTL; key rotation is expected to be rare. */
class JwksCache {
  #keys: Jwk[] = []
  #fetchedAt = 0
  #ttlMs = 3_600_000
  #inflight: Promise<Jwk[]> | null = null
  #fetchImpl: typeof fetch

  constructor(fetchImpl: typeof fetch) {
    this.#fetchImpl = fetchImpl
  }

  async keys(forceRefresh = false): Promise<Jwk[]> {
    if (!forceRefresh && this.#keys.length > 0 && Date.now() - this.#fetchedAt < this.#ttlMs) {
      return this.#keys
    }
    if (this.#inflight) return this.#inflight
    this.#inflight = this.#load().finally(() => {
      this.#inflight = null
    })
    return this.#inflight
  }

  async #load(): Promise<Jwk[]> {
    let uri = JWKS_URL_FALLBACK
    try {
      const res = await this.#fetchImpl(OIDC_DISCOVERY_URL)
      if (res.ok) {
        const doc = (await res.json()) as { jwks_uri?: string }
        if (doc.jwks_uri) uri = doc.jwks_uri
      }
    } catch {
      // Fall through to the known-good fallback.
    }
    const res = await this.#fetchImpl(uri)
    if (!res.ok) throw new Error(`JWKS fetch failed: HTTP ${res.status}`)
    const body = (await res.json()) as { keys?: Jwk[] }
    this.#keys = body.keys ?? []
    this.#fetchedAt = Date.now()
    return this.#keys
  }
}

const jwksCaches = new WeakMap<typeof fetch, JwksCache>()

function cacheFor(fetchImpl: typeof fetch): JwksCache {
  let cache = jwksCaches.get(fetchImpl)
  if (!cache) {
    cache = new JwksCache(fetchImpl)
    jwksCaches.set(fetchImpl, cache)
  }
  return cache
}

function jwkToKey(jwk: Jwk): KeyObject {
  const key = createPublicKey({ key: jwk as never, format: 'jwk' })
  return key
}

/** Convert a JOSE ES256 signature (r||s) into the DER form node expects. */
function joseToDer(signature: Buffer): Buffer {
  const size = 32
  if (signature.length !== size * 2) return signature
  const r = signature.subarray(0, size)
  const s = signature.subarray(size)
  const encode = (part: Buffer): Buffer => {
    let i = 0
    while (i < part.length - 1 && part[i] === 0) i++
    let v = part.subarray(i)
    if (v[0] & 0x80) v = Buffer.concat([Buffer.from([0]), v])
    return v
  }
  const rEnc = encode(r)
  const sEnc = encode(s)
  const len = 2 + rEnc.length + 2 + sEnc.length
  return Buffer.concat([
    Buffer.from([0x30, len]),
    Buffer.from([0x02, rEnc.length]), rEnc,
    Buffer.from([0x02, sEnc.length]), sEnc,
  ])
}

function verifySignature(token: string, jwk: Jwk): boolean {
  const [headerB64, payloadB64, signatureB64] = token.split('.')
  const signed = Buffer.from(`${headerB64}.${payloadB64}`, 'ascii')
  const signature = Buffer.from(signatureB64, 'base64url')
  const key = jwkToKey(jwk)

  if (jwk.kty === 'RSA') {
    return cryptoVerify('sha256', signed, key, signature)
  }
  if (jwk.kty === 'EC') {
    if (cryptoVerify('sha256', signed, { key, dsaEncoding: 'ieee-p1363' }, signature)) return true
    return cryptoVerify('sha256', signed, key, joseToDer(signature))
  }
  if (jwk.kty === 'OKP') {
    return cryptoVerify(null, signed, key, signature)
  }
  throw new Error(`unsupported JWK key type: ${jwk.kty}`)
}

/**
 * Verify an ID token and return its validated claims.
 *
 * @throws when the signature, issuer, audience, expiry, or nonce is invalid.
 */
export async function verifyIdToken(
  token: string,
  options: VerifyIdTokenOptions,
): Promise<IdTokenClaims> {
  const fetchImpl = options.fetchImpl ?? fetch
  const now = options.now ?? (() => Date.now())
  const tolerance = options.clockToleranceSec ?? 60

  const [headerB64] = token.split('.')
  const header = JSON.parse(Buffer.from(headerB64, 'base64url').toString('utf8')) as {
    kid?: string
    alg?: string
  }
  const claims = decodeJwtPayload(token) as unknown as IdTokenClaims

  const cache = cacheFor(fetchImpl)
  const verifyWith = async (forceRefresh: boolean): Promise<boolean> => {
    const keys = await cache.keys(forceRefresh)
    const candidates = header.kid
      ? keys.filter((k) => k.kid === header.kid)
      : keys.filter((k) => !header.alg || !k.alg || k.alg === header.alg)
    for (const jwk of candidates) {
      try {
        if (verifySignature(token, jwk)) return true
      } catch {
        // try the next key
      }
    }
    return false
  }

  let signatureOk = await verifyWith(false)
  if (!signatureOk) signatureOk = await verifyWith(true) // key rotation
  if (!signatureOk) throw new Error('ID token signature verification failed')

  if (claims.iss !== ISSUER) {
    throw new Error(`ID token issuer mismatch: expected ${ISSUER}, got ${claims.iss}`)
  }

  // aud is an array in the live ID token; accept both shapes defensively.
  const audiences = Array.isArray(claims.aud) ? claims.aud : [claims.aud]
  if (!audiences.includes(options.clientId)) {
    throw new Error(
      `ID token audience mismatch: expected ${options.clientId}, got ${JSON.stringify(claims.aud)}`,
    )
  }

  const nowSec = Math.floor(now() / 1000)
  if (typeof claims.exp === 'number' && claims.exp + tolerance < nowSec) {
    throw new Error('ID token is expired')
  }
  if (typeof claims.iat === 'number' && claims.iat - tolerance > nowSec) {
    throw new Error('ID token issued in the future')
  }

  if (options.nonce !== undefined && claims.nonce !== options.nonce) {
    throw new Error('ID token nonce mismatch')
  }

  return claims
}
