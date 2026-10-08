/**
 * base64url + PKCE + random helpers.
 *
 * Protocol-critical detail (verified live 2026-10-08):
 *  - code_challenge MUST be base64url(SHA256(verifier)) with NO padding.
 *  - state / nonce MUST be fresh per attempt.
 */

import { createHash, randomBytes } from 'node:crypto'

/** RFC 4648 §5 base64url without padding. */
export function base64url(input: Buffer | Uint8Array): string {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '')
}

/** Random URL-safe token, `bytes` of entropy. */
export function randomToken(bytes = 32): string {
  return base64url(randomBytes(bytes))
}

export interface PkcePair {
  /** The secret kept locally; sent only in the token exchange. */
  verifier: string
  /** The value sent to the authorize endpoint. */
  challenge: string
}

/** Generate a PKCE S256 pair. */
export function createPkce(): PkcePair {
  const verifier = randomToken(32)
  const challenge = base64url(createHash('sha256').update(verifier).digest())
  return { verifier, challenge }
}

/** Decode a JWT payload segment without verifying it. */
export function decodeJwtPayload(token: string): Record<string, unknown> {
  const parts = token.split('.')
  if (parts.length !== 3) throw new Error('malformed JWT: expected 3 segments')
  try {
    return JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'))
  } catch (error) {
    throw new Error(`malformed JWT payload: ${String(error)}`)
  }
}
