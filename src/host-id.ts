/**
 * `ext_agent_host_id`: a stable, opaque, per-host identifier.
 *
 * Docs require choosing and persisting it BEFORE the host's first sign-in.
 * Supported formats (preference order):
 *   1. urn:ietf:params:oauth:jwk-thumbprint:<thumbprint>   (recommended)
 *   2. urn:uuid:<uuidv4>                                    (supported)
 *   3. did:key:<key>
 *
 * It is an identifier only — NOT an authentication credential, and OpenAI
 * does not verify possession of any private key in this flow.
 */

import { createHash, generateKeyPairSync, randomUUID, type KeyObject } from 'node:crypto'
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { dirname } from 'node:path'
import { base64url } from './crypto.ts'

const UUID_PREFIX = 'urn:uuid:'
const THUMBPRINT_PREFIX = 'urn:ietf:params:oauth:jwk-thumbprint:'

/** Generate a UUIDv4-based host id. */
export function generateUuidHostId(): string {
  return `${UUID_PREFIX}${randomUUID()}`
}

/**
 * Generate a host id derived from a fresh Ed25519 public key via RFC 9278
 * JWK thumbprint (the format OpenAI recommends).
 *
 * Returns the private key too, so a deployment that later wants to prove
 * possession can persist it. This flow does not require that proof.
 */
export function generateJwkThumbprintHostId(): { hostId: string; privateKey: KeyObject } {
  const { publicKey, privateKey } = generateKeyPairSync('ed25519')
  const jwk = publicKey.export({ format: 'jwk' }) as Record<string, string>

  // RFC 9278: thumbprint over the canonical JWK members, SHA-256, base64url.
  // Ed25519 canonical members are crv, kty, x (lexicographic order).
  const canonical = JSON.stringify({ crv: jwk.crv, kty: jwk.kty, x: jwk.x })
  const thumbprint = base64url(createHash('sha256').update(canonical).digest())
  return { hostId: `${THUMBPRINT_PREFIX}${thumbprint}`, privateKey }
}

/** Validate an accepted host-id format. */
export function isValidHostId(value: string): boolean {
  return (
    value.startsWith(UUID_PREFIX) ||
    value.startsWith(THUMBPRINT_PREFIX) ||
    value.startsWith('did:key:')
  )
}

/**
 * Read the persisted host id, or create and persist one on first use.
 *
 * Generation MUST happen before the first sign-in; reusing the stored value
 * is what keeps plan-usage settings associated with the same host.
 */
export async function loadOrCreateHostId(
  filePath: string,
  generate: () => string = generateUuidHostId,
): Promise<string> {
  try {
    const existing = (await readFile(filePath, 'utf8')).trim()
    if (existing) {
      if (!isValidHostId(existing)) {
        throw new Error(`persisted host id has an unsupported format: ${existing}`)
      }
      return existing
    }
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    if (code !== 'ENOENT') throw error
  }

  const created = generate()
  await mkdir(dirname(filePath), { recursive: true, mode: 0o700 })
  await writeFile(filePath, created, { mode: 0o600 })
  return created
}
