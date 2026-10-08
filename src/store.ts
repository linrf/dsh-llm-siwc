/**
 * Credential persistence.
 *
 * Requirements from the docs:
 *  - protected local storage, owner-only permissions
 *  - one record per issued client id, keyed with its verified identity
 *  - never merge one registration's client id with another's tokens
 *  - atomic writes
 *
 * The `CredentialStore` interface is deliberately narrow so DSH can back it
 * with `dsh-credentials-local` instead of the filesystem implementation here.
 */

import { createHash } from 'node:crypto'
import { mkdir, readFile, writeFile, rename, readdir, unlink, chmod } from 'node:fs/promises'
import { join } from 'node:path'
import type { SiwcCredential } from './types.ts'

export interface CredentialStore {
  get(clientId: string): Promise<SiwcCredential | null>
  list(): Promise<SiwcCredential[]>
  save(credential: SiwcCredential): Promise<void>
  remove(clientId: string): Promise<void>
}

/** Filename-safe token derived from a client id. */
function fileKey(clientId: string): string {
  return createHash('sha256').update(clientId).digest('hex').slice(0, 32)
}

/**
 * Filesystem store: `<dir>/credentials/<hash>.json`, mode 0600, atomic rename.
 */
export class FileCredentialStore implements CredentialStore {
  readonly #dir: string

  constructor(dir: string) {
    this.#dir = join(dir, 'credentials')
  }

  #path(clientId: string): string {
    return join(this.#dir, `${fileKey(clientId)}.json`)
  }

  async get(clientId: string): Promise<SiwcCredential | null> {
    try {
      const raw = await readFile(this.#path(clientId), 'utf8')
      const parsed = JSON.parse(raw) as SiwcCredential
      // Guard against hash collisions / tampering: the record must own this id.
      if (parsed.clientId !== clientId) return null
      return parsed
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null
      throw error
    }
  }

  async list(): Promise<SiwcCredential[]> {
    let names: string[]
    try {
      names = await readdir(this.#dir)
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []
      throw error
    }
    const out: SiwcCredential[] = []
    for (const name of names) {
      if (!name.endsWith('.json')) continue
      try {
        out.push(JSON.parse(await readFile(join(this.#dir, name), 'utf8')) as SiwcCredential)
      } catch {
        // A corrupt record must not take down the whole listing.
      }
    }
    return out.sort((a, b) => a.savedAt.localeCompare(b.savedAt))
  }

  async save(credential: SiwcCredential): Promise<void> {
    await mkdir(this.#dir, { recursive: true, mode: 0o700 })
    const target = this.#path(credential.clientId)
    const tmp = `${target}.${process.pid}.tmp`
    await writeFile(tmp, JSON.stringify(credential, null, 2), { mode: 0o600 })
    await chmod(tmp, 0o600)
    await rename(tmp, target) // atomic on the same filesystem
  }

  async remove(clientId: string): Promise<void> {
    try {
      await unlink(this.#path(clientId))
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
    }
  }
}
