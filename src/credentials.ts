/**
 * Credential selection for the ChatGPT-plan route.
 *
 * Kept free of harness imports so it stays testable: `main.ts` pulls in
 * `@deepseek-ai/*` peers, which only resolve inside a running installation.
 *
 * @module dsh-llm-siwc/credentials
 */

/** The fields selection depends on. */
export interface SelectableCredential {
  clientId: string
  createdAt?: number
  expiresAt?: number
}

/**
 * Pick the registration this route should use.
 *
 * The route is account-agnostic, so the newest usable registration wins. A
 * re-registration lands beside the previous credential — the old file is kept,
 * not deleted — and `readdir` order is not a contract. Choosing the stale entry
 * would surface as an auth failure immediately after a fresh sign-in, so the
 * order is decided here rather than inherited from the directory.
 */
export function newestCredential<T extends SelectableCredential>(
  credentials: readonly T[],
): T | null {
  const usable = credentials.filter((credential) => credential.clientId.startsWith('oaiapp_'))
  if (usable.length === 0) return null
  const ordered = [...usable].sort((left, right) => {
    const byCreated = (left.createdAt ?? 0) - (right.createdAt ?? 0)
    if (byCreated !== 0) return byCreated
    return (left.expiresAt ?? 0) - (right.expiresAt ?? 0)
  })
  return ordered.at(-1) ?? null
}
