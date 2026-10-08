/**
 * Peer resolution bootstrap — MUST run before any `@deepseek-ai/*` import.
 *
 * The problem: a profile-installed plugin lives outside the DSH installation,
 * while its peers (`@deepseek-ai/dsh-llm`, …) live inside the packaged
 * `app.asar/dsh/node_modules`. Ordinary node_modules resolution from the
 * plugin's own directory cannot reach them, and `app.asar` is a single file,
 * so it cannot sit in the middle of a directory chain.
 *
 * The fix: a resolve hook that maps `@deepseek-ai/<name>` onto the running
 * installation's own module tree. Electron reads `app.asar` paths natively.
 *
 * Two constraints learned the hard way:
 *  - node's `registerHooks` resolve hook must be SYNCHRONOUS (an async hook
 *    makes node reject the result), so discovery finishes before the plugin
 *    body loads and the hook only reads the resolved root;
 *  - the hook must return the package's ENTRY FILE, not its directory. ESM
 *    does not resolve a directory to a manifest entry the way CJS does, so
 *    `createRequire` is used to read `exports`/`main` for us.
 */

import { registerHooks, createRequire } from 'node:module'
import { existsSync } from 'node:fs'
import { dirname, isAbsolute, join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

const SCOPE = '@deepseek-ai/'

/**
 * Only these peers are redirected.
 *
 * The hook must NOT claim the whole `@deepseek-ai/` scope: the harness creates
 * its own loader entries dynamically (for example
 * `@deepseek-ai/dsh-host-directory-picker-native`), and rewriting those makes
 * `ctx.loader.create()` fail to produce a fiber. The preset audit then sees
 * pending rows and rejects every session with `agent-preset/invalid`.
 *
 * Keep this list in step with the plugin's actual imports.
 */
const PEER_PACKAGES = new Set([
  '@deepseek-ai/cordis',
  '@deepseek-ai/dsh-llm',
  '@deepseek-ai/dsh-credentials',
  '@deepseek-ai/dsh-authorization',
  '@deepseek-ai/dsh-commands',
  '@deepseek-ai/schemastery',
])

/** The package part of a specifier: `@scope/name/sub` -> `@scope/name`. */
function packageNameOf(specifier: string): string {
  const parts = specifier.split('/')
  return specifier.startsWith('@') ? parts.slice(0, 2).join('/') : (parts[0] ?? specifier)
}

let hostRoot: string | null = null

/** Candidate DSH package roots, most specific first. */
function candidateRoots(): string[] {
  const roots: string[] = []
  const fromEnv = process.env.DSH_HOST_ROOT
  if (fromEnv !== undefined && fromEnv !== '') roots.push(fromEnv)

  const executable = process.execPath
  // macOS: <App>.app/Contents/MacOS/<bin> -> <App>.app/Contents/Resources
  roots.push(resolve(dirname(executable), '..', 'Resources', 'app.asar', 'dsh'))
  roots.push(resolve(dirname(executable), '..', 'Resources', 'app', 'dsh'))
  // Windows / Linux unpacked layouts.
  roots.push(resolve(dirname(executable), 'resources', 'app.asar', 'dsh'))
  roots.push(resolve(dirname(executable), 'resources', 'app', 'dsh'))
  // A source checkout run through the bundled CLI.
  roots.push(resolve(dirname(executable), '..', '..', 'app.asar', 'dsh'))
  return roots
}

/**
 * Probe a candidate root by importing one peer every composition ships.
 *
 * `existsSync` cannot decide this for an `app.asar` path, because that path is
 * only readable through Electron's patched fs.
 */
async function probe(root: string): Promise<boolean> {
  const anchor = join(root, 'package.json')
  try {
    const entry = createRequire(anchor).resolve(`${SCOPE}dsh-llm`)
    const url = isAbsolute(entry) ? pathToFileURL(entry).href : pathToFileURL(join(root, entry)).href
    await import(url)
    return true
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    if (code === 'ERR_MODULE_NOT_FOUND' || code === 'ENOENT' || code === 'ENOTDIR') return false
    // A malformed manifest still proves the tree was reachable.
    return true
  }
}

async function discoverHostRoot(): Promise<string | null> {
  const candidates = candidateRoots()
  for (const candidate of candidates) {
    if (await probe(candidate)) return candidate
  }
  for (const candidate of candidates) {
    if (existsSync(join(candidate, 'node_modules', SCOPE, 'dsh-llm'))) return candidate
  }
  return null
}

let installed = false

/**
 * Discover the host installation, then install the resolve hook.
 *
 * Await this before importing any `@deepseek-ai/*` module.
 */
export async function initPeerResolution(): Promise<void> {
  if (installed) return
  installed = true

  hostRoot = await discoverHostRoot()
  if (hostRoot === null) {
    process.emitWarning(
      'dsh-llm-siwc: could not locate the DSH installation; set DSH_HOST_ROOT to its dsh package directory',
    )
  }

  registerHooks({
    resolve(specifier, context, nextResolve) {
      if (!specifier.startsWith(SCOPE)) return nextResolve(specifier, context)
      // Never claim packages the harness loads for itself.
      if (!PEER_PACKAGES.has(packageNameOf(specifier))) return nextResolve(specifier, context)
      const root = hostRoot
      if (root === null) return nextResolve(specifier, context)
      try {
        // Let node read the package manifest: `exports`/`main` decide the entry.
        const resolved = createRequire(join(root, 'package.json')).resolve(specifier)
        const url = isAbsolute(resolved)
          ? pathToFileURL(resolved).href
          : pathToFileURL(join(root, resolved)).href
        return { url, shortCircuit: true }
      } catch {
        return nextResolve(specifier, context)
      }
    },
  })
}

/** The discovered host root, for diagnostics. */
export function resolvedHostRoot(): string | null {
  return hostRoot
}
