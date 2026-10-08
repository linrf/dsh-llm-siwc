/**
 * Plugin entry.
 *
 * The peer-resolution hook must be installed BEFORE the plugin body imports
 * anything from `@deepseek-ai/*`, and that hook must be synchronous. So:
 *
 *   1. discover the DSH installation and register the resolve hook
 *   2. only then load the plugin body, which imports DSH peers statically
 */

import { initPeerResolution } from './bootstrap.ts'

await initPeerResolution()

const main = await import('./main.ts')

export const name = main.name
export const inject = main.inject
export const apply = main.apply
export const Config = main.Config

export type { Config as SiwcPluginConfig } from './main.ts'
