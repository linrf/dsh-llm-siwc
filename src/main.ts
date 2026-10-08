/**
 * @dsh/llm-siwc — Sign in with ChatGPT provider plugin for DeepSeek Harness.
 *
 * Registers two things against the running composition:
 *  1. an authorization flow per ChatGPT account, so any surface (Web GUI,
 *     CLI, ACP) can start "Continue with ChatGPT" sign-in
 *  2. an `LlmAdapter` route serving the ChatGPT-plan Responses API
 *
 * The plugin owns its own protected credential store, because a SIWC
 * registration carries fields (`client_id`, `ext_agent_host_id`, `id_token`,
 * granted scopes) that the generic credential record does not model.
 *
 * Verified live 2026-10-08 against the real service.
 */

// Peer resolution is installed by ./index.ts before this module is loaded.
import { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import { LlmAdapter } from '@deepseek-ai/dsh-llm'
import { credentialKey, type CredentialKey } from '@deepseek-ai/dsh-credentials'
import { join } from 'node:path'

import {
  SiwcResponsesAdapter,
  type GenerateOptionsLike,
  type StreamChunk,
} from './adapter.ts'
import { authorize, ensureFreshCredential, planUsageEnabled } from './authorization.ts'
import { FileCredentialStore, type CredentialStore } from './store.ts'
import { loadOrCreateHostId } from './host-id.ts'
import { resolveConfig, type SiwcConfig } from './config.ts'
import type { SiwcCredential } from './types.ts'

export const name = 'llm-siwc'

/**
 * Services to await before activation.
 *
 * MUST be the array form. cordis reads `inject` as a map of
 * "service name → intercept config", so `{ required: [...], optional: [...] }`
 * is taken as two services literally named `required` and `optional`. Neither
 * ever exists, so the plugin stays pending forever and `apply()` never runs.
 */
export const inject = ['llm', 'authorization']

export const Config = z.object({
  /** Provider route name requests select with `GenerateOptions.provider`. */
  provider: z.string().default('chatgpt'),
  /** Model ids to advertise in pickers. */
  models: z.array(z.string()).default([
    'gpt-6.1-sol',
    'gpt-6-astra',
    'gpt-6-sol',
    'gpt-6-luna',
    'gpt-5.6-sol',
    'gpt-5.6-terra',
    'gpt-5.6-luna',
  ]),
  /** Where SIWC credentials and the host id are persisted. */
  storeDir: z.string().default(''),
  /** Loopback callback host; keep 127.0.0.1 (docs forbid `localhost`). */
  callbackHost: z.string().default('127.0.0.1'),
  /** Preferred loopback callback port. */
  callbackPort: z.natural().default(1455),
  /** Refresh this many ms before access-token expiry. */
  refreshLeadMs: z.natural().default(300_000),
  /** Internal name for the flow registry. */
  flowId: z.string().default('chatgpt'),
  /** Label shown to the user on the sign-in surface. */
  flowLabel: z.string().default('ChatGPT'),
})

export type Config = z.infer<typeof Config>

function defaultStoreDir(): string {
  const home = process.env.DSH_HOME ?? `${process.env.HOME ?? ''}/.dsh`
  return join(home, 'siwc')
}

/** Harness-facing adapter: extends the real base class and delegates. */
class HarnessSiwcAdapter extends LlmAdapter {
  readonly #core: SiwcResponsesAdapter

  constructor(core: SiwcResponsesAdapter) {
    super()
    this.#core = core
  }

  override providerInfo(provider: string): { id: string; name: string } {
    return this.#core.providerInfo(provider)
  }

  override listModels(
    provider: string,
  ): Promise<readonly { provider: string; id: string; name: string }[]> {
    return Promise.resolve(this.#core.listModels(provider))
  }

  override resolveModel(
    provider: string,
    model: string,
  ): Promise<{ provider: string; id: string; name: string }> {
    return this.#core.resolveModel(provider, model)
  }

  override stream(options: GenerateOptionsLike): AsyncIterable<StreamChunk> {
    return this.#core.stream(options)
  }
}

export function apply(ctx: Context, config: Config): void {
  // Diagnostics: activation failures in this composition are silent cascades,
  // so the plugin reports what it actually managed to register.
  console.log(`llm-siwc: apply() entered (provider=${config.provider}, models=${config.models.length})`)
  const settings: SiwcConfig = resolveConfig({
    storeDir: config.storeDir === '' ? defaultStoreDir() : config.storeDir,
    callbackHost: config.callbackHost,
    callbackPort: config.callbackPort,
    refreshLeadMs: config.refreshLeadMs,
  })
  const store: CredentialStore = new FileCredentialStore(settings.storeDir)
  const hostIdPath = join(settings.storeDir, 'host_id')
  // Best effort: a host id that cannot be persisted must not fail activation.
  void loadOrCreateHostId(hostIdPath).catch((error: unknown) => {
    console.error('llm-siwc: could not persist the host id:', error)
  })

  // ---- 1. inference route ----
  // Every registration below is failure-isolated: a plugin must never take the
  // host composition down with it. A throw here would abort this fiber and can
  // cascade into unrelated rows (agent presets, tools) failing to start.
  const core = new SiwcResponsesAdapter({
    providers: [config.provider],
    models: config.models,
    resolveAccessToken: async (provider) => {
      const credential = await pickCredential(store, provider)
      if (!credential) {
        throw new Error(
          `no ChatGPT credential for provider "${provider}"; sign in with ChatGPT first`,
        )
      }
      if (!planUsageEnabled(credential)) {
        throw new Error(
          'the stored ChatGPT credential lacks the chatgpt.tokens.use.direct scope; reauthorize with the full scope set',
        )
      }
      const fresh = await ensureFreshCredential(credential.clientId, { store, config: settings })
      return fresh.accessToken
    },
  })
  try {
    ctx.llm.registerAdapter([config.provider], new HarnessSiwcAdapter(core))
    console.log(`llm-siwc: LLM route "${config.provider}" registered`)
  } catch (error) {
    console.error(`llm-siwc: could not register the "${config.provider}" route:`, error)
    return
  }

  // ---- 2. sign-in flow ----
  const authorization = ctx.authorization
  if (authorization === undefined) {
    console.warn(
      'llm-siwc: no authorization service mounted; inference works, but ChatGPT sign-in is unavailable',
    )
    return
  }

  try {
    authorization.registerFlow({
      key: credentialKey('llm-siwc', config.flowId),
      label: config.flowLabel,
      methods: [{ id: 'oauth', label: 'Continue with ChatGPT' }],
      async run(session) {
        const result = await authorize(
          {},
          {
            store,
            config: settings,
            signal: session.signal,
            browser: {
              open: async (url) => {
                session.notify({ message: 'Continue signing in to ChatGPT in your browser.', url })
              },
            },
          },
        )

        if (result.credential.email) {
          session.notify({ message: `Signed in as ${result.credential.email}.` })
        }
        if (!planUsageEnabled(result.credential)) {
          session.notify({
            message:
              'Sign-in succeeded, but ChatGPT plan usage was not granted. Reauthorize with the full scope set or configure another billing path.',
          })
        }
        await commitRecord(session, result.credential, config)
      },
    })
    console.log('llm-siwc: apply() completed — sign-in flow registered')
  } catch (error) {
    console.error('llm-siwc: could not register the sign-in flow:', error)
  }
}

/** Pick the credential to use for a provider route. */
async function pickCredential(
  store: CredentialStore,
  provider: string,
): Promise<SiwcCredential | null> {
  const all = await store.list()
  if (all.length === 0) return null
  // The route is account-agnostic; the newest usable registration wins.
  const usable = all.filter((credential) => credential.clientId.startsWith('oaiapp_'))
  return usable.at(-1) ?? null
}

/**
 * Record the authorization with the harness credential service when present.
 *
 * The plugin keeps its own richer record; this commit only tells the flow
 * registry the attempt succeeded, which `begin()` requires.
 */
async function commitRecord(
  session: { commit: (record: never) => Promise<void> },
  credential: SiwcCredential,
  config: Config,
): Promise<void> {
  const record = {
    type: 'oauth',
    access: credential.accessToken,
    refresh: credential.refreshToken,
    expires: credential.expiresAt,
    accountId: credential.subject,
  }
  try {
    await session.commit(record as never)
  } catch (error) {
    // A composition without a writable credential store must not fail sign-in:
    // the flow's own store already holds the authoritative record.
    throw new Error(
      `ChatGPT sign-in succeeded and the credential was stored, but recording it with the credential service failed: ${String(error)}`,
    )
  }
}

export { SiwcResponsesAdapter, FileCredentialStore, authorize, ensureFreshCredential }
export type { SiwcCredential, CredentialStore, GenerateOptionsLike, StreamChunk }
