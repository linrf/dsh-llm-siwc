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
import { CommandDefinitionId } from '@deepseek-ai/dsh-commands/brand'
import { join } from 'node:path'

import {
  SiwcResponsesAdapter,
  type GenerateOptionsLike,
  type ResolvedModelLike,
  type RetryPolicyLike,
  type StreamChunk,
} from './adapter.ts'
import {
  authorize,
  ensureFreshCredential,
  planUsageEnabled,
  signOut as signOutCredential,
} from './authorization.ts'
import { FileCredentialStore, type CredentialStore } from './store.ts'
import { ModelCatalog, FALLBACK_CATALOG } from './catalog.ts'
import { newestCredential } from './credentials.ts'
import { loadOrCreateHostId } from './host-id.ts'
import { resolveConfig, type SiwcConfig } from './config.ts'
import { PLAN_USAGE_SCOPE } from './config.ts'
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
export const inject = ['llm', 'authorization', 'commands', 'attachments']

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
  /**
   * Reasoning summary to request: `auto`, `concise`, or `none`.
   *
   * The route sends no reasoning text unless a summary is asked for, so the
   * harness thinking stream stays empty without it. `none` opts out, for when
   * the extra output tokens matter more than seeing the reasoning.
   */
  reasoningSummary: z.string().default('auto'),
  /**
   * Per-route settings surface.
   *
   * This exists so the Models page lists the route. `dsh-settings` only
   * publishes a namespace for a Config that declares at least one
   * `.volatile()` field, and the provider directory entry this plugin
   * registers points at `['providers', <route>]`. Without a volatile field the
   * row is neither shown nor addable, even though `/model` still works.
   *
   * The values are informational: the adapter reads its behavior from the
   * top-level options above, so nothing here has to be set. The default still
   * names the default route, because the Models page only renders a row once
   * `getPath(value, ['providers', <route>])` resolves — an empty default leaves
   * the route merely "addable" instead of listed.
   */
  providers: z
    .dict(
      z.object({
        /** Label shown for this route in pickers and settings. */
        displayName: z.string().default('ChatGPT'),
        /** Override the API base URL (self-hosted gateway / tests). */
        baseUrl: z.string().default(''),
      }),
    )
    .default({ chatgpt: { displayName: 'ChatGPT', baseUrl: '' } })
    .volatile(),
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

  /** Route-owned retry policy: transient failures only, never RATE_LIMIT. */
  override providerRetryPolicy(): RetryPolicyLike {
    return this.#core.providerRetryPolicy()
  }

  override listModels(provider: string): Promise<
    readonly { provider: string; id: string; name: string; inputModalities: readonly string[] }[]
  > {
    return this.#core.listModels(provider)
  }

  override resolveModel(provider: string, model: string): Promise<ResolvedModelLike> {
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
  // Token resolution is shared by the adapter and the model catalog.
  const resolveAccessToken = async (provider: string): Promise<string> => {
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
  }

  // The route publishes its real catalog at GET /v1/models; hardcoding it hides
  // models the account can use and advertises reasoning levels it rejects.
  const catalog = new ModelCatalog({
    resolveAccessToken: () => resolveAccessToken(config.provider),
    seed: FALLBACK_CATALOG,
    onError: (error) => console.error('llm-siwc: could not read the model catalog:', error),
  })
  // Warm the catalog so the first model pick already shows the live list.
  // Both outcomes are handled: the harness treats an unhandled rejection as a
  // fatal load failure and exits the application, and a stale credential must
  // never be able to do that.
  void catalog.load().then(
    (models) => {
      console.log(`llm-siwc: model catalog ready (${models.length} models)`)
    },
    (error: unknown) => {
      console.error('llm-siwc: model catalog prewarm failed:', error)
    },
  )

  const core = new SiwcResponsesAdapter({
    providers: [config.provider],
    models: config.models,
    resolveAccessToken,
    catalog,
    reasoningSummary: config.reasoningSummary,
    // Image bytes are never in the session log, so each referenced attachment
    // is read here and handed to the wire as a data URL.
    resolveImage: async (attachment, signal) => {
      try {
        const ref = attachment as { width?: number; height?: number }
        const version = (await ctx.attachments.readImageRequest(
          attachment as never,
          {
            width: ref.width ?? 1024,
            height: ref.height ?? 1024,
            maxBytes: 1_048_576,
          },
          signal,
        )) as { data: Uint8Array; mediaType?: string }
        const base64 = Buffer.from(version.data).toString('base64')
        return `data:${version.mediaType ?? 'image/png'};base64,${base64}`
      } catch (error) {
        console.error('llm-siwc: could not read an image attachment:', error)
        return undefined
      }
    },
  })
  try {
    ctx.llm.registerAdapter([config.provider], new HarnessSiwcAdapter(core))
    console.log(`llm-siwc: LLM route "${config.provider}" registered`)
  } catch (error) {
    console.error(`llm-siwc: could not register the "${config.provider}" route:`, error)
    return
  }

  // Advertise the route in the GUI's provider directory so Models settings
  // shows a ChatGPT row. The settings namespace is this row's own id, which is
  // what pi-ai does — no separate settings registration is needed.
  try {
    const settingsNs =
      (ctx as { fiber?: { entry?: { options?: { id?: string } } } }).fiber?.entry?.options?.id ??
      'llm-siwc'
    ctx.llm.registerConfigurableProviders([
      {
        provider: config.provider,
        displayName: 'ChatGPT',
        settingsNs,
        settingsPath: ['providers', config.provider],
      },
    ])
    console.log(`llm-siwc: provider directory entry registered (settingsNs=${settingsNs})`)
  } catch (error) {
    console.error('llm-siwc: could not register the provider directory entry:', error)
  }

  // Let the "add a provider" flow pull this route's catalog. The request may
  // carry a typed API key, but this route authenticates with the stored OAuth
  // credential, so discovery reads the catalog the same way the adapter does.
  try {
    const settingsNs =
      (ctx as { fiber?: { entry?: { options?: { id?: string } } } }).fiber?.entry?.options?.id ??
      'llm-siwc'
    ctx.llm.registerModelDiscovery(settingsNs, async (request, signal) => {
      const models = await catalog.load(signal)
      const provider = request.provider ?? config.provider
      return models.map((model) => ({
        provider,
        id: model.slug,
        name: model.displayName,
        ...(model.description === undefined ? {} : { description: model.description }),
        inputModalities: [...model.inputModalities],
      }))
    })
    console.log('llm-siwc: model discovery registered')
  } catch (error) {
    console.error('llm-siwc: could not register model discovery:', error)
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

  // ---- 3. GUI command surface ----
  //
  // The web GUI has no generic authorization entry point, and adding one would
  // need a typert-generated Remote (the Electron client talks over an IPC
  // bridge, not HTTP). A slash command needs neither: the existing
  // `remote.commands.execute` path already reaches the host, so `/chatgpt`
  // becomes the GUI entry for sign-in, status, and sign-out.
  try {
    ctx.commands.register({
      definitionId: CommandDefinitionId('@deepseek-ai/dsh-llm-siwc'),
      name: 'chatgpt',
      // No `input` descriptor on purpose: a host descriptor WITH `input` is
      // resolved as `leadingInput` (the composer waits for more text), so a
      // bare `/chatgpt` would appear to do nothing until something else is
      // typed. Without it the line is `execute` and runs immediately.
      description: 'Sign in to ChatGPT, show the signed-in account, or sign out — /chatgpt [login|status|logout]',
      handler: (invocation) =>
        runChatgptCommand(invocation, {
          store,
          settings,
          signIn: () => authorize({}, { store, config: settings }),
          signOut: (clientId) => signOutCredential(clientId, { store, config: settings }),
        }),
    })
    console.log('llm-siwc: /chatgpt command registered')
  } catch (error) {
    console.error('llm-siwc: could not register the /chatgpt command:', error)
  }
}

/** Shape a command handler receives; only the fields this command reads. */
export interface CommandInvocationLike {
  readonly rawInput?: string
}

/** Shape a command handler returns. */
type CommandResultLike =
  | { kind: 'success'; text?: string }
  | { kind: 'error'; text: string }

interface CommandDeps {
  store: CredentialStore
  settings: SiwcConfig
  signIn: () => Promise<{ credential: SiwcCredential; planUsageEnabled: boolean }>
  signOut: (clientId: string) => Promise<void>
}

/** Guards against overlapping sign-in attempts from repeated commands. */
let signInInFlight = false

/**
 * Implement `/chatgpt [login|status|logout]`.
 *
 * Sign-in runs in the background: `authorize()` blocks on a browser round trip,
 * so the command reports immediately and the account appears on the next
 * `/chatgpt status`.
 */
async function runChatgptCommand(
  invocation: CommandInvocationLike,
  deps: CommandDeps,
): Promise<CommandResultLike> {
  const argument = String(invocation.rawInput ?? '').trim().toLowerCase()

  if (argument === '' || argument === 'status') {
    const records = await deps.store.list()
    if (records.length === 0) {
      return {
        kind: 'success',
        text: 'No ChatGPT account is signed in.\n\nRun /chatgpt login to authorize this installation.',
      }
    }
    const blocks = records.map((record) => {
      const expires = new Date(record.expiresAt)
      const expired = expires.getTime() < Date.now()
      return [
        `Account    : ${record.email ?? record.subject}`,
        `Client id  : ${record.clientId}`,
        `Plan usage : ${record.scopes.includes(PLAN_USAGE_SCOPE) ? 'enabled' : 'NOT granted'}`,
        `Access     : ${expires.toISOString()}${expired ? ' (expired; renewed on next use)' : ''}`,
      ].join('\n')
    })
    return {
      kind: 'success',
      text: `ChatGPT — ${records.length} account${records.length === 1 ? '' : 's'}\n\n${blocks.join('\n\n')}`,
    }
  }

  if (argument === 'login') {
    if (signInInFlight) {
      return {
        kind: 'error',
        text: 'A sign-in is already in progress. Finish it in the browser window that opened.',
      }
    }
    signInInFlight = true
    void deps
      .signIn()
      .then((result) => {
        console.log(`llm-siwc: signed in as ${result.credential.email ?? result.credential.subject}`)
      })
      .catch((error: unknown) => {
        console.error('llm-siwc: sign-in failed:', error)
      })
      .finally(() => {
        signInInFlight = false
      })
    return {
      kind: 'success',
      text:
        'Opening your browser…\n\n' +
        'Authorize "DeepSeek Harness" there, then run /chatgpt status to confirm the account.',
    }
  }

  if (argument === 'logout') {
    const records = await deps.store.list()
    if (records.length === 0) return { kind: 'success', text: 'Nothing to sign out.' }
    const lines: string[] = []
    for (const record of records) {
      const label = record.email ?? record.clientId
      try {
        await deps.signOut(record.clientId)
        lines.push(`signed out: ${label}`)
      } catch (error) {
        lines.push(`signed out locally, remote revocation unconfirmed: ${label} — ${String(error)}`)
      }
    }
    return { kind: 'success', text: lines.join('\n') }
  }

  return {
    kind: 'error',
    text: `Unknown argument "${argument}". Usage: /chatgpt [login|status|logout]`,
  }
}

/** Pick the credential to use for a provider route. */
export async function pickCredential(
  store: CredentialStore,
  provider: string,
): Promise<SiwcCredential | null> {
  void provider
  return newestCredential(await store.list())
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
