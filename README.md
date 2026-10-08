# dsh-llm-siwc

A DeepSeek Harness **plugin** that adds ChatGPT-plan inference through OpenAI's
official [Sign in with ChatGPT](https://developers.openai.com/siwc) (SIWC)
flow. No DSH rebuild is involved: this is a standalone bundle the running
application loads from its profile.

Verified against the live service on 2026-10-08.

## What it registers

| Registration | Effect |
|---|---|
| Authorization flow `llm-siwc/chatgpt` | Any surface can start "Continue with ChatGPT" sign-in |
| LLM adapter route `chatgpt` | Inference over `https://api.openai.com/v1/responses` |
| `/chatgpt` command | In-GUI sign-in, account status, and sign-out |
| Provider directory entry | A **ChatGPT** row on Settings → Models |

The plugin owns its own credential store because a SIWC registration carries
fields the generic credential record does not model: `client_id`,
`ext_agent_host_id`, `id_token`, and the granted scopes.

### What the route supports

| Capability | Behaviour |
|---|---|
| Streaming | SSE deltas mapped to harness `StreamChunk`s |
| Tool calls | `function_call` / `function_call_output` round trips |
| Images | Read from the attachment service per request, sent as base64 data URLs |
| Model catalog | Read live from `GET /v1/models`; per-model names, context, modalities, reasoning levels |
| Reasoning effort | Each model's own accepted levels, forwarded as `reasoning.effort` |
| Usage | Counts mapped to the harness camelCase shape, so sessions project cleanly |
| Retry policy | Transient failures only — **never** a usage-limit 429 |

**On the retry policy:** the harness default retries `RATE_LIMIT` five times. On
this route a 429 is `subscription_sharing_usage_limit_exceeded`, a persistent
plan limit, and OpenAI's documentation says to pause the account rather than
repeat the request. The route therefore declares a policy that retries only
`EMPTY_RESPONSE`, `SERVER`, `TIMEOUT`, and `TRANSPORT`.

**On reasoning output:** the route sends no reasoning text unless a summary is
requested, so the plugin asks for `reasoning.summary: auto` — configurable as
`reasoningSummary`, with `none` to opt out. Two route behaviours are worth
knowing, both verified against the live endpoint:

- A summary arrives only when the request carries **no tools**. With tools
  present the reasoning deltas never come, so the thinking stream stays empty in
  ordinary tool-using sessions. The plugin keeps requesting the summary anyway,
  so it starts working if the route changes.
- `effort: low` yields no summary even without tools; `medium` and above do.

## Using it

### Sign in

Three equivalent ways; pick whichever fits:

**In the GUI** — type `/chatgpt login` in the composer:

```
/chatgpt           show the signed-in account (same as /chatgpt status)
/chatgpt status    account, client id, plan-usage scope, access-token expiry
/chatgpt login     open the browser and run the official SIWC flow
/chatgpt logout    revoke the session and clear the local credential
```

The browser round trip runs in the background, so `/chatgpt login` returns
immediately; confirm with `/chatgpt status` once you finish authorizing.

**From the CLI** — no session required:

```bash
node scripts/login.mjs            # sign in
node scripts/login.mjs --status   # show the stored account
node scripts/login.mjs --logout   # revoke and clear
```

### Pick the model

After signing in, choose a **ChatGPT** model in the composer's model control
(`/model`). The list is read from the route's own catalog at `GET /v1/models`,
so it reflects what the account can actually use — at the time of writing ten
models, including `gpt-reserve`, `gpt-5.5`, and `codex-auto-review`.

The catalog also supplies each model's real display name, description, context
window, input modalities, and **its own reasoning levels**. Those levels differ
per model (`gpt-5.5` stops at `xhigh`; `gpt-6.1-sol` reaches `ultra`), which is
why the plugin reads them instead of shipping one shared list. The chosen level
is forwarded to the request as `reasoning.effort`.

The read is cached for five minutes and shares one in-flight request. If it
fails — offline, expired credential, preview outage — the picker falls back to
a deliberately conservative built-in list rather than going empty, and the next
successful read replaces it.

Settings → Models also lists a **ChatGPT** row. Its fields are informational
(display name, base URL override); the route works without configuring them.

### Notes on the command

- A command result is rendered **outside model history** by design — it is not
  a chat message, so the model never sees it and no tokens are spent on it.
- Commands are bound to a receiving agent, so **run them from a session**. In a
  fresh, session-less window use `scripts/login.mjs` instead.
- The command deliberately declares no `input` descriptor: a host descriptor
  with `input` resolves as `leadingInput` (the composer waits for more text),
  which would make a bare `/chatgpt` appear to do nothing.

## Requirements

- DSH **0.2.0-rc.2** (the plugin declares `>=0.2.0-rc.2 <0.3.0` peers; DSH
  refuses to load a plugin whose DSH peer ranges do not match its runtime)
- A ChatGPT plan that is eligible for plan usage
- Node 22.19+ to build

## Disclaimer

An independent, unofficial community plugin. It is not affiliated with,
endorsed by, or supported by OpenAI or DeepSeek.

It uses OpenAI's documented [Sign in with ChatGPT](https://developers.openai.com/siwc)
OAuth flow to spend the signed-in user's own ChatGPT plan quota. It stores no
credentials of its own beyond the registration the user explicitly authorizes,
and it never sees account passwords.

ChatGPT plan usage on this route is a preview capability. Eligibility, quota
behaviour, and the accepted request shape can change at any time and may
differ per account. You are responsible for confirming that your account is
eligible and for complying with OpenAI's terms.

## Install

No build step is involved: `lib/` is committed and the package declares its
bundle patch, so installing the package is the whole installation.

### Desktop app — the normal case

Use the app's own **Plugins** page. It is the supported path for the desktop
profile and needs no CLI and no hand-edited configuration:

1. Open **Plugins** in the sidebar.
2. Paste this repository's URL into the install field:
   `https://github.com/linrf/dsh-llm-siwc`
3. Install, then switch the added bundle on if it is not on already.

The page reads the spec, runs the profile's package manager, shows its output,
and activates what it added. Uninstalling asks for confirmation.

### Command line — any other profile

```bash
dsh plugin --profile <name> add https://github.com/linrf/dsh-llm-siwc
```

Verified end to end: pnpm resolves `github:linrf/dsh-llm-siwc`, the bundle is
added to the profile, `node_modules/dsh-llm-siwc/lib` arrives intact, and
`dsh --profile <name> --dump-config` lists the `llm-siwc` row.

A local checkout works the same way with a path or `link:` spec:

```bash
dsh plugin --profile <name> add /absolute/path/to/dsh-llm-siwc
```

### Why `--profile desktop` is refused on the command line

The desktop profile is reserved for the Electron application, and the CLI
enforces it:

| Invocation | Result |
|---|---|
| `dsh --profile desktop` | **always** refused |
| `dsh plugin --profile desktop …` (plain CLI) | refused |
| `dsh plugin --profile desktop …` (the app's own carrier) | allowed, but requires the app to have initialized the profile **and** to be fully quit |
| the app's **Plugins** page | supported — it manages the profile it owns |

The refusals read:

```
error: profile "desktop" is managed exclusively by the Electron application
```

```
Open DeepSeek Harness Desktop once to initialize its profile, then fully quit
it before running dsh plugin --profile desktop.
```

`scripts/install.sh` remains as a fallback: it edits the desktop profile's
`package.json` and links the package directly. Prefer the **Plugins** page — the
script exists only for the case where that page is unavailable.

Then **restart the app** so the new bundle loads.

## Development

Only needed when changing the plugin; installing a release does not build.

```bash
pnpm install
pnpm build     # -> lib/bootstrap.js, lib/index.js, lib/main.js (committed)
pnpm test      # 35 tests
```

`scripts/build.mjs` bundles `src/` with esbuild. Peer imports (`@deepseek-ai/*`)
stay external and are resolved at runtime by `lib/bootstrap.js`.

## How peer resolution works

The plugin's peers (`@deepseek-ai/dsh-llm`, …) live inside the packaged
`app.asar/dsh/node_modules`, which ordinary `node_modules` resolution from the
plugin's own directory cannot reach. `lib/bootstrap.js` discovers the running
installation and installs a synchronous resolve hook that maps
`@deepseek-ai/<name>` onto it via `createRequire`, so `exports`/`main` decide
the entry file exactly as node would.

Two constraints are load-bearing and easy to regress:

- the resolve hook must be **synchronous** — an async hook makes node reject
  the result (`shortCircuit` reads as `undefined`)
- the hook must return the package's **entry file**, not its directory — ESM
  does not resolve a directory to a manifest entry the way CJS does

`DSH_HOST_ROOT` overrides discovery when needed.

## Protocol notes (all verified live)

- `client_id=dynamic_agent_client` is the **registration entrypoint**; the
  callback returns the issued `oaiapp_…`, which is what token exchange uses
- the ID token's `aud` is an **array** (`["oaiapp_…"]`); the access token's
  `aud` is the string `https://api.openai.com/v1`
- the loopback callback must be `127.0.0.1`, never `localhost`
- access tokens last **1 hour** and refresh tokens **30 days**; refresh tokens
  rotate, so refreshes are serialized per registration
- inference must use `api.openai.com/v1/responses` with `store:false` and
  `stream:true`, **never** `chatgpt.com/backend-api`
- `ext_agent_host_id` is generated once and reused; regenerate it and the
  service treats the machine as a new host

## Preview limitations

The Responses route rejects `temperature`, `top_p`, `max_output_tokens`,
`metadata`, `prompt`, `truncation`, `user`, and more —
`stripUnsupportedFields()` removes them. It also rejects explicit
`{"type":"message","role":"system"}` items (they are lifted into
`instructions`) and `previous_response_id` over HTTP (history is replayed).

**Client-side function tools work.** A full tool-call → tool-result →
final-answer round trip was verified live; only *hosted* tools (Code
Interpreter, file search, hosted MCP, `tool_search`) are unavailable.

## Layout

```
src/
  bootstrap.ts      host discovery + synchronous peer resolve hook
  index.ts          entry: init bootstrap, then load the body
  main.ts           plugin body: registers flow, adapter, and /chatgpt
  adapter.ts        Responses SSE -> harness StreamChunk
  client.ts         streaming request, enforces preview constraints
  catalog.ts        live model catalog (names, context, reasoning levels)
  convert.ts        harness messages -> Responses input items
  sse.ts            server-sent events parser
  errors.ts         error matrix + unsupported-field stripping
  authorization.ts  authorize / ensureFreshCredential / signOut
  oauth.ts          authorize URL, code exchange, refresh, revoke
  verify.ts         ID-token verification (JWKS, iss, aud, exp, nonce)
  callback.ts       loopback listener with port fallback
  store.ts          credential persistence (0600, atomic)
  host-id.ts        ext_agent_host_id
  browser.ts        system-browser launcher
  config.ts         protocol constants
test/                    35 tests
scripts/build.mjs        esbuild bundling
scripts/install.sh       desktop-profile installation
scripts/login.mjs        CLI sign-in / status / sign-out
scripts/rollback.sh      remove the plugin if the app stops working
scripts/repair-usage.mjs rewrite stored usage blocks written in provider shape
```

## Tests

```bash
pnpm test
```

Covers message conversion (including the system-message lift and the
tool-call/result pairing), SSE reassembly, adapter stream mapping, usage
conversion, the mandatory `store`/`stream` flags, error classification, and a
live-shaped tool round trip.

## Troubleshooting

If sessions fail to create after installing, run the rollback script and see
the issue tracker:

```bash
bash scripts/rollback.sh    # removes the plugin from the desktop profile
```

Sessions created before v0.1.0-era builds may hold `usage` blocks in the
provider's snake_case shape, which breaks session projection with
`uncachedInputTokens: NaN`. `scripts/repair-usage.mjs` rewrites them in place
(dry-run by default, backs each log up as `*.bak-usage`):

```bash
node scripts/repair-usage.mjs           # report only
node scripts/repair-usage.mjs --apply   # back up and rewrite
```
