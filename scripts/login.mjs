#!/usr/bin/env node
/**
 * Sign in / sign out of ChatGPT for the dsh-llm-siwc plugin.
 *
 * The plugin stores credentials under `<DSH_HOME>/siwc`; this tool performs the
 * official Sign in with ChatGPT OAuth flow and writes the record in exactly the
 * format the plugin reads, so no GUI entry point is required to get started.
 *
 *   node scripts/login.mjs            # sign in
 *   node scripts/login.mjs --logout   # sign out (revoke + clear)
 *   node scripts/login.mjs --status   # show the stored account
 *
 * Options: --store-dir <path>  --port <n>  --model <id>
 */
import { createServer } from 'node:http'
import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'

const AUTHORIZE_URL = 'https://auth.openai.com/api/accounts/authorize'
const TOKEN_URL = 'https://auth.openai.com/api/accounts/oauth/token'
const DISCOVERY_URL = 'https://auth.openai.com/.well-known/openid-configuration'
const RESOURCE = 'https://api.openai.com/v1'
const SCOPE = 'openid profile email offline_access resource.invoke chatgpt.tokens.use.direct'
const PLAN_SCOPE = 'chatgpt.tokens.use.direct'

// ---- arguments ----------------------------------------------------------
const argv = process.argv.slice(2)
const flag = (name, fallback) => {
  const i = argv.indexOf(name)
  return i >= 0 && argv[i + 1] !== undefined ? argv[i + 1] : fallback
}
const has = (name) => argv.includes(name)

const dshHome = process.env.DSH_HOME ?? join(process.env.HOME ?? '', '.dsh')
const storeDir = flag('--store-dir', join(dshHome, 'siwc'))
const port = Number(flag('--port', '1455'))
const redirectUri = `http://127.0.0.1:${port}/auth/callback`
const credentialsDir = join(storeDir, 'credentials')

const b64url = (buf) => Buffer.from(buf).toString('base64')
  .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
const fileKey = (clientId) => createHash('sha256').update(clientId).digest('hex').slice(0, 32)
const accountLabel = (record) => record.email ?? record.subject ?? record.clientId

function loadRecords() {
  if (!existsSync(credentialsDir)) return []
  return readdirSync(credentialsDir)
    .filter((name) => name.endsWith('.json'))
    .map((name) => {
      try {
        return { name, path: join(credentialsDir, name), record: JSON.parse(readFileSync(join(credentialsDir, name), 'utf8')) }
      } catch {
        return null
      }
    })
    .filter(Boolean)
}

// ---- status -------------------------------------------------------------
if (has('--status')) {
  const records = loadRecords()
  if (records.length === 0) {
    console.log(`no stored ChatGPT account (looked in ${credentialsDir})`)
    process.exit(0)
  }
  for (const { record } of records) {
    const expires = new Date(record.expiresAt)
    console.log(`account     : ${accountLabel(record)}`)
    console.log(`client_id   : ${record.clientId}`)
    console.log(`host_id     : ${record.extAgentHostId}`)
    console.log(`plan usage  : ${record.scopes?.includes(PLAN_SCOPE) ? 'enabled' : 'NOT granted'}`)
    console.log(`access until: ${expires.toISOString()}${expires.getTime() < Date.now() ? '  (expired — will refresh)' : ''}`)
  }
  process.exit(0)
}

// ---- logout -------------------------------------------------------------
if (has('--logout')) {
  const records = loadRecords()
  if (records.length === 0) {
    console.log('nothing to sign out')
    process.exit(0)
  }
  let granted = true
  for (const { path, record } of records) {
    try {
      const discovery = await (await fetch(DISCOVERY_URL)).json()
      const endpoint = discovery.revocation_endpoint
      if (!endpoint || !record.refreshToken) throw new Error('no revocation endpoint or refresh token')
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          token: record.refreshToken,
          token_type_hint: 'refresh_token',
          client_id: record.clientId,
        }),
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      console.log(`revoked  : ${accountLabel(record)}`)
    } catch (error) {
      granted = false
      console.log(`warning  : could not confirm remote revocation for ${accountLabel(record)}: ${String(error)}`)
    }
    unlinkSync(path)
    console.log(`removed  : ${path}`)
  }
  console.log()
  console.log(granted
    ? 'Signed out. The registered client is kept, so a later sign-in needs no new registration.'
    : 'Signed out locally, but remote revocation was not confirmed. You can also disconnect the app in ChatGPT settings.')
  process.exit(0)
}

// ---- sign in ------------------------------------------------------------
const hostIdPath = join(storeDir, 'host_id')
const hostId = existsSync(hostIdPath)
  ? readFileSync(hostIdPath, 'utf8').trim()
  : `urn:uuid:${randomUUID()}`

const verifier = b64url(randomBytes(32))
const state = b64url(randomBytes(16))
const nonce = b64url(randomBytes(16))

const authUrl = new URL(AUTHORIZE_URL)
for (const [key, value] of Object.entries({
  client_id: 'dynamic_agent_client',
  agent_name_hint: 'DeepSeek Harness',
  ext_agent_host_id: hostId,
  response_type: 'code',
  redirect_uri: redirectUri,
  scope: SCOPE,
  resource: RESOURCE,
  state,
  nonce,
  code_challenge_method: 'S256',
  code_challenge: b64url(createHash('sha256').update(verifier).digest()),
})) authUrl.searchParams.set(key, value)

console.log('store dir :', storeDir)
console.log('host id   :', hostId)
console.log()
console.log('Open this URL in your browser and authorize:')
console.log()
console.log(authUrl.toString())
console.log()

const params = await new Promise((resolve, reject) => {
  const server = createServer((req, res) => {
    const url = new URL(req.url ?? '/', `http://127.0.0.1:${port}`)
    if (url.pathname !== '/auth/callback') {
      res.writeHead(404); res.end(); return
    }
    const query = Object.fromEntries(url.searchParams)
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' })
    res.end('<h2>Signed in — you can close this tab.</h2>')
    server.close()
    if (query.error) return reject(new Error(`${query.error} ${query.error_description ?? ''}`))
    resolve(query)
  })
  server.once('error', reject)
  server.listen(port, '127.0.0.1', () => console.log(`waiting for the callback on ${redirectUri} …`))
  setTimeout(() => { server.close(); reject(new Error('timed out waiting for authorization')) }, 900_000)
})

if (params.state !== state) throw new Error('authorization state mismatch')
const clientId = params.client_id
if (!clientId || clientId === 'dynamic_agent_client') {
  throw new Error('registration incomplete: the callback carried no issued client_id')
}

const tokenRes = await fetch(TOKEN_URL, {
  method: 'POST',
  headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  body: new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: clientId,
    code: params.code,
    code_verifier: verifier,
    redirect_uri: redirectUri,
    resource: RESOURCE,
  }),
})
const tokens = await tokenRes.json()
if (!tokenRes.ok) {
  console.log('token exchange failed:', JSON.stringify(tokens).slice(0, 400))
  process.exit(2)
}

const claims = JSON.parse(Buffer.from(tokens.id_token.split('.')[1], 'base64url').toString('utf8'))
const scopes = String(tokens.scope ?? '').split(' ').filter(Boolean)

const record = {
  email: claims.email ?? null,
  issuer: claims.iss,
  subject: claims.sub,
  clientId,
  extAgentHostId: hostId,
  idToken: tokens.id_token,
  accessToken: tokens.access_token,
  refreshToken: tokens.refresh_token,
  tokenType: tokens.token_type,
  scopes,
  expiresAt: Date.now() + (tokens.expires_in ?? 0) * 1000,
  earliestRefreshAt: null,
  savedAt: new Date().toISOString(),
}

mkdirSync(credentialsDir, { recursive: true, mode: 0o700 })
const target = join(credentialsDir, `${fileKey(clientId)}.json`)
writeFileSync(target, JSON.stringify(record, null, 2), { mode: 0o600 })

console.log()
console.log('signed in  :', record.email)
console.log('client_id  :', clientId)
console.log('plan usage :', scopes.includes(PLAN_SCOPE) ? 'enabled' : 'NOT granted')
console.log('saved to   :', target)
console.log()
console.log('Restart DeepSeek Harness if it was already running, then pick a ChatGPT model.')
