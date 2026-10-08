/**
 * Loopback callback listener.
 *
 * Hard constraints from the docs (and verified live):
 *  - scheme/host/path are FIXED: http://127.0.0.1:PORT/auth/callback
 *  - "Do not substitute with `localhost`"
 *  - only the PORT may vary between deployments
 *  - the exact redirect_uri (including port) must be used in BOTH the
 *    authorize request and the code exchange
 */

import { createServer, type Server } from 'node:http'
import { CALLBACK_PATH } from './config.ts'

export interface CallbackResult {
  code: string
  /** Present on first registration; absent on some reauthorizations. */
  clientId?: string
  scope?: string
  state: string
}

export interface CallbackHandle {
  /** The exact redirect URI to send to the authorize endpoint. */
  redirectUri: string
  /** Resolves with the callback parameters, or rejects on error/timeout. */
  waitForResult: Promise<CallbackResult>
  /** Stop listening; safe to call more than once. */
  close: () => void
}

export interface StartCallbackOptions {
  host: string
  /** Preferred port. */
  port: number
  /** How many subsequent ports to try if the preferred one is taken. */
  portAttempts?: number
  /** Abort the wait. */
  signal?: AbortSignal
  timeoutMs?: number
}

const SUCCESS_HTML = `<!doctype html><meta charset="utf-8">
<title>Sign in complete</title>
<body style="font:16px/1.5 system-ui;padding:2rem">
<h2>Sign-in complete</h2><p>You can close this tab and return to your app.</p></body>`

const FAILURE_HTML = `<!doctype html><meta charset="utf-8">
<title>Sign in failed</title>
<body style="font:16px/1.5 system-ui;padding:2rem">
<h2>Sign-in failed</h2><p>Return to your app for details.</p></body>`

function listen(server: Server, port: number, host: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: NodeJS.ErrnoException): void => {
      server.removeListener('listening', onListening)
      reject(error)
    }
    const onListening = (): void => {
      server.removeListener('error', onError)
      resolve()
    }
    server.once('error', onError)
    server.once('listening', onListening)
    server.listen(port, host)
  })
}

/**
 * Start the loopback listener, trying the preferred port and then a few
 * subsequent ones. The path is never changed.
 */
export async function startCallbackServer(
  options: StartCallbackOptions,
): Promise<CallbackHandle> {
  const attempts = Math.max(1, options.portAttempts ?? 10)
  let server: Server | null = null
  let boundPort = 0

  for (let i = 0; i < attempts; i++) {
    const candidate = options.port + i
    const s = createServer()
    try {
      await listen(s, candidate, options.host)
      server = s
      boundPort = candidate
      break
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code
      s.close()
      if (code !== 'EADDRINUSE' && code !== 'EACCES') throw error
    }
  }
  if (!server) {
    throw new Error(
      `could not bind a callback port in ${options.port}..${options.port + attempts - 1}`,
    )
  }

  let settle: (result: CallbackResult) => void
  let fail: (error: Error) => void
  const waitForResult = new Promise<CallbackResult>((resolve, reject) => {
    settle = resolve
    fail = reject
  })

  let done = false
  const cleanup = (): void => {
    if (done) return
    done = true
    clearTimeout(timer)
    options.signal?.removeEventListener('abort', onAbort)
    server?.close()
  }

  const timer = setTimeout(() => {
    cleanup()
    fail(new Error('timed out waiting for the authorization callback'))
  }, options.timeoutMs ?? 900_000)

  const onAbort = (): void => {
    cleanup()
    fail(new Error('authorization was cancelled'))
  }
  options.signal?.addEventListener('abort', onAbort, { once: true })

  server.on('request', (req, res) => {
    const url = new URL(req.url ?? '/', `http://${options.host}:${boundPort}`)
    if (url.pathname !== CALLBACK_PATH) {
      res.writeHead(404, { 'content-type': 'text/plain' })
      res.end('not found')
      return
    }

    const params = url.searchParams
    const error = params.get('error')
    if (error) {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' })
      res.end(FAILURE_HTML)
      cleanup()
      fail(
        new Error(
          `authorization denied: ${error}${params.get('error_description') ? ` (${params.get('error_description')})` : ''}`,
        ),
      )
      return
    }

    const code = params.get('code')
    const state = params.get('state')
    if (!code || !state) {
      res.writeHead(400, { 'content-type': 'text/plain' })
      res.end('missing code or state')
      return
    }

    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' })
    res.end(SUCCESS_HTML)
    cleanup()
    settle({
      code,
      state,
      clientId: params.get('client_id') ?? undefined,
      scope: params.get('scope') ?? undefined,
    })
  })

  return {
    redirectUri: `http://${options.host}:${boundPort}${CALLBACK_PATH}`,
    waitForResult,
    close: cleanup,
  }
}
