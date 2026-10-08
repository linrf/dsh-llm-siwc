/**
 * System-browser launcher.
 *
 * The docs require opening the authorize URL in the SYSTEM browser, not an
 * embedded webview. Injectable so DSH can supply its own shell integration.
 */

import { spawn } from 'node:child_process'

export interface BrowserLauncher {
  open(url: string): Promise<void>
}

/** Launch via the platform's default handler. */
export class SystemBrowserLauncher implements BrowserLauncher {
  async open(url: string): Promise<void> {
    const command =
      process.platform === 'darwin'
        ? { file: 'open', args: [url] }
        : process.platform === 'win32'
          ? { file: 'cmd', args: ['/c', 'start', '', url] }
          : { file: 'xdg-open', args: [url] }

    await new Promise<void>((resolve, reject) => {
      const child = spawn(command.file, command.args, { stdio: 'ignore', detached: true })
      child.once('error', reject)
      child.once('spawn', () => {
        child.unref()
        resolve()
      })
    })
  }
}

/** Records URLs instead of opening them — used by tests. */
export class RecordingBrowserLauncher implements BrowserLauncher {
  readonly opened: string[] = []

  async open(url: string): Promise<void> {
    this.opened.push(url)
  }
}
