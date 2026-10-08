/**
 * Bundle the plugin into loadable ESM.
 *
 * Three entry points, all emitted into `lib/`:
 *   bootstrap.ts -> bootstrap.js  (host discovery + synchronous resolve hook)
 *   index.ts     -> index.js      (entry: init bootstrap, then load the body)
 *   main.ts      -> main.js       (the plugin body; imports DSH peers)
 *
 * `index.js` dynamically imports `./main.js`, which is kept external so the
 * body is evaluated only after the resolve hook exists. Every local module is
 * inlined; only `@deepseek-ai/*` stays external (provided by the DSH runtime).
 */
import { build } from 'esbuild'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const shared = {
  bundle: true,
  format: 'esm',
  platform: 'node',
  target: 'node22',
  sourcemap: false,
  external: ['@deepseek-ai/*'],
  logLevel: 'warning',
}

/** Keep the plugin-body import external so it loads after bootstrap. */
const externalBody = {
  name: 'external-plugin-body',
  setup(build) {
    build.onResolve({ filter: /^\.\/main\.ts$/ }, () => ({
      path: './main.js',
      external: true,
    }))
  },
}

await build({
  ...shared,
  entryPoints: [join(root, 'src/bootstrap.ts')],
  outfile: join(root, 'lib/bootstrap.js'),
})

await build({
  ...shared,
  entryPoints: [join(root, 'src/index.ts')],
  outfile: join(root, 'lib/index.js'),
  plugins: [externalBody],
})

await build({
  ...shared,
  entryPoints: [join(root, 'src/main.ts')],
  outfile: join(root, 'lib/main.js'),
})

console.log('built lib/bootstrap.js, lib/index.js, lib/main.js')
