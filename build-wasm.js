import { build } from 'esbuild'
import { wasmLoader } from 'esbuild-plugin-wasm'

const actions = ['sxt-query', 'dispatch-batches', 'fetch-email-content']

for (const action of actions) {
  await build({
    entryPoints: [`${action}/src/index.js`],
    bundle: true,
    outfile: `${action}/dist/index.js`,
    format: 'esm',
    platform: 'node',
    target: 'node24',
    plugins: [wasmLoader({ mode: 'deferred' })],
    banner: { js: "import { createRequire } from 'module'; const require = createRequire(import.meta.url);" },
  })
  console.log(`Built ${action}/dist/index.js`)
}
