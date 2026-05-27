import commonjs from '@rollup/plugin-commonjs'
import json from '@rollup/plugin-json'
import { nodeResolve } from '@rollup/plugin-node-resolve'
import { string } from 'rollup-plugin-string'

const plugins = [
  nodeResolve({ preferBuiltins: true }),
  commonjs(),
  json(),
  string({ include: '**/*.md' }),
]

// Story 0.6 / FR-P0-6: settlement-worker.yaml dispatches via `uses:` against
// each portable action's bundled `dist/index.js`. The Story 0.2 source modules
// at `src/outbox-settlement-{guard,worker,finalizer}/index.js` export a pure
// `run()` API; the matching `main.js` files in those directories are thin
// @actions/core wrappers we Rollup into `dist/index.js`.
const settlementActions = ['outbox-settlement-guard', 'outbox-settlement-worker', 'outbox-settlement-finalizer']

export default [
  {
    input: 'src/index.js',
    output: { file: 'dist/index.js', format: 'es' },
    plugins,
  },
  ...settlementActions.map((name) => ({
    input: `src/${name}/main.js`,
    output: { file: `src/${name}/dist/index.js`, format: 'es' },
    plugins,
  })),
]
