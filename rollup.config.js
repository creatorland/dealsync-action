import commonjs from '@rollup/plugin-commonjs'
import json from '@rollup/plugin-json'
import { nodeResolve } from '@rollup/plugin-node-resolve'
import { wasm } from '@rollup/plugin-wasm'
import { string } from 'rollup-plugin-string'

const shared = {
  output: { esModule: true, format: 'es', sourcemap: false },
  plugins: [
    commonjs(),
    nodeResolve({ preferBuiltins: true }),
    json(),
    string({ include: '**/*.md' }),
  ],
}

const withWasm = {
  output: { esModule: true, format: 'es', sourcemap: false },
  plugins: [
    wasm({ targetEnv: 'node', maxFileSize: 0 }),
    commonjs(),
    nodeResolve({ preferBuiltins: true }),
    json(),
  ],
}

function entry(input, output) {
  return { ...shared, input, output: { ...shared.output, file: output } }
}

function wasmEntry(input, output) {
  return { ...withWasm, input, output: { ...withWasm.output, file: output } }
}

export default [
  entry('http/src/index.js', 'http/dist/index.js'),
  entry('base64-decode/src/index.js', 'base64-decode/dist/index.js'),
  entry('filter-emails/src/index.js', 'filter-emails/dist/index.js'),
  entry('fetch-email-content/src/index.js', 'fetch-email-content/dist/index.js'),
  entry('build-ai-prompt/src/index.js', 'build-ai-prompt/dist/index.js'),
  entry('save-detection-results/src/index.js', 'save-detection-results/dist/index.js'),
  wasmEntry('sxt-query/src/index.js', 'sxt-query/dist/index.js'),
  wasmEntry('dispatch-batches/src/index.js', 'dispatch-batches/dist/index.js'),
]
