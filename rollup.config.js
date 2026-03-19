import commonjs from '@rollup/plugin-commonjs'
import json from '@rollup/plugin-json'
import { nodeResolve } from '@rollup/plugin-node-resolve'
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

function entry(input, output, external = []) {
  return { ...shared, external, input, output: { ...shared.output, file: output } }
}

export default [
  entry('http/src/index.js', 'http/dist/index.js'),
  entry('base64-decode/src/index.js', 'base64-decode/dist/index.js'),
  entry('filter-emails/src/index.js', 'filter-emails/dist/index.js'),
  entry('fetch-email-content/src/index.js', 'fetch-email-content/dist/index.js'),
  entry('dispatch-batches/src/index.js', 'dispatch-batches/dist/index.js'),
  entry('save-detection-results/src/index.js', 'save-detection-results/dist/index.js'),
  entry('build-ai-prompt/src/index.js', 'build-ai-prompt/dist/index.js'),
  entry('sxt-query/src/index.js', 'sxt-query/dist/index.js', ['sxt-nodejs-sdk']),
]
