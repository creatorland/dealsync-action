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

export default [
  {
    input: 'encrypted-http/src/index.js',
    output: { file: 'encrypted-http/dist/index.js', format: 'cjs' },
    plugins,
  },
  {
    input: 'dealsync/src/index.js',
    output: { file: 'dealsync/dist/index.js', format: 'cjs' },
    plugins,
  },
]
