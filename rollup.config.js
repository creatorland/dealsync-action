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
    input: 'src/index.js',
    output: { file: 'dist/index.js', format: 'es' },
    plugins,
  },
]
