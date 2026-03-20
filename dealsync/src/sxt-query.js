import * as core from '@actions/core'
import { authenticate, executeSql } from './sxt-client.js'
import { sanitizeSchema } from '../../shared/queries.js'

/**
 * Standalone SxT query/execute command.
 * Replaces w3-io/w3-sxt-action@main which has CJS/ESM incompatibility on W3 runtime.
 *
 * Inputs: command (query|execute), auth-url, auth-secret, api-url, biscuit, schema, sql
 * Output: { result } — JSON array for queries, or execution result
 */
export async function runSxtQuery() {
  const command = core.getInput('sxt-command') || 'query'
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = core.getInput('schema')
  const sql = core.getInput('sql')

  if (schema) sanitizeSchema(schema)

  const jwt = await authenticate(authUrl, authSecret)
  const result = await executeSql(apiUrl, jwt, biscuit, sql)

  return { result }
}
