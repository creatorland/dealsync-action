import * as core from '@actions/core'
import { authenticate, generateBiscuit, executeSql } from './sxt-client.js'
import { sanitizeSchema } from '../../shared/queries.js'

/**
 * Standalone SxT query/execute command.
 * Authenticates via proxy, generates biscuit per-run, executes SQL.
 *
 * Input: auth-url, auth-secret, api-url, schema, sql
 * Output: { result } — JSON array for queries, or execution result
 */
export async function runSxtQuery() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const schema = sanitizeSchema(core.getInput('schema'))
  const sql = core.getInput('sql')

  const jwt = await authenticate(authUrl, authSecret)
  const biscuit = await generateBiscuit(apiUrl, jwt, schema)
  const result = await executeSql(apiUrl, jwt, biscuit, sql)

  return { result }
}
