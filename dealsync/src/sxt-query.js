import * as core from '@actions/core'
import { authenticate, executeSql } from './sxt-client.js'
import { sanitizeSchema } from '../../shared/queries.js'

/**
 * Standalone SxT query/execute command.
 * Authenticates via proxy, uses pre-generated biscuit from input.
 */
export async function runSxtQuery() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = core.getInput('schema')
  const sql = core.getInput('sql')

  if (schema) sanitizeSchema(schema)

  core.info(`sxt-query: schema=${schema || '(none)'}, sql=${sql.substring(0, 100)}...`)
  const jwt = await authenticate(authUrl, authSecret)
  const result = await executeSql(apiUrl, jwt, biscuit, sql)
  core.info(`sxt-query: returned ${Array.isArray(result) ? result.length : 0} rows`)

  return { result }
}
