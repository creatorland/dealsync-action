import * as crypto from 'crypto'
import * as core from '@actions/core'
import { sanitizeSchema } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

const BATCH_SIZE = 100

/**
 * Query the diff between email_metadata and deal_states,
 * then insert missing deal_states rows with status='pending'.
 */
export async function runCreateDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const offset = parseInt(core.getInput('offset') || '0', 10)
  const limit = parseInt(core.getInput('limit') || '1000', 10)

  console.log(`[create-deal-states] querying diff (limit=${limit}, offset=${offset})`)
  const jwt = await authenticate(authUrl, authSecret)

  const diffSql = `SELECT em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID
FROM EMAIL_CORE_STAGING.EMAIL_METADATA em
WHERE em.PROCESSING_STATUS != 'pending'
  AND em.ID NOT IN (SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES)
ORDER BY em.RECEIVED_AT ASC
LIMIT ${limit} OFFSET ${offset}`

  const rows = await executeSql(apiUrl, jwt, biscuit, diffSql)

  if (!rows || rows.length === 0) {
    console.log('[create-deal-states] no new emails to process')
    return { created_count: 0, skipped_count: 0 }
  }

  console.log(`[create-deal-states] found ${rows.length} new email(s) to insert`)

  let createdCount = 0
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const chunk = rows.slice(i, i + BATCH_SIZE)
    const values = chunk
      .map((em) => {
        const id = crypto.randomUUID()
        return `('${id}', '${em.ID}', '${em.USER_ID}', '${em.THREAD_ID}', '${em.MESSAGE_ID}', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
      })
      .join(',\n')

    const insertSql = `INSERT INTO ${schema}.DEAL_STATES (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT)
VALUES ${values}
ON CONFLICT (EMAIL_METADATA_ID) DO NOTHING`

    await executeSql(apiUrl, jwt, biscuit, insertSql)
    createdCount += chunk.length
    console.log(`[create-deal-states] inserted batch ${Math.floor(i / BATCH_SIZE) + 1} (${chunk.length} rows)`)
  }

  console.log(`[create-deal-states] done: created=${createdCount}`)
  return { created_count: createdCount, skipped_count: 0 }
}
