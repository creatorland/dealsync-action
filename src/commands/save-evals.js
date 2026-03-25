import { uuidv7 } from 'uuidv7'
import * as core from '@actions/core'
import {
  saveResults,
  sanitizeId,
  sanitizeString,
  sanitizeSchema,
} from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'

/**
 * Step 2: Read audit by batch_id → upsert thread evaluations.
 * Batched: single multi-row INSERT ... ON CONFLICT for all threads.
 */
export async function runSaveEvals() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = sanitizeId(core.getInput('batch-id'))

  if (!batchId) throw new Error('batch-id is required')

  const jwt = await authenticate(authUrl, authSecret)

  // Read audit
  const audits = await executeSql(apiUrl, jwt, biscuit, saveResults.getAuditByBatchId(schema, batchId))
  if (audits.length === 0 || !audits[0].AI_EVALUATION) {
    console.log('[save-evals] no audit found — skipping')
    return { upserted: 0 }
  }

  const aiOutput = JSON.parse(audits[0].AI_EVALUATION)
  const threads = aiOutput.threads || []

  if (threads.length === 0) {
    console.log('[save-evals] no threads in audit')
    return { upserted: 0 }
  }

  // Build batched VALUES for all threads
  const values = threads.map((thread) => {
    const threadId = sanitizeId(thread.thread_id)
    const evalId = uuidv7()
    const category = sanitizeString(thread.category || '')
    const aiSummary = sanitizeString(thread.ai_summary || '')
    const isDeal = thread.is_deal ? 'true' : 'false'
    const isLikelyScam = (thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'
    const aiScore = typeof thread.ai_score === 'number' ? thread.ai_score : 0
    return `('${evalId}', '${threadId}', '', '${category}', '${aiSummary}', ${isDeal}, ${isLikelyScam}, ${aiScore}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
  }).join(', ')

  const sql = `INSERT INTO ${schema}.EMAIL_THREAD_EVALUATIONS
    (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT)
  VALUES ${values}
  ON CONFLICT (THREAD_ID) DO UPDATE SET
    AI_EVALUATION_AUDIT_ID = EXCLUDED.AI_EVALUATION_AUDIT_ID,
    AI_INSIGHT = EXCLUDED.AI_INSIGHT,
    AI_SUMMARY = EXCLUDED.AI_SUMMARY,
    IS_DEAL = EXCLUDED.IS_DEAL,
    LIKELY_SCAM = EXCLUDED.LIKELY_SCAM,
    AI_SCORE = EXCLUDED.AI_SCORE,
    UPDATED_AT = CURRENT_TIMESTAMP`

  await executeSql(apiUrl, jwt, biscuit, sql)

  console.log(`[save-evals] upserted ${threads.length} thread evaluations (1 query)`)
  return { upserted: threads.length, total: threads.length }
}
