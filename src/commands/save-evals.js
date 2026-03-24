import * as crypto from 'crypto'
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
 * Idempotent: ON CONFLICT (THREAD_ID) DO UPDATE.
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

  let upserted = 0
  let failed = 0
  for (const thread of threads) {
    try {
      const threadId = sanitizeId(thread.thread_id)
      const evalId = crypto.randomUUID()
      const category = sanitizeString(thread.category || '')
      const aiSummary = sanitizeString(thread.ai_summary || '')
      const isDeal = thread.is_deal ? 'true' : 'false'
      const isLikelyScam = (thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'
      const aiScore = typeof thread.ai_score === 'number' ? thread.ai_score : 0

      await executeSql(apiUrl, jwt, biscuit,
        saveResults.upsertThreadEvaluation(schema, {
          id: evalId, threadId, auditId: '', category, summary: aiSummary,
          isDeal, likelyScam: isLikelyScam, score: aiScore,
        }))
      upserted++
    } catch (err) {
      failed++
      core.error(`Failed eval for thread ${thread.thread_id}: ${err.message}`)
    }
  }

  console.log(`[save-evals] upserted ${upserted}/${threads.length}, failed ${failed}`)
  if (failed > 0) throw new Error(`${failed}/${threads.length} thread eval(s) failed`)
  return { upserted, total: threads.length }
}
