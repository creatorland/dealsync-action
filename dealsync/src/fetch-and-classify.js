import * as crypto from 'crypto'
import * as core from '@actions/core'
import { buildPrompt } from './build-prompt.js'
import {
  saveResults,
  processor,
  sanitizeString,
  sanitizeSchema,
} from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

/**
 * Step 1: Fetch content + call AI + save audit checkpoint.
 *
 * If audit already exists for batch_id → skip AI, return immediately.
 * Otherwise: fetch content → build prompt → call AI → parse JSON → save audit.
 *
 * Output: { skipped: boolean, thread_count: number }
 */
export async function runFetchAndClassify() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = core.getInput('batch-id')
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const hyperbolicKey = core.getInput('hyperbolic-key')
  const primaryModel = core.getInput('primary-model') || 'deepseek-ai/DeepSeek-V3'
  const fallbackModel = core.getInput('fallback-model') || 'Qwen/Qwen2.5-72B-Instruct'
  const aiApiUrl = core.getInput('ai-api-url') || 'https://api.hyperbolic.xyz/v1/chat/completions'

  if (!batchId) throw new Error('batch-id is required')

  console.log(`[classify] starting batch ${batchId}`)

  const jwt = await authenticate(authUrl, authSecret)

  // Check metadata exists
  const metadataRows = await executeSql(apiUrl, jwt, biscuit,
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, SYNC_STATE_ID, THREAD_ID
    FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`)

  if (!metadataRows || metadataRows.length === 0) {
    console.log('[classify] no rows for batch (already completed?)')
    return { skipped: true, thread_count: 0 }
  }

  console.log(`[classify] ${metadataRows.length} deal_states`)

  // Increment attempts
  await executeSql(apiUrl, jwt, biscuit, processor.incrementAttempts(schema, batchId))

  // Check for existing audit (checkpoint)
  const existingAudit = await executeSql(apiUrl, jwt, biscuit, saveResults.getAuditByBatchId(schema, batchId))

  if (existingAudit.length > 0 && existingAudit[0].AI_EVALUATION) {
    console.log('[classify] audit exists — skipping AI call')
    try {
      const parsed = JSON.parse(existingAudit[0].AI_EVALUATION)
      return { skipped: true, thread_count: (parsed.threads || []).length }
    } catch {
      console.log('[classify] existing audit has invalid JSON, re-running AI')
    }
  }

  // Fetch content
  const userId = metadataRows[0].USER_ID
  const syncStateId = metadataRows[0].SYNC_STATE_ID
  const messageIds = metadataRows.map((r) => r.MESSAGE_ID)

  const MAX_PER_BATCH = 50
  const allEmails = []
  for (let i = 0; i < messageIds.length; i += MAX_PER_BATCH) {
    const chunk = messageIds.slice(i, i + MAX_PER_BATCH)
    try {
      const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, syncStateId, messageIds: chunk }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const result = await resp.json()
      const emails = result.data || result
      for (const email of emails) {
        const meta = metadataRows.find((r) => r.MESSAGE_ID === email.messageId)
        if (meta) {
          email.id = meta.EMAIL_METADATA_ID
          email.threadId = meta.THREAD_ID
          if (meta.PREVIOUS_AI_SUMMARY) email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
        }
        allEmails.push(email)
      }
    } catch (err) {
      console.log(`[classify] content fetch failed: ${err.message}`)
    }
  }

  if (allEmails.length === 0) throw new Error('No email content fetched')

  // Build prompt + call AI
  const { systemPrompt, userPrompt } = buildPrompt(allEmails)

  let aiResponseRaw = null
  for (const model of [primaryModel, fallbackModel]) {
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        console.log(`[classify] AI: ${model} (attempt ${attempt}/2)`)
        const controller = new AbortController()
        const timeout = setTimeout(() => controller.abort(), 90000)
        const resp = await fetch(aiApiUrl, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${hyperbolicKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            model,
            messages: [
              { role: 'system', content: systemPrompt },
              { role: 'user', content: userPrompt },
            ],
            response_format: { type: 'json_object' },
          }),
          signal: controller.signal,
        })
        clearTimeout(timeout)

        if (!resp.ok) {
          console.log(`[classify] AI ${model} attempt ${attempt}: HTTP ${resp.status}`)
          if (attempt < 2) await new Promise((r) => setTimeout(r, 3000))
          continue
        }

        const result = await resp.json()
        aiResponseRaw = result.choices?.[0]?.message?.content
        if (aiResponseRaw) break
      } catch (err) {
        console.log(`[classify] AI ${model} attempt ${attempt}: ${err.message}`)
        if (attempt < 2) await new Promise((r) => setTimeout(r, 3000))
      }
    }
    if (aiResponseRaw) break
  }

  if (!aiResponseRaw) throw new Error('All AI models failed')

  // Parse + save audit checkpoint
  aiResponseRaw = aiResponseRaw.replace(/^```(?:json)?\s*\n?/i, '').replace(/\n?```\s*$/i, '')
  const aiOutput = JSON.parse(aiResponseRaw)

  const auditId = crypto.randomUUID()
  const evaluation = sanitizeString(JSON.stringify(aiOutput).substring(0, 6400))
  await executeSql(apiUrl, jwt, biscuit,
    saveResults.insertAudit(schema, {
      id: auditId, batchId, threadCount: (aiOutput.threads || []).length,
      emailCount: metadataRows.length, cost: 0, inputTokens: 0,
      outputTokens: 0, model: primaryModel, evaluation,
    }))

  console.log(`[classify] audit saved: ${auditId}, ${(aiOutput.threads || []).length} threads`)
  return { skipped: false, thread_count: (aiOutput.threads || []).length }
}
