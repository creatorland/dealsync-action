import * as crypto from 'crypto'
import * as core from '@actions/core'
import { buildPrompt } from '../lib/build-prompt.js'
import {
  saveResults,
  sanitizeString,
  sanitizeSchema,
  sanitizeId,
} from '../lib/queries.js'
import { authenticate, executeSql, withTimeout } from '../lib/sxt-client.js'

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
  const batchId = sanitizeId(core.getInput('batch-id'))
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const hyperbolicKey = core.getInput('hyperbolic-key')
  const primaryModel = core.getInput('primary-model') || 'deepseek-ai/DeepSeek-V3-0324'
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

  const MAX_PER_CHUNK = 10
  const allEmails = []
  for (let i = 0; i < messageIds.length; i += MAX_PER_CHUNK) {
    const chunk = messageIds.slice(i, i + MAX_PER_CHUNK)
    try {
      const { signal, clear } = withTimeout()
      const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, syncStateId, messageIds: chunk }),
        signal,
      })
      clear()
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

  const AI_REQUEST_TIMEOUT_MS = 90000
  const AI_RETRY_DELAY_MS = 3000
  const MAX_HTTP_RETRIES = 2

  /**
   * Call an AI model. Returns raw response string or null on HTTP failure.
   * Retries on HTTP errors (same model). Throws on abort/network errors after retries.
   */
  async function callModel(model, messages) {
    for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt++) {
      try {
        console.log(`[classify] AI: ${model} (attempt ${attempt}/${MAX_HTTP_RETRIES})`)
        const controller = new AbortController()
        const timeout = setTimeout(() => controller.abort(), AI_REQUEST_TIMEOUT_MS)
        const resp = await fetch(aiApiUrl, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${hyperbolicKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ model, messages, response_format: { type: 'json_object' } }),
          signal: controller.signal,
        })
        clearTimeout(timeout)

        if (!resp.ok) {
          const errBody = await resp.text().catch(() => '')
          console.log(`[classify] AI ${model} HTTP ${resp.status}: ${errBody.substring(0, 200)}`)
          if (attempt < MAX_HTTP_RETRIES) {
            await new Promise((r) => setTimeout(r, AI_RETRY_DELAY_MS))
            continue
          }
          return null // HTTP failure after all retries
        }

        const result = await resp.json()
        return result.choices?.[0]?.message?.content || null
      } catch (err) {
        console.log(`[classify] AI ${model} attempt ${attempt}: ${err.message}`)
        if (attempt < MAX_HTTP_RETRIES) {
          await new Promise((r) => setTimeout(r, AI_RETRY_DELAY_MS))
          continue
        }
        return null
      }
    }
    return null
  }

  /** Strip markdown fences and parse JSON. Returns parsed object or null. */
  function tryParseJson(raw) {
    try {
      const cleaned = raw.trim().replace(/^```(?:json)?\s*\n?/i, '').replace(/\n?```\s*$/i, '').trim()
      return JSON.parse(cleaned)
    } catch {
      return null
    }
  }

  /** Ask a model to fix broken JSON. Returns parsed object or null. */
  async function repairJson(fixerModel, brokenJson) {
    console.log(`[classify] JSON repair: asking ${fixerModel} to fix malformed response`)
    const repairMessages = [
      {
        role: 'system',
        content: 'You are a JSON repair tool. Fix the malformed JSON below so it is valid. Return ONLY the corrected JSON array. No markdown, no explanation.',
      },
      { role: 'user', content: brokenJson },
    ]
    const fixed = await callModel(fixerModel, repairMessages)
    if (!fixed) return null
    return tryParseJson(fixed)
  }

  // --- AI Classification Pipeline ---
  const classifyMessages = [
    { role: 'system', content: systemPrompt },
    { role: 'user', content: userPrompt },
  ]

  let aiOutput = null
  let modelUsed = primaryModel

  // Step 1: Try primary model
  const primaryRaw = await callModel(primaryModel, classifyMessages)

  if (primaryRaw) {
    aiOutput = tryParseJson(primaryRaw)
    if (!aiOutput) {
      // Primary succeeded but returned invalid JSON → ask fallback to fix it
      console.log(`[classify] Primary model returned invalid JSON, attempting cross-model repair`)
      aiOutput = await repairJson(fallbackModel, primaryRaw)
      if (aiOutput) {
        console.log(`[classify] JSON repaired by ${fallbackModel}`)
        modelUsed = `${primaryModel}+${fallbackModel}(repair)`
      }
    }
  }

  // Step 2: If primary failed entirely or repair failed → try fallback model
  if (!aiOutput) {
    console.log(`[classify] Primary model failed, trying fallback: ${fallbackModel}`)
    modelUsed = fallbackModel
    const fallbackRaw = await callModel(fallbackModel, classifyMessages)

    if (fallbackRaw) {
      aiOutput = tryParseJson(fallbackRaw)
      if (!aiOutput) {
        // Fallback succeeded but returned invalid JSON → ask primary to fix it
        console.log(`[classify] Fallback model returned invalid JSON, attempting cross-model repair`)
        aiOutput = await repairJson(primaryModel, fallbackRaw)
        if (aiOutput) {
          console.log(`[classify] JSON repaired by ${primaryModel}`)
          modelUsed = `${fallbackModel}+${primaryModel}(repair)`
        }
      }
    }
  }

  // Step 3: If everything failed
  if (!aiOutput) throw new Error('All AI models failed — both primary and fallback returned no valid JSON')

  // Normalize: prompt returns array, downstream expects { threads: [...] }
  if (Array.isArray(aiOutput)) {
    aiOutput = { threads: aiOutput }
  }

  const auditId = crypto.randomUUID()
  const evaluation = sanitizeString(JSON.stringify(aiOutput).substring(0, 6400))
  try {
    await executeSql(apiUrl, jwt, biscuit,
      saveResults.insertAudit(schema, {
        id: auditId, batchId, threadCount: (aiOutput.threads || []).length,
        emailCount: metadataRows.length, cost: 0, inputTokens: 0,
        outputTokens: 0, model: modelUsed, evaluation,
      }))
    console.log(`[classify] audit saved: ${auditId}`)
  } catch (err) {
    // Unique constraint on batch_id — another run already saved the audit
    if (err.message.includes('integrity constraint') || err.message.includes('unique') || err.message.includes('duplicate')) {
      console.log(`[classify] audit already exists for batch (concurrent run), continuing`)
    } else {
      throw err
    }
  }

  console.log(`[classify] ${(aiOutput.threads || []).length} threads ready for processing`)
  return { skipped: false, thread_count: (aiOutput.threads || []).length }
}
