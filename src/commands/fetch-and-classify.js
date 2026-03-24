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

// --- Constants ---
const AI_REQUEST_TIMEOUT_MS = 120000
const AI_RETRY_DELAY_MS = 1000
const AI_BACKOFF_MULTIPLIER = 2
const MAX_HTTP_RETRIES = 2
const MAX_TOKENS = 20480

// --- Valid categories and deal types for validation ---
const VALID_CATEGORIES = new Set([
  'new', 'in_progress', 'completed', 'not_interested', 'likely_scam', 'low_confidence',
])
const VALID_DEAL_TYPES = new Set([
  'brand_collaboration', 'sponsorship', 'affiliate', 'product_seeding',
  'ambassador', 'content_partnership', 'paid_placement', 'other_business',
])

/**
 * Step 1: Fetch content + call AI + save audit checkpoint.
 *
 * Resilience pipeline:
 *   Layer 0: Primary model call (with HTTP retries + exponential backoff)
 *   Layer 1: Local JSON repair (strip fences, extract array, coerce schema)
 *   Layer 2: Corrective retry (send broken output back to same model with error)
 *   Layer 3: Fallback model (same prompt, different model)
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
  const primaryModel = core.getInput('primary-model') || 'Qwen/Qwen3-235B-A22B-Instruct-2507'
  const fallbackModel = core.getInput('fallback-model') || 'moonshotai/Kimi-K2-Instruct'
  const aiApiUrl = core.getInput('ai-api-url') || 'https://api.hyperbolic.xyz/v1/chat/completions'

  if (!batchId) throw new Error('batch-id is required')

  console.log(`[classify] starting batch ${batchId}`)

  const jwt = await authenticate(authUrl, authSecret)

  // Check metadata exists
  const metadataRows = await executeSql(
    apiUrl, jwt, biscuit,
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, SYNC_STATE_ID, THREAD_ID
    FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
  )

  if (!metadataRows || metadataRows.length === 0) {
    console.log('[classify] no rows for batch (already completed?)')
    return { skipped: true, thread_count: 0 }
  }

  console.log(`[classify] ${metadataRows.length} deal_states`)

  // Check for existing audit (checkpoint)
  const existingAudit = await executeSql(
    apiUrl, jwt, biscuit, saveResults.getAuditByBatchId(schema, batchId),
  )

  if (existingAudit.length > 0 && existingAudit[0].AI_EVALUATION) {
    console.log('[classify] audit exists — skipping AI call')
    try {
      const parsed = JSON.parse(existingAudit[0].AI_EVALUATION)
      return { skipped: true, thread_count: (parsed.threads || parsed || []).length }
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

  // Build prompt
  const { systemPrompt, userPrompt } = buildPrompt(allEmails)

  // =========================================================================
  //  AI RESILIENCE PIPELINE
  // =========================================================================

  /**
   * Call a model with HTTP retries + exponential backoff.
   * Returns raw content string or throws on total failure.
   */
  async function callModel(model, messages, { temperature = 0 } = {}) {
    let lastError
    for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt++) {
      try {
        console.log(`[classify] AI: ${model} (attempt ${attempt}/${MAX_HTTP_RETRIES})`)
        const resp = await fetch(aiApiUrl, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${hyperbolicKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            model,
            messages,
            temperature,
            max_tokens: MAX_TOKENS,
            response_format: { type: 'json_object' },
          }),
          signal: AbortSignal.timeout(AI_REQUEST_TIMEOUT_MS),
        })

        if (!resp.ok) {
          const errBody = await resp.text().catch(() => '')
          lastError = new Error(`HTTP ${resp.status}: ${errBody.substring(0, 200)}`)
          console.log(`[classify] AI ${model} HTTP ${resp.status}: ${errBody.substring(0, 200)}`)
          if (attempt < MAX_HTTP_RETRIES) {
            const delay = AI_RETRY_DELAY_MS * Math.pow(AI_BACKOFF_MULTIPLIER, attempt - 1)
            await new Promise((r) => setTimeout(r, delay))
            continue
          }
          throw lastError
        }

        const result = await resp.json()
        const content = result.choices?.[0]?.message?.content
        if (!content) throw new Error('Empty response from model')
        return content
      } catch (err) {
        lastError = err
        if (attempt < MAX_HTTP_RETRIES) {
          const delay = AI_RETRY_DELAY_MS * Math.pow(AI_BACKOFF_MULTIPLIER, attempt - 1)
          console.log(`[classify] AI ${model} attempt ${attempt} failed: ${err.message}, retrying in ${delay}ms`)
          await new Promise((r) => setTimeout(r, delay))
        }
      }
    }
    throw lastError || new Error(`${model} failed after ${MAX_HTTP_RETRIES} attempts`)
  }

  /**
   * Layer 1: Local JSON repair — strip fences, extract array, unwrap objects, coerce schema.
   * Returns validated array or throws with parse error details.
   */
  function parseAndValidate(raw) {
    let content = raw.trim()

    // Strip markdown fences
    content = content.replace(/^```(?:json)?\s*\n?/gi, '').replace(/\n?```\s*$/gi, '').trim()

    // Try to find JSON array in mixed output
    if (!content.startsWith('[')) {
      const arrayStart = content.indexOf('[')
      const arrayEnd = content.lastIndexOf(']')
      if (arrayStart !== -1 && arrayEnd > arrayStart) {
        content = content.slice(arrayStart, arrayEnd + 1)
      }
    }

    // Parse
    let parsed
    try {
      parsed = JSON.parse(content)
    } catch {
      // Try to extract from wrapper object like {"results": [...]}
      const objStart = content.indexOf('{')
      const objEnd = content.lastIndexOf('}')
      if (objStart !== -1 && objEnd > objStart) {
        const obj = JSON.parse(content.slice(objStart, objEnd + 1))
        const arrays = Object.values(obj).filter(Array.isArray)
        if (arrays.length === 1) {
          parsed = arrays[0]
        } else {
          throw new Error('Cannot extract JSON array from response')
        }
      } else {
        throw new Error('No valid JSON found in response')
      }
    }

    // Unwrap if object with single array property
    if (!Array.isArray(parsed)) {
      const arrays = Object.values(parsed).filter(Array.isArray)
      if (arrays.length === 1) {
        parsed = arrays[0]
      } else {
        throw new Error('Response is not a JSON array')
      }
    }

    // Schema validation and coercion
    return parsed.map((r) => ({
      thread_id: String(r.thread_id || ''),
      is_deal: Boolean(r.is_deal),
      is_english: r.is_english !== false,
      language: r.language || null,
      ai_score: Math.min(10, Math.max(1, Math.round(Number(r.ai_score) || 5))),
      category: r.is_deal
        ? (VALID_CATEGORIES.has(r.category) ? r.category : 'low_confidence')
        : null,
      likely_scam: Boolean(r.likely_scam) || r.category === 'likely_scam',
      ai_insight: String(r.ai_insight || ''),
      ai_summary: String(r.ai_summary || '').slice(0, 1000),
      main_contact: r.is_deal ? (r.main_contact || null) : null,
      deal_brand: r.is_deal ? (r.deal_brand || null) : null,
      deal_type: r.is_deal
        ? (VALID_DEAL_TYPES.has(r.deal_type) ? r.deal_type : 'other_business')
        : null,
      deal_name: r.is_deal ? (r.deal_name || null) : null,
      deal_value: r.deal_value != null ? Number(r.deal_value) : null,
      deal_currency: r.deal_currency || null,
    }))
  }

  const classifyMessages = [
    { role: 'system', content: systemPrompt },
    { role: 'user', content: userPrompt },
  ]

  let threads = null
  let modelUsed = primaryModel

  // --- Layer 0: Primary model call ---
  let primaryRaw
  try {
    primaryRaw = await callModel(primaryModel, classifyMessages, { temperature: 0 })
  } catch (primaryApiError) {
    console.log(`[classify] Primary model API failed: ${primaryApiError.message}`)
    primaryRaw = null
  }

  if (primaryRaw) {
    // --- Layer 1: Local JSON repair ---
    try {
      threads = parseAndValidate(primaryRaw)
      console.log(`[classify] Primary model succeeded: ${threads.length} threads`)
    } catch (parseError) {
      console.log(`[classify] Primary JSON parse failed: ${parseError.message}`)

      // --- Layer 2: Corrective retry (same model, send broken output back) ---
      try {
        console.log(`[classify] Attempting corrective retry with ${primaryModel}`)
        const correctiveMessages = [
          ...classifyMessages,
          { role: 'assistant', content: primaryRaw },
          {
            role: 'user',
            content: `Your previous classification response could not be parsed as valid JSON.\n\nParse error:\n${parseError.message}\n\nPlease return the corrected classification as a valid JSON array. Fix only the JSON formatting issue. Do not change any classification decisions. Return ONLY the JSON array with no other text.`,
          },
        ]
        const correctedRaw = await callModel(primaryModel, correctiveMessages, { temperature: 0 })
        threads = parseAndValidate(correctedRaw)
        modelUsed = `${primaryModel}(corrective-retry)`
        console.log(`[classify] Corrective retry succeeded: ${threads.length} threads`)
      } catch (correctiveError) {
        console.log(`[classify] Corrective retry failed: ${correctiveError.message}`)
      }
    }
  }

  // --- Layer 3: Fallback model ---
  if (!threads) {
    console.log(`[classify] Falling back to ${fallbackModel}`)
    modelUsed = fallbackModel
    try {
      const fallbackRaw = await callModel(fallbackModel, classifyMessages, { temperature: 0.6 })
      threads = parseAndValidate(fallbackRaw)
      console.log(`[classify] Fallback model succeeded: ${threads.length} threads`)
    } catch (fallbackError) {
      console.error(`[classify] All layers exhausted. Primary and fallback both failed.`)
      throw new Error(
        `Classification failed: primary and fallback models both returned no valid JSON. Last error: ${fallbackError.message}`,
      )
    }
  }

  // Wrap in { threads: [...] } for downstream compatibility
  const aiOutput = { threads }

  // Save audit checkpoint
  const auditId = crypto.randomUUID()
  const evaluation = sanitizeString(JSON.stringify(aiOutput).substring(0, 6400))
  try {
    await executeSql(
      apiUrl, jwt, biscuit,
      saveResults.insertAudit(schema, {
        id: auditId, batchId, threadCount: threads.length,
        emailCount: metadataRows.length, cost: 0, inputTokens: 0,
        outputTokens: 0, model: modelUsed, evaluation,
      }),
    )
    console.log(`[classify] audit saved: ${auditId} (model: ${modelUsed})`)
  } catch (err) {
    if (err.message.includes('integrity constraint') || err.message.includes('unique') || err.message.includes('duplicate')) {
      console.log(`[classify] audit already exists for batch (concurrent run), continuing`)
    } else {
      throw err
    }
  }

  console.log(`[classify] ${threads.length} threads ready for processing`)
  return { skipped: false, thread_count: threads.length }
}
