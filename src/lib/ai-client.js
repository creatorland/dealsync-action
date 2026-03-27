// --- Constants ---
export const AI_REQUEST_TIMEOUT_MS = 240000
export const AI_RETRY_DELAY_MS = 2000
export const AI_BACKOFF_MULTIPLIER = 2
export const MAX_HTTP_RETRIES = 3
export const MAX_TOKENS = 20480

// --- Valid categories and deal types for validation ---
export const VALID_CATEGORIES = new Set([
  'new',
  'in_progress',
  'completed',
  'not_interested',
  'likely_scam',
  'low_confidence',
])
export const VALID_DEAL_TYPES = new Set([
  'brand_collaboration',
  'sponsorship',
  'affiliate',
  'product_seeding',
  'ambassador',
  'content_partnership',
  'paid_placement',
  'other_business',
])

/**
 * Call a model with HTTP retries + exponential backoff.
 * Returns { content, usage } or throws on total failure.
 *
 * @param {string} model - Model ID
 * @param {Array} messages - Chat messages
 * @param {Object} opts - { temperature, apiUrl, apiKey }
 */
const MAX_RATE_LIMIT_WAITS = 10

export async function callModel(model, messages, { temperature = 0, apiUrl, apiKey } = {}) {
  let lastError
  let rateLimitWaits = 0
  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt++) {
    try {
      console.log(`[ai-client] ${model} (attempt ${attempt}/${MAX_HTTP_RETRIES})`)
      const resp = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
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
        console.log(`[ai-client] ${model} HTTP ${resp.status}: ${errBody.substring(0, 200)}`)

        // 429: respect Retry-After or wait 5s, don't consume retry budget
        if (resp.status === 429 && rateLimitWaits < MAX_RATE_LIMIT_WAITS) {
          rateLimitWaits++
          const retryAfter = parseInt(resp.headers.get('retry-after') || '5', 10) * 1000
          console.log(
            `[ai-client] ${model} rate limited (${rateLimitWaits}/${MAX_RATE_LIMIT_WAITS}), waiting ${retryAfter}ms`,
          )
          await new Promise((r) => setTimeout(r, retryAfter))
          attempt-- // don't consume attempt
          continue
        }

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

      const usage = result.usage || {}
      return { content, usage }
    } catch (err) {
      lastError = err
      if (attempt < MAX_HTTP_RETRIES) {
        const delay = AI_RETRY_DELAY_MS * Math.pow(AI_BACKOFF_MULTIPLIER, attempt - 1)
        console.log(
          `[ai-client] ${model} attempt ${attempt} failed: ${err.message}, retrying in ${delay}ms`,
        )
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
export function parseAndValidate(raw) {
  let content = raw.trim()

  // Strip markdown fences
  content = content
    .replace(/^```(?:json)?\s*\n?/gi, '')
    .replace(/\n?```\s*$/gi, '')
    .trim()

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
    category: r.is_deal ? (VALID_CATEGORIES.has(r.category) ? r.category : 'low_confidence') : null,
    likely_scam: Boolean(r.likely_scam) || r.category === 'likely_scam',
    ai_insight: String(r.ai_insight || ''),
    ai_summary: String(r.ai_summary || '').slice(0, 1000),
    main_contact: r.is_deal ? r.main_contact || null : null,
    deal_brand: r.is_deal ? r.deal_brand || null : null,
    deal_type: r.is_deal
      ? VALID_DEAL_TYPES.has(r.deal_type)
        ? r.deal_type
        : 'other_business'
      : null,
    deal_name: r.is_deal ? r.deal_name || null : null,
    deal_value: r.deal_value != null ? Number(r.deal_value) : null,
    deal_currency: r.deal_currency || null,
  }))
}
