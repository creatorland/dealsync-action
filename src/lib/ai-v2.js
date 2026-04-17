import * as core from '@actions/core'
import { getHeader, sanitizeEmailBody } from './emails.js'
import { sleep, backoffMs } from './retry.js'
import systemTemplate from '../../prompts/system.md'
import systemTemplateLlama from '../../prompts/system-llama.md'
import classificationInstructions from '../../prompts/user.md'

// --- Prompt building (unchanged from ai.js) ---

function groupByThread(emails) {
  const threads = {}
  for (const email of emails) {
    const threadId = email.threadId || email.id
    if (!threads[threadId]) threads[threadId] = []
    threads[threadId].push(email)
  }
  return threads
}

function buildThreadData(emails) {
  const threads = groupByThread(emails)
  const parts = []
  const threadOrder = []
  let threadIndex = 0

  for (const [threadId, threadEmails] of Object.entries(threads)) {
    threadIndex++
    threadOrder.push(threadId)
    let section = `THREAD_ID_INDEX: ${threadIndex}\n`
    section += `MODE: FULL_THREAD\n`
    section += `Message Count: ${threadEmails.length}\n`

    const previousSummary = threadEmails[0].previousAiSummary
    section += `PREVIOUS_AI_SUMMARY: ${previousSummary || 'None'}\n\n`

    threadEmails.forEach((email, i) => {
      const from = getHeader(email, 'from')
      const date = getHeader(email, 'date')
      const subject = getHeader(email, 'subject')
      section += `[Message ${i + 1}]\n`
      section += `From: ${from}\n`
      section += `Date: ${date}\n`
      section += `Subject: ${subject}\n\n`
      const rawBody = email.body || email.replyBody || ''
      const body = sanitizeEmailBody(rawBody) || '[no body]'
      section += `${body}\n\n`
      if (rawBody.length > 5000) {
        console.log(
          `[ai-prompt] thread=${threadId} msg=${i + 1}: rawBody=${rawBody.length} chars → sanitized=${body.length} chars`,
        )
      }
    })

    section += '===\n'
    parts.push(section)
  }

  return { text: parts.join('\n'), threadOrder }
}

export function buildPrompt(emails, { systemOverride, userOverride, creatorEmail, model } = {}) {
  const { text: threadData, threadOrder } = buildThreadData(emails)

  const isLlama = model && model.toLowerCase().includes('llama')
  const defaultTemplate = isLlama ? systemTemplateLlama : systemTemplate
  const systemPrompt = (systemOverride || defaultTemplate).trim()

  const creatorLine = creatorEmail ? `Creator email: ${creatorEmail}\n\n` : ''

  const userPrompt = (userOverride || classificationInstructions)
    .replace('{{THREAD_DATA}}', creatorLine + threadData)
    .trim()

  console.log(
    `[ai-prompt] ${emails.length} emails, ${threadOrder.length} threads, ` +
      `system=${systemPrompt.length} chars, user=${userPrompt.length} chars, ` +
      `threadData=${threadData.length} chars`,
  )

  return { systemPrompt, userPrompt, threadOrder }
}

// --- Constants ---
export const AI_REQUEST_TIMEOUT_MS = 240000
export const AI_RETRY_DELAY_MS = 2000
export const MAX_HTTP_RETRIES = parseInt(core.getInput('ai-max-retries') || '3', 10)
export const MAX_TOKENS = 20480

// --- Valid categories and deal types ---
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

// --- JSON Schema for structured output ---
const CLASSIFICATION_SCHEMA = {
  name: 'deal_classifications',
  strict: true,
  schema: {
    type: 'object',
    properties: {
      results: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            thread_index: { type: 'integer' },
            is_deal: { type: 'boolean' },
            is_english: { type: 'boolean' },
            language: { type: ['string', 'null'] },
            ai_score: { type: 'integer' },
            category: { type: ['string', 'null'] },
            likely_scam: { type: 'boolean' },
            ai_insight: { type: 'string' },
            ai_summary: { type: 'string' },
            main_contact: { type: ['string', 'null'] },
            deal_brand: { type: ['string', 'null'] },
            deal_type: { type: ['string', 'null'] },
            deal_name: { type: ['string', 'null'] },
            deal_value: { type: ['number', 'null'] },
            deal_currency: { type: ['string', 'null'] },
          },
          required: [
            'thread_index',
            'is_deal',
            'is_english',
            'ai_score',
            'category',
            'likely_scam',
            'ai_insight',
            'ai_summary',
          ],
          additionalProperties: false,
        },
      },
    },
    required: ['results'],
    additionalProperties: false,
  },
}

// --- AI client with json_schema ---

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
          response_format: { type: 'json_schema', json_schema: CLASSIFICATION_SCHEMA },
        }),
        signal: AbortSignal.timeout(AI_REQUEST_TIMEOUT_MS),
      })

      if (!resp.ok) {
        const errBody = await resp.text().catch(() => '')
        lastError = new Error(`HTTP ${resp.status}: ${errBody.substring(0, 500)}`)
        console.log(`[ai-client] ${model} HTTP ${resp.status}: ${errBody.substring(0, 500)}`)

        if (resp.status === 429 && rateLimitWaits < MAX_RATE_LIMIT_WAITS) {
          rateLimitWaits++
          const retryAfter = parseInt(resp.headers.get('retry-after') || '5', 10) * 1000
          console.log(
            `[ai-client] ${model} rate limited (${rateLimitWaits}/${MAX_RATE_LIMIT_WAITS}), waiting ${retryAfter}ms`,
          )
          await sleep(retryAfter)
          attempt--
          continue
        }

        if (attempt < MAX_HTTP_RETRIES) {
          await sleep(backoffMs(attempt - 1, { base: AI_RETRY_DELAY_MS }))
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
        const delay = backoffMs(attempt - 1, { base: AI_RETRY_DELAY_MS })
        console.log(
          `[ai-client] ${model} attempt ${attempt} failed: ${err.message}, retrying in ${delay}ms`,
        )
        await sleep(delay)
      }
    }
  }
  throw lastError || new Error(`${model} failed after ${MAX_HTTP_RETRIES} attempts`)
}

/**
 * Parse and validate structured JSON schema output.
 * Expects { results: [...] } from json_schema constrained decoding.
 * Only does schema coercion — no repair logic needed.
 */
export function parseAndValidate(raw, threadOrder) {
  const parsed = JSON.parse(raw.trim())
  const results = parsed.results || parsed

  const items = Array.isArray(results) ? results : [results]

  return items.map((r) => ({
    thread_id:
      threadOrder && r.thread_index != null
        ? threadOrder[Math.max(0, Number(r.thread_index) - 1)] || String(r.thread_id || '')
        : String(r.thread_id || ''),
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
