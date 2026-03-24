/**
 * Shared SxT helpers for all commands.
 * Auth via proxy, static biscuit from input.
 * Rate limiter integration: acquires token before each SQL call.
 *
 * All fetch calls have a 2-minute timeout by default.
 */

import * as core from '@actions/core'

const DEFAULT_TIMEOUT_MS = 120000 // 2 minutes

function withTimeout(ms = DEFAULT_TIMEOUT_MS) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), ms)
  return { signal: controller.signal, clear: () => clearTimeout(timeout) }
}

export { withTimeout }

export async function authenticate(authUrl, authSecret) {
  const { signal, clear } = withTimeout()
  try {
    const resp = await fetch(authUrl, {
      method: 'GET',
      headers: { 'x-shared-secret': authSecret },
      signal,
    })
    if (!resp.ok) throw new Error(`Auth failed: ${resp.status}`)
    const data = await resp.json()
    return data.data || data.accessToken || data
  } finally {
    clear()
  }
}

/**
 * Acquire a rate limit token before executing a query.
 * Rate limit denials (granted: false) keep retrying indefinitely — they don't consume error budget.
 * Only actual errors (network failures, non-429 HTTP errors) consume the error budget.
 * Fail-open: if rate limiter is unavailable after error retries, proceed with query.
 */
async function acquireRateLimitToken() {
  const rateLimiterUrl = core.getInput('rate-limiter-url')
  const apiKey = core.getInput('rate-limiter-api-key')

  if (!rateLimiterUrl || !apiKey) return // No rate limiter configured — skip

  const MAX_ERROR_RETRIES = 3
  const OVERALL_TIMEOUT_MS = 5 * 60 * 1000 // 5 min safety valve
  const startTime = Date.now()
  let errorAttempts = 0

  while (errorAttempts < MAX_ERROR_RETRIES) {
    if (Date.now() - startTime > OVERALL_TIMEOUT_MS) {
      console.log(`[sxt-client] Rate limiter: overall timeout exceeded, proceeding (fail-open)`)
      return
    }

    try {
      const resp = await fetch(`${rateLimiterUrl}/acquire`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ apiKey, tokens: 1, source: 'dealsync-action' }),
        signal: AbortSignal.timeout(30000),
      })

      if (!resp.ok && resp.status !== 429) {
        // Non-rate-limit HTTP error — consumes error budget
        errorAttempts++
        const delay = Math.min(1000 * Math.pow(2, errorAttempts), 30000)
        console.log(`[sxt-client] Rate limiter HTTP ${resp.status}, error ${errorAttempts}/${MAX_ERROR_RETRIES}, retrying in ${delay}ms`)
        await new Promise((r) => setTimeout(r, delay))
        continue
      }

      const result = await resp.json()
      const data = result.data || result

      if (data.granted) return // Got token, proceed

      // Rate limited — wait and retry (does NOT consume error budget)
      const waitMs = data.retryAfterMs || 1000
      console.log(`[sxt-client] Rate limited, waiting ${waitMs}ms`)
      await new Promise((r) => setTimeout(r, waitMs))
    } catch (err) {
      // Network error — consumes error budget
      errorAttempts++
      const delay = Math.min(1000 * Math.pow(2, errorAttempts), 30000)
      console.log(`[sxt-client] Rate limiter error: ${err.message}, error ${errorAttempts}/${MAX_ERROR_RETRIES}, retrying in ${delay}ms`)
      await new Promise((r) => setTimeout(r, delay))
    }
  }

  // Error budget exhausted — fail open
  console.log(`[sxt-client] Rate limiter: error retries exhausted, proceeding (fail-open)`)
}

export async function executeSql(apiUrl, jwt, biscuit, sql) {
  // Acquire rate limit token before query (fail-open if unavailable)
  await acquireRateLimitToken()

  const { signal, clear } = withTimeout()
  try {
    const resp = await fetch(`${apiUrl}/v1/sql`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${jwt}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ sqlText: sql, biscuits: [biscuit] }),
      signal,
    })
    if (!resp.ok) throw new Error(`SxT ${resp.status}: ${await resp.text()}`)
    return resp.json()
  } finally {
    clear()
  }
}
