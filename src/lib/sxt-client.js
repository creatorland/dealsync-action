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
 * Waits until granted. If rate limiter is unavailable, proceeds (fail-open).
 */
async function acquireRateLimitToken() {
  const rateLimiterUrl = core.getInput('rate-limiter-url')
  const apiKey = core.getInput('rate-limiter-api-key')

  if (!rateLimiterUrl || !apiKey) return // No rate limiter configured — skip

  const MAX_ATTEMPTS = 10

  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    try {
      const resp = await fetch(`${rateLimiterUrl}/acquire`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ apiKey, tokens: 1, source: 'dealsync-action' }),
        signal: AbortSignal.timeout(30000),
      })

      if (!resp.ok && resp.status !== 429) {
        // Non-rate-limit error — fail open, proceed with query
        console.log(`[sxt-client] Rate limiter HTTP ${resp.status}, proceeding (fail-open)`)
        return
      }

      const result = await resp.json()
      const data = result.data || result

      if (data.granted) return // Got token, proceed

      // Rate limited — wait and retry
      const waitMs = data.retryAfterMs || 1000
      console.log(`[sxt-client] Rate limited, waiting ${waitMs}ms (attempt ${attempt + 1}/${MAX_ATTEMPTS})`)
      await new Promise((r) => setTimeout(r, waitMs))
    } catch (err) {
      // Rate limiter unreachable — fail open
      console.log(`[sxt-client] Rate limiter error: ${err.message}, proceeding (fail-open)`)
      return
    }
  }

  // Exhausted attempts — fail open
  console.log(`[sxt-client] Rate limiter: max attempts reached, proceeding (fail-open)`)
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
