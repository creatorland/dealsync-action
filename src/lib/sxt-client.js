/**
 * SxT client for dealsync-action.
 *
 * Flow:
 *   acquireRateLimitToken() → wait until granted (no max, fail-open on errors)
 *   authenticate() → get cached token from sxt/auth
 *   executeSql() → call SxT API
 *   401? → authenticate(badJwt) with backoff → retry (max 3)
 *   max retry? → fail
 */

import * as core from '@actions/core'

const SQL_TIMEOUT_MS = 120000
const MAX_AUTH_RETRIES = 3

let cachedJwt = null

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function withTimeout(ms = SQL_TIMEOUT_MS) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), ms)
  return { signal: controller.signal, clear: () => clearTimeout(timeout) }
}

export { withTimeout }

/**
 * Get token from sxt/auth. If badToken provided, backend refreshes.
 */
export async function authenticate(authUrl, authSecret, badToken) {
  const { signal, clear } = withTimeout(30000)
  try {
    const headers = { 'x-shared-secret': authSecret }
    if (badToken) headers['x-bad-token'] = badToken
    const resp = await fetch(authUrl, { method: 'GET', headers, signal })
    if (!resp.ok) throw new Error(`Auth failed: ${resp.status}`)
    const data = await resp.json()
    const jwt = data.data || data.accessToken
    if (!jwt) throw new Error('Auth returned no token')
    cachedJwt = jwt
    return jwt
  } finally {
    clear()
  }
}

/**
 * Acquire rate limit token. Waits until granted (no max).
 * Only network errors consume budget (3 max, then fail-open).
 */
async function acquireRateLimitToken() {
  const rateLimiterUrl = core.getInput('rate-limiter-url')
  const apiKey = core.getInput('rate-limiter-api-key')

  if (!rateLimiterUrl || !apiKey) return

  const MAX_ERRORS = 3
  const OVERALL_TIMEOUT_MS = 5 * 60 * 1000
  const startTime = Date.now()
  let errors = 0

  while (errors < MAX_ERRORS) {
    if (Date.now() - startTime > OVERALL_TIMEOUT_MS) {
      console.log('[sxt-client] Rate limiter: overall timeout, proceeding (fail-open)')
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
        errors++
        const delay = Math.min(1000 * Math.pow(2, errors), 30000)
        console.log(`[sxt-client] Rate limiter HTTP ${resp.status}, error ${errors}/${MAX_ERRORS}, retrying in ${delay}ms`)
        await sleep(delay)
        continue
      }

      const result = await resp.json()
      const data = result.data || result
      if (data.granted) return

      const waitMs = data.retryAfterMs || 1000
      console.log(`[sxt-client] Rate limited, waiting ${waitMs}ms`)
      await sleep(waitMs)
    } catch (err) {
      errors++
      const delay = Math.min(1000 * Math.pow(2, errors), 30000)
      console.log(`[sxt-client] Rate limiter error: ${err.message}, error ${errors}/${MAX_ERRORS}, retrying in ${delay}ms`)
      await sleep(delay)
    }
  }

  console.log('[sxt-client] Rate limiter: error budget exhausted, proceeding (fail-open)')
}

/**
 * Execute SQL against SxT.
 *
 * 1. Acquire rate limit token (waits, no max)
 * 2. Call SQL with cached JWT
 * 3. On 401 → authenticate(badJwt) with backoff → retry (max 3)
 * 4. Max retry → fail
 */
export async function executeSql(apiUrl, jwt, biscuit, sql) {
  await acquireRateLimitToken()

  // Use provided jwt or cached
  let currentJwt = jwt || cachedJwt
  if (!currentJwt) throw new Error('No JWT available — call authenticate() first')

  for (let attempt = 0; attempt <= MAX_AUTH_RETRIES; attempt++) {
    const { signal, clear } = withTimeout()
    try {
      const resp = await fetch(`${apiUrl}/v1/sql`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${currentJwt}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sqlText: sql, biscuits: [biscuit] }),
        signal,
      })

      if (resp.status === 401) {
        clear()
        if (attempt >= MAX_AUTH_RETRIES) {
          throw new Error(`SxT 401 after ${MAX_AUTH_RETRIES} auth retries`)
        }

        const delay = Math.min(2000 * Math.pow(2, attempt), 10000)
        console.log(`[sxt-client] 401 received (attempt ${attempt + 1}/${MAX_AUTH_RETRIES}), re-authenticating, backoff ${delay}ms`)

        const authUrl = core.getInput('auth-url')
        const authSecret = core.getInput('auth-secret')
        currentJwt = await authenticate(authUrl, authSecret, currentJwt)
        await sleep(delay)
        continue
      }

      if (!resp.ok) throw new Error(`SxT ${resp.status}: ${await resp.text()}`)
      return resp.json()
    } finally {
      clear()
    }
  }

  throw new Error('SxT query failed after all retries')
}
