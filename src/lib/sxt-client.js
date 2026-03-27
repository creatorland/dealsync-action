/**
 * SxT client for dealsync-action.
 *
 * Flow:
 *   acquireRateLimitToken() → wait until granted (no max, fail-open on errors)
 *   authenticate() → get cached token from sxt/auth (retry with backoff)
 *   executeSql() → call SxT API (retry with backoff)
 *   401? → authenticate(badJwt) with backoff → retry
 *   max retry? → fail
 *
 * Every path has exponential backoff.
 */

import * as core from '@actions/core'

const SQL_TIMEOUT_MS = 120000
const AUTH_TIMEOUT_MS = 30000
const MAX_RETRIES = 6

let cachedJwt = null
let reauthPromise = null

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function backoff(attempt, base = 2000, max = 10000) {
  const delay = Math.min(base * Math.pow(2, attempt), max)
  const jitter = Math.random() * delay * 0.5
  return Math.round(delay + jitter)
}

function withTimeout(ms = SQL_TIMEOUT_MS) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), ms)
  return { signal: controller.signal, clear: () => clearTimeout(timeout) }
}

export { withTimeout }

/**
 * Get token from sxt/auth with retry + backoff.
 * If badToken provided, backend refreshes.
 */
export async function authenticate(authUrl, authSecret, badToken) {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const headers = { 'x-shared-secret': authSecret }
      if (badToken) headers['x-bad-token'] = badToken
      const { signal, clear } = withTimeout(AUTH_TIMEOUT_MS)
      try {
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
    } catch (err) {
      if (attempt < MAX_RETRIES - 1) {
        const delay = backoff(attempt)
        console.log(`[sxt-client] Auth failed (attempt ${attempt + 1}/${MAX_RETRIES}): ${err.message}, retrying in ${delay}ms`)
        await sleep(delay)
      } else {
        throw err
      }
    }
  }
  throw new Error('Auth failed after all retries')
}

/**
 * Acquire rate limit tokens. Waits until granted (no max).
 * Only network errors consume budget (3 max, then fail-open).
 * @param {number} tokens — number of tokens to acquire (default 1)
 */
export async function acquireRateLimitToken(tokens = 1) {
  const rateLimiterUrl = core.getInput('rate-limiter-url')
  const apiKey = core.getInput('rate-limiter-api-key')

  if (!rateLimiterUrl || !apiKey) return

  const MAX_ERRORS = 3
  const OVERALL_TIMEOUT_MS = 5 * 60 * 1000
  const startTime = Date.now()
  let errors = 0
  let denials = 0
  let waiting = false

  while (errors < MAX_ERRORS) {
    if (Date.now() - startTime > OVERALL_TIMEOUT_MS) {
      console.log('[sxt-client] Rate limiter: overall timeout, proceeding (fail-open)')
      return
    }

    try {
      const resp = await fetch(`${rateLimiterUrl}/acquire`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ apiKey, tokens, source: 'dealsync-action' }),
        signal: AbortSignal.timeout(AUTH_TIMEOUT_MS),
      })

      if (!resp.ok && resp.status !== 429) {
        errors++
        const delay = backoff(errors)
        console.log(`[sxt-client] Rate limiter HTTP ${resp.status}, error ${errors}/${MAX_ERRORS}, retrying in ${delay}ms`)
        await sleep(delay)
        continue
      }

      const result = await resp.json()
      const data = result.data || result
      if (data.granted) return

      denials++
      if (!waiting) {
        console.log('[sxt-client] Waiting for rate limit token...')
        waiting = true
      }
      const waitMs = data.retryAfterMs || backoff(denials)
      await sleep(waitMs)
    } catch (err) {
      errors++
      const delay = backoff(errors)
      console.log(`[sxt-client] Rate limiter error: ${err.message}, error ${errors}/${MAX_ERRORS}, retrying in ${delay}ms`)
      await sleep(delay)
    }
  }

  console.log('[sxt-client] Rate limiter: error budget exhausted, proceeding (fail-open)')
}

/**
 * Deduplicated re-authentication. Only re-auths if the bad token matches
 * the current cachedJwt (meaning no one else has refreshed it yet).
 * If cachedJwt is already different, the token was already refreshed — skip.
 * If another worker is already re-authing, wait for that result.
 */
async function reauthenticate(badToken) {
  // Already refreshed by another worker — skip
  if (cachedJwt && cachedJwt !== badToken) return cachedJwt
  // Another worker is already re-authing — wait for it
  if (reauthPromise) return reauthPromise

  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  reauthPromise = authenticate(authUrl, authSecret, badToken).finally(() => {
    reauthPromise = null
  })
  return reauthPromise
}

/**
 * Execute SQL against SxT. Every path retries with backoff.
 *
 * 1. Acquire rate limit token (unless skipRateLimit)
 * 2. Call SQL with cached JWT (retry with backoff on network error)
 * 3. On 401 → authenticate(badJwt) with backoff → retry
 * 4. Max retries → fail
 *
 * @param {string} apiUrl
 * @param {string} jwt
 * @param {string} biscuit
 * @param {string} sql
 * @param {{ skipRateLimit?: boolean }} opts
 */
export async function executeSql(apiUrl, jwt, biscuit, sql, { skipRateLimit = false } = {}) {
  if (!skipRateLimit) await acquireRateLimitToken()

  // Always prefer cachedJwt (refreshed on 401), fall back to passed jwt
  if (!cachedJwt && jwt) cachedJwt = jwt
  if (!cachedJwt) throw new Error('No JWT available — call authenticate() first')

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    const { signal, clear } = withTimeout()
    try {
      const resp = await fetch(`${apiUrl}/v1/sql`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${cachedJwt}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sqlText: sql, biscuits: [biscuit] }),
        signal,
      })

      if (resp.status === 401) {
        clear()
        const delay = backoff(attempt)
        console.log(`[sxt-client] 401 received (attempt ${attempt + 1}/${MAX_RETRIES}), re-authenticating, backoff ${delay}ms`)
        await reauthenticate(cachedJwt)
        await sleep(delay)
        continue
      }

      if (!resp.ok) {
        const body = await resp.text()
        console.error(`[sxt-client] SxT ${resp.status} error. SQL: ${sql.substring(0, 500)}`)
        throw new Error(`SxT ${resp.status}: ${body}`)
      }
      return resp.json()
    } catch (err) {
      clear()
      if (err.message.startsWith('SxT ')) throw err // Non-retryable SxT error

      if (attempt < MAX_RETRIES - 1) {
        const delay = backoff(attempt)
        console.log(`[sxt-client] SQL query failed (attempt ${attempt + 1}/${MAX_RETRIES}): ${err.message}, retrying in ${delay}ms`)
        await sleep(delay)
      } else {
        throw err
      }
    }
  }

  throw new Error('SxT query failed after all retries')
}
