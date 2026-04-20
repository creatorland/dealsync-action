/**
 * scan_complete cron helpers: Firestore dedupe (read-only), backend webhook POST, SxT row → DTO.
 * @see backend/src/dtos/dealsync-v2.webhooks.dto.ts
 */

import * as core from '@actions/core'
import { createPrivateKey, createSign } from 'node:crypto'
import { sleep, backoffMs } from './retry.js'

const TRANSIENT_MAX_ATTEMPTS = 3
const RETRY_AFTER_CAP_MS = 30_000

const OAUTH_TOKEN_URL = 'https://oauth2.googleapis.com/token'
const FIRESTORE_SCOPE = 'https://www.googleapis.com/auth/datastore'

/**
 * Retryable HTTP statuses: 408 (Request Timeout), 429 (Too Many Requests), 5xx.
 * 4xx other than 408/429 are client errors and should not be retried.
 * @param {number} status
 */
function isRetryableStatus(status) {
  return status === 408 || status === 429 || status >= 500
}

/**
 * Honor RFC 7231 Retry-After if present (delta-seconds or HTTP-date), otherwise fall back to
 * jittered exponential backoff. Capped at {@link RETRY_AFTER_CAP_MS} to bound tick duration.
 * @param {Response} resp
 * @param {number} attempt
 */
function retryAfterMs(resp, attempt) {
  const header = resp?.headers?.get?.('retry-after')
  if (header) {
    const asNumber = Number(header)
    if (Number.isFinite(asNumber) && asNumber >= 0) {
      return Math.min(asNumber * 1000, RETRY_AFTER_CAP_MS)
    }
    const asDate = Date.parse(header)
    if (Number.isFinite(asDate)) {
      return Math.max(0, Math.min(asDate - Date.now(), RETRY_AFTER_CAP_MS))
    }
  }
  return backoffMs(attempt, { base: 500, max: 4000, jitter: true })
}

function base64UrlJson(obj) {
  return Buffer.from(JSON.stringify(obj), 'utf8')
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
}

function base64UrlBuffer(buf) {
  return Buffer.from(buf)
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
}

/**
 * @param {{ client_email: string, private_key: string }} credentials — service account JSON fields
 * @returns {Promise<string>} access_token
 */
export async function getGoogleDatastoreAccessToken(credentials) {
  const { client_email: iss, private_key: privateKeyPem } = credentials
  if (!iss || !privateKeyPem)
    throw new Error('Service account JSON missing client_email or private_key')

  const now = Math.floor(Date.now() / 1000)
  const header = { alg: 'RS256', typ: 'JWT' }
  const payload = {
    iss,
    sub: iss,
    aud: OAUTH_TOKEN_URL,
    iat: now,
    exp: now + 3600,
    scope: FIRESTORE_SCOPE,
  }
  const unsigned = `${base64UrlJson(header)}.${base64UrlJson(payload)}`
  const sign = createSign('RSA-SHA256')
  sign.update(unsigned)
  sign.end()
  const signature = base64UrlBuffer(sign.sign(createPrivateKey(privateKeyPem)))

  const jwt = `${unsigned}.${signature}`
  const resp = await fetch(OAUTH_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt,
    }),
    signal: AbortSignal.timeout(30000),
  })
  const body = await resp.json().catch(() => ({}))
  if (!resp.ok) {
    throw new Error(`OAuth token ${resp.status}: ${JSON.stringify(body)}`)
  }
  const token = body.access_token
  if (!token || typeof token !== 'string') throw new Error('OAuth response missing access_token')
  core.setSecret(token)
  return token
}

/**
 * Memoizing token provider. Refreshes ~5 min before the 1h OAuth expiry so a long cron run
 * (or a burst of retries) never fails on a stale token.
 * @param {{ client_email: string, private_key: string }} credentials
 * @returns {() => Promise<string>}
 */
export function makeGoogleDatastoreTokenProvider(credentials) {
  let token = ''
  let expiresAt = 0
  return async () => {
    if (token && Date.now() < expiresAt - 300_000) return token
    token = await getGoogleDatastoreAccessToken(credentials)
    expiresAt = Date.now() + 3600_000
    return token
  }
}

/**
 * Firestore REST encodes int64 as string; only treat as set when it parses as an integer.
 * @param {unknown} raw
 */
function isValidFirestoreIntegerString(raw) {
  if (raw == null || raw === '') return false
  const s = String(raw).trim()
  return s !== '' && /^-?\d+$/.test(s)
}

/**
 * @param {unknown} doc — Firestore REST GET document JSON
 */
export function firestoreDocumentHasScanCompleteSentAt(doc) {
  const field = doc?.fields?.scanCompleteSentAt
  if (!field || typeof field !== 'object') return false
  if (field.integerValue != null && field.integerValue !== '') {
    return isValidFirestoreIntegerString(field.integerValue)
  }
  if (field.doubleValue != null && Number.isFinite(Number(field.doubleValue))) return true
  return false
}

/**
 * @param {{ projectId: string, userId: string, getAccessToken: () => Promise<string> }} args
 */
export async function userHasScanCompleteSentAt({ projectId, userId, getAccessToken }) {
  const path = `projects/${encodeURIComponent(projectId)}/databases/(default)/documents/users/${encodeURIComponent(userId)}`
  const url = new URL(`https://firestore.googleapis.com/v1/${path}`)
  url.searchParams.set('mask.fieldPaths', 'scanCompleteSentAt')

  let lastErr
  for (let attempt = 0; attempt < TRANSIENT_MAX_ATTEMPTS; attempt++) {
    let waitMs = backoffMs(attempt, { base: 500, max: 4000, jitter: true })
    try {
      const accessToken = await getAccessToken()
      const resp = await fetch(url.toString(), {
        method: 'GET',
        headers: { Authorization: `Bearer ${accessToken}` },
        signal: AbortSignal.timeout(30000),
      })

      if (resp.status === 404) return false
      if (resp.ok) {
        const doc = await resp.json()
        return firestoreDocumentHasScanCompleteSentAt(doc)
      }
      if (isRetryableStatus(resp.status) && attempt < TRANSIENT_MAX_ATTEMPTS - 1) {
        await resp.body?.cancel().catch(() => {})
        lastErr = new Error(`Firestore GET ${resp.status} (retrying)`)
        waitMs = retryAfterMs(resp, attempt)
      } else {
        const text = await resp.text()
        throw new Error(`Firestore GET ${resp.status}: ${text}`)
      }
    } catch (err) {
      lastErr = err
      // Terminal status error (has "Firestore GET <code>: <body>") or final attempt → stop.
      // Plain network/abort errors fall through to retry.
      if (attempt >= TRANSIENT_MAX_ATTEMPTS - 1 || /Firestore GET \d+:/.test(err.message)) {
        throw err
      }
    }
    await sleep(waitMs)
  }
  throw lastErr ?? new Error('Firestore GET failed after retries')
}

/**
 * Coerce a single cell value from an SxT row to a finite number (else 0).
 * @param {unknown} v
 */
export function coerceNumber(v) {
  if (v === null || v === undefined) return 0
  const n = Number(v)
  return Number.isFinite(n) ? n : 0
}

/** @param {Record<string, unknown>} row */
function col(row, upper) {
  if (row[upper] !== undefined && row[upper] !== null) return row[upper]
  const lower = upper.toLowerCase()
  if (row[lower] !== undefined && row[lower] !== null) return row[lower]
  return undefined
}

/**
 * @param {Record<string, unknown>} row
 * @returns {string}
 */
export function getRowUserId(row) {
  const v = col(row, 'USER_ID')
  if (v === undefined || v === null || String(v).trim() === '') {
    throw new Error('Eligible row missing USER_ID')
  }
  return String(v)
}

/**
 * @param {Record<string, unknown>} row
 */
export function rowToScanCompleteWebhookBody(row) {
  const userId = getRowUserId(row)
  return {
    userId,
    eventType: 'scan_complete',
    eventData: {
      dealCounts: {
        new: coerceNumber(col(row, 'DB_NEW')),
        inProgress: coerceNumber(col(row, 'DB_IN_PROGRESS')),
        completed: coerceNumber(col(row, 'DB_COMPLETED')),
        likelyScam: coerceNumber(col(row, 'DB_LIKELY_SCAM')),
        lowConfidence: coerceNumber(col(row, 'DB_LOW_CONFIDENCE')),
        notInterested: coerceNumber(col(row, 'DB_NOT_INTERESTED')),
      },
      contactsAdded: coerceNumber(col(row, 'CONTACTS_ADDED')),
    },
  }
}

/**
 * @param {string} baseUrl
 * @param {string} sharedSecret
 * @param {{ userId: string, eventType: string, eventData: object }} body
 */
export async function postScanCompleteWebhook(baseUrl, sharedSecret, body, headers = {}) {
  const root = baseUrl.replace(/\/+$/, '')
  const url = `${root}/dealsync-v2/webhooks`
  const payload = JSON.stringify(body)
  let last = { ok: false, status: 0, text: '' }
  for (let attempt = 0; attempt < TRANSIENT_MAX_ATTEMPTS; attempt++) {
    let waitMs = backoffMs(attempt, { base: 500, max: 4000, jitter: true })
    try {
      const resp = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-shared-secret': sharedSecret,
          ...headers,
        },
        body: payload,
        signal: AbortSignal.timeout(120000),
      })
      if (resp.ok) {
        await resp.body?.cancel().catch(() => {})
        return { ok: true, status: resp.status, text: '' }
      }
      const text = await resp.text()
      last = { ok: false, status: resp.status, text }
      // Retry 408/429/5xx; all other 4xx are client errors and terminal.
      if (!isRetryableStatus(resp.status) || attempt >= TRANSIENT_MAX_ATTEMPTS - 1) return last
      waitMs = retryAfterMs(resp, attempt)
    } catch (err) {
      last = { ok: false, status: 0, text: String(err?.message ?? err) }
      if (attempt >= TRANSIENT_MAX_ATTEMPTS - 1) return last
    }
    await sleep(waitMs)
  }
  return last
}
