/**
 * scan_complete cron helpers: Firestore dedupe (read-only), backend webhook POST, SxT row → DTO.
 * @see backend/src/dtos/dealsync-v2.webhooks.dto.ts
 */

import { createPrivateKey, createSign } from 'node:crypto'

const OAUTH_TOKEN_URL = 'https://oauth2.googleapis.com/token'
const FIRESTORE_SCOPE = 'https://www.googleapis.com/auth/datastore'

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
  return token
}

/**
 * @param {unknown} doc — Firestore REST GET document JSON
 */
export function firestoreDocumentHasScanCompleteSentAt(doc) {
  const field = doc?.fields?.scanCompleteSentAt
  if (!field || typeof field !== 'object') return false
  if (field.integerValue != null && field.integerValue !== '') return true
  if (field.doubleValue != null && Number.isFinite(Number(field.doubleValue))) return true
  return false
}

/**
 * @param {{ projectId: string, userId: string, accessToken: string }} args
 */
export async function userHasScanCompleteSentAt({ projectId, userId, accessToken }) {
  const path = `projects/${encodeURIComponent(projectId)}/databases/(default)/documents/users/${encodeURIComponent(userId)}`
  const url = new URL(`https://firestore.googleapis.com/v1/${path}`)
  url.searchParams.set('mask.fieldPaths', 'scanCompleteSentAt')

  const resp = await fetch(url.toString(), {
    method: 'GET',
    headers: { Authorization: `Bearer ${accessToken}` },
    signal: AbortSignal.timeout(30000),
  })

  if (resp.status === 404) return false
  if (!resp.ok) {
    const text = await resp.text()
    throw new Error(`Firestore GET ${resp.status}: ${text}`)
  }
  const doc = await resp.json()
  return firestoreDocumentHasScanCompleteSentAt(doc)
}

/**
 * @param {Record<string, unknown>} row — SxT row (UPPERCASE keys)
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
export async function postScanCompleteWebhook(baseUrl, sharedSecret, body) {
  const root = baseUrl.replace(/\/+$/, '')
  const url = `${root}/dealsync-v2/webhooks`
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-shared-secret': sharedSecret,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(120000),
  })
  const text = await resp.text()
  if (!resp.ok) return { ok: false, status: resp.status, text }
  return { ok: true, status: resp.status, text }
}
