import * as core from '@actions/core'
import { authenticate, executeSql } from '../lib/db.js'
import { sanitizeSchema, scanCompleteEligibility } from '../lib/sql/index.js'
import {
  getGoogleDatastoreAccessToken,
  userHasScanCompleteSentAt,
  rowToScanCompleteWebhookBody,
  getRowUserId,
  postScanCompleteWebhook,
} from '../lib/scan-complete.js'

/**
 * @param {string} raw
 * @param {string} inputName
 * @returns {number}
 */
export function parsePositiveIntegerInput(raw, inputName) {
  const normalized = String(raw ?? '').trim()
  if (normalized === '') {
    throw new Error(`${inputName} must be a positive integer`)
  }

  const parsed = Number.parseInt(normalized, 10)
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`${inputName} must be a positive integer`)
  }
  return parsed
}

/**
 * @param {string} raw
 * @returns {string}
 */
export function normalizeOptionalProjectId(raw) {
  return String(raw ?? '').trim()
}

/**
 * Cron: eligible first LOOKBACK completions → Firestore dedupe → POST /dealsync-v2/webhooks (scan_complete).
 * @see docs/plans/2026-04-16-scan-complete-w3-cron-tech-spec.md
 */
export async function runEmitScanCompleteWebhooks() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const dealsyncSchema = sanitizeSchema(core.getInput('sxt-schema'))
  const emailCoreSchema = sanitizeSchema(core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING')

  const backendBaseUrl = core.getInput('dealsync-backend-base-url')
  const sharedSecret = core.getInput('dealsync-v2-shared-secret')
  const saJsonRaw = core.getInput('firestore-service-account-json')
  const concurrency = parsePositiveIntegerInput(
    core.getInput('scan-complete-webhook-concurrency') || '5',
    'scan-complete-webhook-concurrency',
  )

  if (!authUrl || !authSecret || !apiUrl || !biscuit) {
    throw new Error('sxt-auth-url, sxt-auth-secret, sxt-api-url, and sxt-biscuit are required')
  }
  if (!backendBaseUrl || !sharedSecret || !saJsonRaw) {
    throw new Error(
      'dealsync-backend-base-url, dealsync-v2-shared-secret, and firestore-service-account-json are required',
    )
  }

  let credentials
  try {
    credentials = JSON.parse(saJsonRaw)
  } catch {
    throw new Error('firestore-service-account-json must be valid JSON')
  }
  const firestoreProjectId =
    typeof credentials.project_id === 'string'
      ? normalizeOptionalProjectId(credentials.project_id)
      : ''
  if (!firestoreProjectId) {
    throw new Error('firestore-service-account-json must include a non-empty project_id')
  }

  const sql = scanCompleteEligibility.selectEligibleUsers(emailCoreSchema, dealsyncSchema)
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (q) => executeSql(apiUrl, jwt, biscuit, q)

  console.log('[emit-scan-complete-webhooks] executing eligibility query')
  const result = await exec(sql)
  const rows = Array.isArray(result) ? result : []
  console.log(`[emit-scan-complete-webhooks] eligibility rows=${rows.length}`)

  let firestoreToken = ''
  if (rows.length > 0) {
    firestoreToken = await getGoogleDatastoreAccessToken(credentials)
  }

  let scanned = rows.length
  let skippedDeduped = 0
  let posted = 0
  let errors = 0

  for (let i = 0; i < rows.length; i += concurrency) {
    const chunk = rows.slice(i, i + concurrency)
    await Promise.all(
      chunk.map(async (row) => {
        let userId
        try {
          userId = getRowUserId(row)
        } catch (err) {
          core.error(`[emit-scan-complete-webhooks] skip invalid row: ${err.message}`)
          errors++
          return
        }

        try {
          const alreadySent = await userHasScanCompleteSentAt({
            projectId: firestoreProjectId,
            userId,
            accessToken: firestoreToken,
          })
          if (alreadySent) {
            skippedDeduped++
            console.log(`[emit-scan-complete-webhooks] skip dedupe userId=${userId}`)
            return
          }

          const body = rowToScanCompleteWebhookBody(row)
          const res = await postScanCompleteWebhook(backendBaseUrl, sharedSecret, body)
          if (!res.ok) {
            errors++
            core.error(
              `[emit-scan-complete-webhooks] POST failed userId=${userId} status=${res.status} body=${(res.text || '').slice(0, 500)}`,
            )
            return
          }
          posted++
          console.log(`[emit-scan-complete-webhooks] posted userId=${userId}`)
        } catch (err) {
          errors++
          core.error(`[emit-scan-complete-webhooks] error userId=${userId ?? '?'}: ${err.message}`)
        }
      }),
    )
  }

  const summary = { scanned, skippedDeduped, posted, errors }
  console.log(`[emit-scan-complete-webhooks] done ${JSON.stringify(summary)}`)
  return summary
}
