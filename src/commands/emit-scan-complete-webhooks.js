import * as core from '@actions/core'
import { randomUUID } from 'node:crypto'
import { authenticate, executeSql } from '../lib/db.js'
import { scanCompleteEligibility } from '../lib/sql/index.js'
import {
  makeGoogleDatastoreTokenProvider,
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
  if (!/^[1-9][0-9]*$/.test(normalized)) {
    throw new Error(`${inputName} must be a positive integer`)
  }
  return Number(normalized)
}

/**
 * @param {string} raw
 * @returns {string}
 */
export function normalizeOptionalProjectId(raw) {
  return String(raw ?? '').trim()
}

/**
 * GCP service account JSON: GitHub Action input `firestore-service-account-json`, or env `FIRESTORE_SERVICE_ACCOUNT_JSON`
 * (stringified JSON). Input wins when non-empty so workflows keep explicit precedence.
 * @returns {string}
 */
export function resolveFirestoreServiceAccountJson() {
  const fromInput = String(core.getInput('firestore-service-account-json') ?? '').trim()
  if (fromInput) return fromInput
  const fromEnv = process.env.FIRESTORE_SERVICE_ACCOUNT_JSON
  if (fromEnv !== undefined && String(fromEnv).trim() !== '') {
    return String(fromEnv).trim()
  }
  return ''
}

/**
 * Cron: eligible first LOOKBACK completions → Firestore dedupe → POST /dealsync-v2/webhooks (scan_complete).
 * @see docs/plans/2026-04-16-scan-complete-w3-cron-tech-spec.md
 */
export async function runEmitScanCompleteWebhooks() {
  const cid = randomUUID()
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const sxtSchemaRaw = core.getInput('sxt-schema')
  const emailCoreSchemaRaw = core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING'

  const backendBaseUrl = core.getInput('dealsync-backend-base-url')
  const sharedSecret = core.getInput('dealsync-v2-shared-secret')
  const saJsonRaw = resolveFirestoreServiceAccountJson()
  if (saJsonRaw) {
    core.setSecret(saJsonRaw)
  }
  const concurrency = parsePositiveIntegerInput(
    core.getInput('scan-complete-webhook-concurrency') || '5',
    'scan-complete-webhook-concurrency',
  )
  const batchSize = parsePositiveIntegerInput(
    core.getInput('scan-complete-batch-size') || '500',
    'scan-complete-batch-size',
  )

  if (!authUrl || !authSecret || !apiUrl || !biscuit || !sxtSchemaRaw) {
    throw new Error(
      'sxt-auth-url, sxt-auth-secret, sxt-api-url, sxt-biscuit, and sxt-schema are required',
    )
  }
  if (!backendBaseUrl || !sharedSecret || !saJsonRaw) {
    throw new Error(
      'dealsync-backend-base-url, dealsync-v2-shared-secret, and Firestore service account JSON are required (action input firestore-service-account-json or env FIRESTORE_SERVICE_ACCOUNT_JSON)',
    )
  }

  let credentials
  try {
    credentials = JSON.parse(saJsonRaw)
  } catch {
    throw new Error('Firestore service account JSON must be valid JSON')
  }
  const firestoreProjectId =
    typeof credentials.project_id === 'string'
      ? normalizeOptionalProjectId(credentials.project_id)
      : ''
  if (!firestoreProjectId) {
    throw new Error('Firestore service account JSON must include a non-empty project_id')
  }

  const sql = scanCompleteEligibility.selectEligibleUsers(
    emailCoreSchemaRaw,
    sxtSchemaRaw,
    batchSize,
  )
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (q) => executeSql(apiUrl, jwt, biscuit, q)

  console.log(`[emit-scan-complete-webhooks] cid=${cid} executing eligibility query`)
  const result = await exec(sql)
  const rows = Array.isArray(result) ? result : []
  console.log(`[emit-scan-complete-webhooks] cid=${cid} eligibility rows=${rows.length}`)

  const getFirestoreAccessToken = makeGoogleDatastoreTokenProvider(credentials)
  if (rows.length > 0) {
    await getFirestoreAccessToken()
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
          core.error(`[emit-scan-complete-webhooks] cid=${cid} skip invalid row: ${err.message}`)
          errors++
          return
        }

        try {
          const alreadySent = await userHasScanCompleteSentAt({
            projectId: firestoreProjectId,
            userId,
            getAccessToken: getFirestoreAccessToken,
          })
          if (alreadySent) {
            skippedDeduped++
            console.log(`[emit-scan-complete-webhooks] cid=${cid} skip dedupe userId=${userId}`)
            return
          }

          const body = rowToScanCompleteWebhookBody(row)
          const res = await postScanCompleteWebhook(backendBaseUrl, sharedSecret, body, {
            'x-correlation-id': cid,
          })
          if (!res.ok) {
            errors++
            core.error(
              `[emit-scan-complete-webhooks] cid=${cid} POST failed userId=${userId} status=${res.status} body=${(res.text || '').slice(0, 500)}`,
            )
            return
          }
          posted++
          console.log(`[emit-scan-complete-webhooks] cid=${cid} posted userId=${userId}`)
        } catch (err) {
          errors++
          core.error(
            `[emit-scan-complete-webhooks] cid=${cid} error userId=${userId ?? '?'}: ${err.message}`,
          )
        }
      }),
    )
  }

  const summary = { correlationId: cid, scanned, skippedDeduped, posted, errors }
  console.log(`[emit-scan-complete-webhooks] cid=${cid} done ${JSON.stringify(summary)}`)
  return summary
}
