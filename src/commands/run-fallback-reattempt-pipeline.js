import * as core from '@actions/core'
import { randomUUID } from 'node:crypto'
import { authenticate, executeSql } from '../lib/db.js'
import { parsePositiveIntegerInput } from '../lib/inputs.js'
import { fallbackReattemptEligibility } from '../lib/sql/index.js'
import { UEI_LOOKBACK_DAYS_FALLBACK } from '../lib/uei-lookback.js'

export { parsePositiveIntegerInput }

/**
 * @param {string} backendBaseUrl
 * @returns {string} normalized base URL with trailing slash trimmed
 */
function normalizeBaseUrl(backendBaseUrl) {
  return String(backendBaseUrl ?? '').replace(/\/+$/, '')
}

/**
 * POST one row to backend's ingestion-trigger route with the §A1 fallback
 * re-attempt override params. Returns `{ ok, status, text? }` (where `text`
 * is the raw response body on non-2xx, non-409 responses). Treats 409
 * (already-in-progress) as success — backend has decided the row already has
 * a successor in flight, which is what we wanted to ensure anyway.
 *
 * @param {string} backendBaseUrl
 * @param {string} sharedSecret
 * @param {{userId: string, originatingSyncStateId: string}} payload
 * @param {{[key: string]: string}} [extraHeaders]
 * @returns {Promise<{ok: boolean, status: number, text?: string}>}
 */
export async function postFallbackReattempt(
  backendBaseUrl,
  sharedSecret,
  payload,
  extraHeaders = {},
) {
  const url = `${normalizeBaseUrl(backendBaseUrl)}/v1/dealsync-v2/sync/ingestion-trigger`
  const body = {
    userId: payload.userId,
    syncStrategy: 'LOOKBACK',
    lookbackDaysOverride: UEI_LOOKBACK_DAYS_FALLBACK,
    originatingSyncStateId: payload.originatingSyncStateId,
  }
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-shared-secret': sharedSecret,
      ...extraHeaders,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  })
  if (resp.status === 409) {
    await resp.body?.cancel().catch(() => {})
    return { ok: true, status: 409 }
  }
  if (!resp.ok) {
    const text = await resp.text().catch(() => '<unreadable>')
    return { ok: false, status: resp.status, text }
  }
  await resp.body?.cancel().catch(() => {})
  return { ok: true, status: resp.status }
}

/**
 * Extract `userId` and `syncStateId` from an SxT row.
 * SxT returns column names UPPERCASE; tolerate snake/camel/upper/lower.
 *
 * @param {Record<string, unknown>} row
 */
export function extractRowFields(row) {
  if (!row || typeof row !== 'object') {
    throw new Error('row is not an object')
  }
  const get = (...keys) => {
    for (const k of keys) {
      if (k in row && row[k] != null && row[k] !== '') return row[k]
    }
    return null
  }
  const userId = get('USER_ID', 'user_id', 'userId')
  const syncStateId = get('SYNC_STATE_ID', 'sync_state_id', 'syncStateId')
  if (!userId || typeof userId !== 'string') {
    throw new Error('row missing user_id')
  }
  if (!syncStateId || typeof syncStateId !== 'string') {
    throw new Error('row missing sync_state_id')
  }
  return { userId, syncStateId }
}

/**
 * §A1 / NFR-3 (creatorland/dealsync-v2#522, Phase 3) — periodic safety-net
 * sweep. Polls SxT for failed 60-day LOOKBACK sync_states whose fallback
 * reason was persisted but which don't yet have a 45-day successor row, then
 * posts to backend's ingestion-trigger route per row to create the
 * re-attempt. Handles inline-trigger failures (network, backend brief
 * outage) and any environment where the inline trigger is disabled.
 *
 * @see docs/plans/2026-05-01-uei-fallback-emission-plan.md (Phase 3)
 */
export async function runFallbackReattemptPipeline() {
  const cid = randomUUID()
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const emailCoreSchemaRaw = core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING'

  const backendBaseUrl = core.getInput('dealsync-backend-base-url')
  const sharedSecret = core.getInput('dealsync-v2-shared-secret')
  const concurrency = parsePositiveIntegerInput(
    core.getInput('fallback-reattempt-concurrency') || '5',
    'fallback-reattempt-concurrency',
  )
  const batchSize = parsePositiveIntegerInput(
    core.getInput('fallback-reattempt-batch-size') || '200',
    'fallback-reattempt-batch-size',
  )

  if (!authUrl || !authSecret || !apiUrl || !biscuit || !emailCoreSchemaRaw) {
    throw new Error(
      'sxt-auth-url, sxt-auth-secret, sxt-api-url, sxt-biscuit, and email-core-schema are required',
    )
  }
  if (!backendBaseUrl || !sharedSecret) {
    throw new Error(
      'dealsync-backend-base-url and dealsync-v2-shared-secret are required',
    )
  }

  const sql = fallbackReattemptEligibility.selectUnreattemptedFallbacks(
    emailCoreSchemaRaw,
    batchSize,
  )
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (q) => executeSql(apiUrl, jwt, biscuit, q)

  console.log(`[fallback-reattempt] cid=${cid} executing eligibility query`)
  const result = await exec(sql)
  const rows = Array.isArray(result) ? result : []
  console.log(`[fallback-reattempt] cid=${cid} eligibility rows=${rows.length}`)

  let scanned = rows.length
  let posted = 0
  let alreadyInProgress = 0
  let errors = 0

  for (let i = 0; i < rows.length; i += concurrency) {
    const chunk = rows.slice(i, i + concurrency)
    await Promise.all(
      chunk.map(async (row) => {
        let userId, syncStateId
        try {
          ;({ userId, syncStateId } = extractRowFields(row))
        } catch (err) {
          core.error(
            `[fallback-reattempt] cid=${cid} skip invalid row: ${err.message}`,
          )
          errors++
          return
        }

        try {
          const res = await postFallbackReattempt(
            backendBaseUrl,
            sharedSecret,
            { userId, originatingSyncStateId: syncStateId },
            { 'x-correlation-id': cid },
          )
          if (!res.ok) {
            errors++
            core.error(
              `[fallback-reattempt] cid=${cid} POST failed userId=${userId} status=${res.status} body=${(res.text || '').slice(0, 500)}`,
            )
            return
          }
          if (res.status === 409) {
            alreadyInProgress++
            console.log(
              `[fallback-reattempt] cid=${cid} already in progress userId=${userId} originating=${syncStateId}`,
            )
            return
          }
          posted++
          console.log(
            `[fallback-reattempt] cid=${cid} posted userId=${userId} originating=${syncStateId}`,
          )
        } catch (err) {
          errors++
          core.error(
            `[fallback-reattempt] cid=${cid} error userId=${userId ?? '?'} originating=${syncStateId ?? '?'}: ${err.message}`,
          )
        }
      }),
    )
  }

  const summary = {
    correlationId: cid,
    scanned,
    posted,
    alreadyInProgress,
    errors,
  }
  console.log(`[fallback-reattempt] cid=${cid} done ${JSON.stringify(summary)}`)
  return summary
}
