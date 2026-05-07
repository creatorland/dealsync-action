/**
 * Brand Contacts backfill command — paginates Firestore for tier-eligible users,
 * checks legacy OAuth token presence, dispatches to backend /sync/ingestion-trigger.
 * @see _bmad-output/implementation-artifacts/2-4-implement-dealsync-action-backfill-workflow.md
 */

import * as core from '@actions/core'
import { randomUUID } from 'node:crypto'
import { parsePositiveIntegerInput } from '../lib/inputs.js'
import {
  resolveFirestoreServiceAccountJson,
  normalizeOptionalProjectId,
} from './emit-scan-complete-webhooks.js'
import { makeGoogleDatastoreTokenProvider } from '../lib/scan-complete.js'
import { paginateTierEligibleUsers, checkLegacyTokenPresence } from '../lib/firestore-users.js'
import { postBackfillIngestionTrigger } from '../lib/backfill-dispatch.js'

/**
 * @returns {Promise<object>} summary with dispatch tallies
 */
export async function runBrandContactsBackfill() {
  const correlationId = randomUUID()
  const startMs = Date.now()

  const backendBaseUrl = core.getInput('dealsync-backend-base-url')
  const sharedSecret = core.getInput('dealsync-v2-shared-secret')
  const saJsonRaw = resolveFirestoreServiceAccountJson()
  if (saJsonRaw) core.setSecret(saJsonRaw)

  const gcpProjectIdInput = normalizeOptionalProjectId(core.getInput('gcp-project-id'))

  const batchSize = parsePositiveIntegerInput(
    core.getInput('backfill-batch-size') || '75',
    'backfill-batch-size',
  )
  const concurrency = parsePositiveIntegerInput(
    core.getInput('backfill-concurrency') || '5',
    'backfill-concurrency',
  )
  const attributionTag = core.getInput('backfill-attribution-tag') || 'brand-contacts-backfill'
  const dryRun = ['true', '1', 'yes'].includes(
    String(core.getInput('backfill-dry-run') ?? '')
      .trim()
      .toLowerCase(),
  )

  if (!backendBaseUrl || !sharedSecret || !saJsonRaw) {
    throw new Error(
      'dealsync-backend-base-url, dealsync-v2-shared-secret, and Firestore service account JSON are required',
    )
  }

  let credentials
  try {
    credentials = JSON.parse(saJsonRaw)
  } catch {
    throw new Error('Firestore service account JSON must be valid JSON')
  }

  const gcpProjectId =
    gcpProjectIdInput ||
    (typeof credentials.project_id === 'string'
      ? normalizeOptionalProjectId(credentials.project_id)
      : '')
  if (!gcpProjectId) {
    throw new Error('gcp-project-id input or Firestore service account JSON project_id is required')
  }
  if (!/^[a-z][-a-z0-9]{4,28}[a-z0-9]$/.test(gcpProjectId)) {
    throw new Error(
      `gcp-project-id must match GCP project ID format (lowercase letter start, 6-30 chars, ends with letter/digit): got "${gcpProjectId}"`,
    )
  }

  const tokenProvider = makeGoogleDatastoreTokenProvider(credentials)

  let usersConsidered = 0
  let usersEligible = 0
  let usersSkippedRevoked = 0
  let usersSkippedNoToken = 0
  let usersSkippedAlreadyInFlight = 0
  let dispatched = 0
  let dispatchSkippedAlreadyInProgress = 0
  let dispatchFailed = 0

  // Cap per-user skip logs at first N per cohort to bound action-log size.
  // Without this, a high-skip-rate run (e.g., bulk of remaining users are revoked
  // or circuit-broken) would emit one info line per ineligible user across multiple
  // raw-page iterations. Counters + the final structured backfill_run_complete log
  // remain authoritative; spot-check sample is preserved for ops.
  const SKIP_LOG_CAP = 5
  let revokedLogged = 0
  let circuitBrokenLogged = 0

  console.log(
    `[brand-contacts-backfill] cid=${correlationId} starting batchSize=${batchSize} concurrency=${concurrency} dryRun=${dryRun}`,
  )

  const candidates = []

  for await (const page of paginateTierEligibleUsers({
    tokenProvider,
    gcpProjectId,
    batchSize,
  })) {
    for (const { userId, permissionTier } of page) {
      usersConsidered++

      // Belt-and-suspenders: Firestore composite filter already excludes non-readonly,
      // but this guard catches any drift in paginateTierEligibleUsers' query shape.
      if (permissionTier.tier !== 'readonly') {
        continue
      }

      if (permissionTier.tierRevokedAt != null) {
        usersSkippedRevoked++
        if (revokedLogged < SKIP_LOG_CAP) {
          core.info(`[brand-contacts-backfill] cid=${correlationId} skip revoked userId=${userId}`)
          revokedLogged++
        }
        continue
      }

      if (permissionTier.backfillCircuitBrokenAt != null) {
        usersSkippedAlreadyInFlight++
        if (circuitBrokenLogged < SKIP_LOG_CAP) {
          core.info(
            `[brand-contacts-backfill] cid=${correlationId} skip circuit-broken userId=${userId}`,
          )
          circuitBrokenLogged++
        }
        continue
      }

      usersEligible++
      candidates.push(userId)

      if (candidates.length >= batchSize) break
    }
    if (candidates.length >= batchSize) break
  }

  console.log(
    `[brand-contacts-backfill] cid=${correlationId} considered=${usersConsidered} eligible=${usersEligible} candidates=${candidates.length}`,
  )

  let candidateIdx = 0

  const claimFn = async () => {
    if (candidateIdx >= candidates.length) return null
    const userId = candidates[candidateIdx++]
    return { batch_id: userId, userId }
  }

  const workerFn = async (batch) => {
    const { userId } = batch

    let tokenPresent
    try {
      const result = await checkLegacyTokenPresence({ tokenProvider, gcpProjectId, userId })
      tokenPresent = result.present
    } catch (err) {
      core.error(
        `[brand-contacts-backfill] cid=${correlationId} token check failed userId=${userId}: ${err.message}`,
      )
      dispatchFailed++
      return
    }

    if (!tokenPresent) {
      usersSkippedNoToken++
      core.warning(`[brand-contacts-backfill] cid=${correlationId} skip no-token userId=${userId}`)
      return
    }

    try {
      const res = await postBackfillIngestionTrigger({
        backendBaseUrl,
        sharedSecret,
        userId,
        attributionTag,
        dryRun,
        extraHeaders: { 'x-correlation-id': correlationId },
      })

      if (res.alreadyInProgress) {
        dispatchSkippedAlreadyInProgress++
        console.log(
          `[brand-contacts-backfill] cid=${correlationId} already in progress userId=${userId}`,
        )
        return
      }

      if (!res.ok) {
        dispatchFailed++
        core.error(
          `[brand-contacts-backfill] cid=${correlationId} POST failed userId=${userId} status=${res.status} body=${(res.text || '').slice(0, 500)}`,
        )
        return
      }

      dispatched++
      console.log(`[brand-contacts-backfill] cid=${correlationId} dispatched userId=${userId}`)
    } catch (err) {
      dispatchFailed++
      core.error(
        `[brand-contacts-backfill] cid=${correlationId} dispatch error userId=${userId}: ${err.message}`,
      )
    }
  }

  if (candidates.length > 0) {
    await runPool(claimFn, workerFn, { maxConcurrent: concurrency })
  }

  const durationMs = Date.now() - startMs
  const summary = {
    correlationId,
    usersConsidered,
    usersEligible,
    usersSkippedRevoked,
    usersSkippedNoToken,
    usersSkippedAlreadyInFlight,
    dispatched,
    dispatchSkippedAlreadyInProgress,
    dispatchFailed,
    durationMs,
    attributionTag,
  }

  console.log(JSON.stringify({ event: 'backfill_run_complete', ...summary }))

  return summary
}

/**
 * Inline concurrency pool — mirrors runPool from pipeline.js but simplified for
 * the backfill use case where each "batch" is a single userId and per-user
 * errors are caught inside workerFn (no retry / dead-letter at the pool level).
 *
 * @param {() => Promise<object|null>} claimFn
 * @param {(batch: object) => Promise<void>} workerFn
 * @param {{ maxConcurrent: number }} opts
 */
async function runPool(claimFn, workerFn, { maxConcurrent }) {
  const active = new Set()

  while (true) {
    if (active.size < maxConcurrent) {
      const batch = await claimFn()
      if (batch === null) {
        if (active.size === 0) break
        await Promise.race(active)
        continue
      }
      // Defensive .catch(): workerFn catches its own errors today, but if a future
      // change lets one leak, swallow at the pool boundary so siblings finish.
      const worker = (async () => {
        await workerFn(batch)
      })().catch((err) => {
        console.log(`[brand-contacts-backfill] worker exception: ${err?.message ?? err}`)
      })
      active.add(worker)
      worker.finally(() => active.delete(worker))
    } else {
      await Promise.race(active)
    }
  }
}
