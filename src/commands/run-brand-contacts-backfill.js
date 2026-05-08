/**
 * Brand Contacts backfill command — paginates Firestore for tier-eligible users,
 * checks legacy OAuth token presence, dispatches to backend /sync/ingestion-trigger.
 * @see _bmad-output/implementation-artifacts/2-4-implement-dealsync-action-backfill-workflow.md
 */

import * as core from '@actions/core'
import { randomUUID } from 'node:crypto'
import { parsePositiveIntegerInput, parseStrictBoolean } from '../lib/inputs.js'
import {
  resolveFirestoreServiceAccountJson,
  normalizeOptionalProjectId,
} from './emit-scan-complete-webhooks.js'
import { makeGoogleDatastoreTokenProvider } from '../lib/scan-complete.js'
import {
  paginateTierEligibleUsers,
  checkLegacyTokenPresence,
  markBackfillDispatched,
} from '../lib/firestore-users.js'
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
  const dryRun = parseStrictBoolean(core.getInput('backfill-dry-run'), 'backfill-dry-run', false)

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
  let usersSkippedAlreadyDispatched = 0
  let dispatched = 0
  let dispatchSkippedAlreadyInProgress = 0
  // Split failure counters so Story 2.10's canary can distinguish "Firestore is
  // broken (us)" from "backend is broken (them)" without grepping logs. The
  // legacy single `dispatchFailed` is preserved as a sum of the two for
  // backwards-compatible alerting (= dispatchFailedTokenCheck + dispatchFailedBackend).
  let dispatchFailedTokenCheck = 0
  let dispatchFailedBackend = 0
  // Surfaces unexpected worker exceptions (TypeError, etc.) that escape the
  // workerFn's try/catch. Without an explicit counter the .catch() at the
  // pool boundary would swallow systemic bugs into a `console.log` line and
  // the run would exit 0. Story 2.10's canary should fire on this >0.
  let workerExceptions = 0

  // Cap per-user skip logs at first N per cohort to bound action-log size.
  // Without this, a high-skip-rate run (e.g., bulk of remaining users are revoked
  // or circuit-broken) would emit one info line per ineligible user across multiple
  // raw-page iterations. Counters + the final structured backfill_run_complete log
  // remain authoritative; spot-check sample is preserved for ops.
  const SKIP_LOG_CAP = 5
  let revokedLogged = 0
  let circuitBrokenLogged = 0
  let alreadyDispatchedLogged = 0

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

      // Persistent dispatch marker: skip users already dispatched in a prior cron
      // firing. Without this, the read-side query keeps re-yielding the same
      // already-dispatched users every day (the dispatch doesn't mutate `tier`
      // or `tierRevokedAt` — those are owned by Epic 1's tier-change webhook),
      // starving NFR-1's 8-week / 4,116-user budget. Marker is written by
      // `markBackfillDispatched` after a successful 200/201 dispatch.
      if (permissionTier.backfillDispatchedAt != null) {
        usersSkippedAlreadyDispatched++
        if (alreadyDispatchedLogged < SKIP_LOG_CAP) {
          core.info(
            `[brand-contacts-backfill] cid=${correlationId} skip already-dispatched userId=${userId}`,
          )
          alreadyDispatchedLogged++
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
      dispatchFailedTokenCheck++
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
        dispatchFailedBackend++
        core.error(
          `[brand-contacts-backfill] cid=${correlationId} POST failed userId=${userId} status=${res.status} body=${(res.text || '').slice(0, 500)}`,
        )
        return
      }

      dispatched++
      console.log(`[brand-contacts-backfill] cid=${correlationId} dispatched userId=${userId}`)

      // Persist `permissionTier.backfillDispatchedAt = serverTimestamp` AFTER the
      // 2xx ack so the same userId doesn't re-enqueue tomorrow. Mark-failure is
      // recoverable (backend's idempotency catches the re-dispatch tomorrow), so
      // the failure is logged but does NOT roll back `dispatched` or the metric.
      try {
        await markBackfillDispatched({ tokenProvider, gcpProjectId, userId })
      } catch (markErr) {
        core.warning(
          `[brand-contacts-backfill] cid=${correlationId} mark-dispatched failed userId=${userId}: ${markErr.message} (user will be re-considered next cron firing)`,
        )
      }
    } catch (err) {
      // Network-level throw on the backend POST (DNS / connection refused / timeout).
      // Counted under dispatchFailedBackend — symptom is "backend POST didn't
      // complete," same triage bucket as 4xx/5xx responses.
      dispatchFailedBackend++
      core.error(
        `[brand-contacts-backfill] cid=${correlationId} dispatch error userId=${userId}: ${err.message}`,
      )
    }
  }

  const onWorkerException = (err) => {
    workerExceptions++
    core.error(
      `[brand-contacts-backfill] cid=${correlationId} worker exception (escaped workerFn try/catch): ${err?.message ?? err}`,
    )
  }

  if (candidates.length > 0) {
    await runPool(claimFn, workerFn, { maxConcurrent: concurrency, onWorkerException })
  }

  const durationMs = Date.now() - startMs
  const summary = {
    correlationId,
    usersConsidered,
    usersEligible,
    usersSkippedRevoked,
    usersSkippedNoToken,
    usersSkippedAlreadyInFlight,
    usersSkippedAlreadyDispatched,
    dispatched,
    dispatchSkippedAlreadyInProgress,
    dispatchFailedTokenCheck,
    dispatchFailedBackend,
    // Legacy aggregate counter retained for backwards-compatible alerting on
    // Story 2.10's canary. New filters should prefer the split fields.
    dispatchFailed: dispatchFailedTokenCheck + dispatchFailedBackend,
    workerExceptions,
    durationMs,
    attributionTag,
  }

  console.log(JSON.stringify({ event: 'backfill_run_complete', ...summary }))

  // Surface escaped worker exceptions to the GitHub Action step's exit code.
  // Per-user failures (token check / backend POST) are bounded outcomes counted
  // in `dispatchFailed*`; an exception escaping `workerFn`'s try/catch is a
  // programming bug. Refuse to exit 0 on this so the runner job goes red and
  // operators don't silently miss systemic regressions.
  if (workerExceptions > 0) {
    throw new Error(
      `Backfill run completed but ${workerExceptions} worker exception(s) escaped — see action log; refusing to exit 0`,
    )
  }

  return summary
}

/**
 * Inline concurrency pool — mirrors runPool from pipeline.js but simplified for
 * the backfill use case where each "batch" is a single userId and per-user
 * errors are caught inside workerFn (no retry / dead-letter at the pool level).
 *
 * Unexpected worker exceptions (TypeError, etc. that escape workerFn's
 * try/catch) are surfaced via the optional `onWorkerException` callback —
 * the caller increments a counter + uses `core.error` so the GitHub Action
 * step is annotated with the failure. Pool continues so siblings finish; the
 * caller decides whether to throw at end-of-run based on the counter.
 *
 * @param {() => Promise<object|null>} claimFn
 * @param {(batch: object) => Promise<void>} workerFn
 * @param {{ maxConcurrent: number, onWorkerException?: (err: unknown) => void }} opts
 */
export async function runPool(claimFn, workerFn, { maxConcurrent, onWorkerException }) {
  const active = new Set()

  while (true) {
    if (active.size < maxConcurrent) {
      const batch = await claimFn()
      if (batch === null) {
        if (active.size === 0) break
        await Promise.race(active)
        continue
      }
      // workerFn catches its own expected per-user errors. Anything that escapes
      // is a programming bug, not a user-side failure — surface via callback so
      // the run summary reflects it; do NOT re-throw (siblings must complete).
      const worker = (async () => {
        await workerFn(batch)
      })().catch((err) => {
        if (onWorkerException) onWorkerException(err)
      })
      active.add(worker)
      worker.finally(() => active.delete(worker))
    } else {
      await Promise.race(active)
    }
  }
}
