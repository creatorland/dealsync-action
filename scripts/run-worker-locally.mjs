#!/usr/bin/env node
/**
 * Phase 0 spike — local orchestrator for the settlement worker.
 *
 * Replicates what `settlement-worker.yaml` does on W3, but locally:
 *   guard → worker (if claimed) → finalize (if claimed)
 *
 * Env vars (loaded from process.env; the runner script in dealsync-v2 sources
 * `.env.staging` before invoking this):
 *   SUPABASE_URL                 e.g. https://rplbqswbhcinfkkueojl.supabase.co
 *   SUPABASE_SERVICE_ROLE_KEY    sb_secret_... (service_role; bypasses RLS)
 *   SXT_AUTH_URL                 backend JWT proxy URL (optional if dry-run)
 *   SXT_AUTH_SECRET              shared secret (optional if dry-run)
 *   SXT_BISCUIT                  pre-generated biscuit (optional; absent = dry-run)
 *   SXT_API_URL                  defaults to https://api.makeinfinite.dev
 *
 * Arguments / env:
 *   --outbox-id=<id>             (or OUTBOX_ID env) target a specific row
 *   --batch-size=<n>             (or BATCH_SIZE; default 100)
 *   --lease-seconds=<n>          (or LEASE_SECONDS; default 60)
 *   WORKER_ID                    defaults to `local-<timestamp>`
 *
 * Exit codes:
 *   0 = settle path completed successfully (claimed-count may be 0)
 *   1 = error mid-run
 *
 * Usage:
 *   set -a; source .env.staging; set +a
 *   node scripts/run-worker-locally.mjs --outbox-id=42
 */

import { run as runGuard } from '../src/outbox-settlement-guard/index.js'
import { run as runWorker } from '../src/outbox-settlement-worker/index.js'
import { run as runFinalizer } from '../src/outbox-settlement-finalizer/index.js'

function parseArgs() {
  const args = {}
  for (const a of process.argv.slice(2)) {
    const m = a.match(/^--([^=]+)=(.*)$/)
    if (m) args[m[1]] = m[2]
  }
  return args
}

async function main() {
  const args = parseArgs()
  const workerId = process.env.WORKER_ID || `local-${Date.now()}`
  const batchSize = Number(args['batch-size'] || process.env.BATCH_SIZE || 100)
  const leaseSeconds = Number(args['lease-seconds'] || process.env.LEASE_SECONDS || 60)
  const outboxId = args['outbox-id'] || process.env.OUTBOX_ID || null

  const t0 = Date.now()

  // Step 1: guard
  const guardOut = await runGuard({ batchSize, leaseSeconds, workerId, outboxId })

  if (guardOut.claimedCount === 0) {
    // Mirror the workflow YAML's gate: skip worker + finalize when no claim.
    // Still emit the workflow_completed terminal event for observability.
    const workerOut = { results: [] }
    await runFinalizer({ results: workerOut.results, workerId, batchSize })
    const elapsed = Date.now() - t0
    process.stdout.write(`\n→ local-runner: claimed 0 rows, exiting (elapsed ${elapsed}ms)\n`)
    return
  }

  // Step 2: worker
  const workerOut = await runWorker({
    claimedBatch: guardOut.claimedBatch,
    workerId,
    leaseSeconds,
  })

  // Step 3: finalize
  const finalizerOut = await runFinalizer({
    results: workerOut.results,
    workerId,
    batchSize,
  })

  const elapsed = Date.now() - t0
  process.stdout.write(
    `\n→ local-runner: settled=${finalizerOut.settledCount}, failed=${finalizerOut.failedCount}, dead-letter=${finalizerOut.deadLetterCount}, more-work=${finalizerOut.moreWork}, elapsed=${elapsed}ms\n`,
  )
}

main().catch((err) => {
  process.stderr.write(`\n✗ local-runner FAILED: ${err.message}\n`)
  if (err.stack) process.stderr.write(err.stack + '\n')
  process.exit(1)
})
