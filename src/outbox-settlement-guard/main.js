/**
 * outbox-settlement-guard — GHA / W3 entry point (Story 0.6 / FR-P0-6).
 *
 * Thin wrapper around the pure `run()` exported by index.js:
 *   1. Read inputs via @actions/core.getInput()
 *   2. Map them onto process.env (the run() implementation reads supabase
 *      credentials via requireEnv() — keep that contract intact so the
 *      local-runner regression check from Story 0.2 keeps working)
 *   3. Set outputs via @actions/core.setOutput()
 *
 * `dist/index.js` is the Rollup-bundled version of this file. The
 * `runs.main` field in action.yml points there.
 */

import * as core from '@actions/core'

import { run } from './index.js'

async function main() {
  try {
    const supabaseUrl = core.getInput('supabase-url', { required: true })
    const supabaseServiceRoleKey = core.getInput('supabase-service-role-key', { required: true })
    process.env.SUPABASE_URL = supabaseUrl
    process.env.SUPABASE_SERVICE_ROLE_KEY = supabaseServiceRoleKey

    const batchSize = Number(core.getInput('batch-size') || '100')
    const leaseSeconds = Number(core.getInput('lease-seconds') || '60')
    const workerId = core.getInput('worker-id', { required: true })
    const outboxIdRaw = core.getInput('outbox-id')
    const outboxId = outboxIdRaw && outboxIdRaw.trim().length > 0 ? outboxIdRaw.trim() : null

    const { claimedBatch, claimedCount } = await run({
      batchSize,
      leaseSeconds,
      workerId,
      outboxId,
    })

    core.setOutput('claimed-batch', JSON.stringify(claimedBatch))
    core.setOutput('claimed-count', String(claimedCount))
  } catch (error) {
    core.setFailed(error instanceof Error ? error.message : String(error))
  }
}

main()
