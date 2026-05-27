/**
 * outbox-settlement-worker — GHA / W3 entry point (Story 0.6 / FR-P0-6).
 *
 * Thin wrapper around the pure `run()` exported by index.js. See the guard's
 * main.js for the pattern.
 */

import * as core from '@actions/core'

import { run } from './index.js'

async function main() {
  try {
    const supabaseUrl = core.getInput('supabase-url', { required: true })
    const supabaseServiceRoleKey = core.getInput('supabase-service-role-key', { required: true })
    process.env.SUPABASE_URL = supabaseUrl
    process.env.SUPABASE_SERVICE_ROLE_KEY = supabaseServiceRoleKey

    // SxT credentials are optional — without SXT_BISCUIT the worker dry-runs
    // (logs SQL without executing). Keep the empty-string-is-absent contract.
    const sxtAuthUrl = core.getInput('sxt-auth-url')
    if (sxtAuthUrl) process.env.SXT_AUTH_URL = sxtAuthUrl
    const sxtAuthSecret = core.getInput('sxt-auth-secret')
    if (sxtAuthSecret) process.env.SXT_AUTH_SECRET = sxtAuthSecret
    const sxtBiscuit = core.getInput('sxt-biscuit')
    if (sxtBiscuit) process.env.SXT_BISCUIT = sxtBiscuit
    const sxtApiUrl = core.getInput('sxt-api-url')
    if (sxtApiUrl) process.env.SXT_API_URL = sxtApiUrl

    const claimedBatchRaw = core.getInput('claimed-batch', { required: true })
    let claimedBatch
    try {
      claimedBatch = JSON.parse(claimedBatchRaw)
    } catch (e) {
      throw new Error(`claimed-batch input is not valid JSON: ${(e instanceof Error ? e.message : String(e))}`)
    }
    if (!Array.isArray(claimedBatch)) {
      throw new Error('claimed-batch input must decode to a JSON array')
    }

    const workerId = core.getInput('worker-id', { required: true })
    const leaseSeconds = Number(core.getInput('lease-seconds') || '60')

    const { results } = await run({
      claimedBatch,
      workerId,
      leaseSeconds,
    })

    core.setOutput('results', JSON.stringify(results))
  } catch (error) {
    core.setFailed(error instanceof Error ? error.message : String(error))
  }
}

main()
