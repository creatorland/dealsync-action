/**
 * outbox-settlement-finalizer — GHA / W3 entry point (Story 0.6 / FR-P0-6).
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

    const resultsRaw = core.getInput('results', { required: true })
    let results
    try {
      results = JSON.parse(resultsRaw)
    } catch (e) {
      throw new Error(`results input is not valid JSON: ${(e instanceof Error ? e.message : String(e))}`)
    }
    if (!Array.isArray(results)) {
      throw new Error('results input must decode to a JSON array')
    }

    const workerId = core.getInput('worker-id', { required: true })
    const batchSize = Number(core.getInput('batch-size') || '100')
    const claimedCountRaw = core.getInput('claimed-count')
    const claimedCount =
      claimedCountRaw && claimedCountRaw.trim().length > 0
        ? Number(claimedCountRaw)
        : results.length

    const { moreWork, settledCount, failedCount, deadLetterCount } = await run({
      results,
      workerId,
      batchSize,
      claimedCount,
    })

    core.setOutput('more-work', String(moreWork))
    core.setOutput('settled-count', String(settledCount))
    core.setOutput('failed-count', String(failedCount))
    core.setOutput('dead-letter-count', String(deadLetterCount))
  } catch (error) {
    core.setFailed(error instanceof Error ? error.message : String(error))
  }
}

main()
