import * as core from '@actions/core'
import { runDispatch } from './dispatch.js'
import { runDispatchDealStateSync } from './dispatch-deal-state-sync.js'
import { runSyncDealStates } from './sync-deal-states.js'
import { runFetchAndClassify } from './fetch-and-classify.js'
import { runFetchAndFilter } from './fetch-and-filter.js'
import { runRetriggerStuck } from './retrigger-stuck.js'
import { runSaveDeals } from './save-deals.js'
import { runSaveEvals } from './save-evals.js'
import { runSxtQuery } from './sxt-query.js'
import { runUpdateDealStates } from './update-deal-states.js'

const COMMANDS = {
  dispatch: runDispatch,
  'sxt-execute': runSxtQuery,
  'fetch-and-filter': runFetchAndFilter,
  'retrigger-stuck': runRetriggerStuck,
  'fetch-and-classify': runFetchAndClassify,
  'save-evals': runSaveEvals,
  'save-deals': runSaveDeals,
  'update-deal-states': runUpdateDealStates,
  'sync-deal-states': runSyncDealStates,
  'dispatch-deal-state-sync': runDispatchDealStateSync,
}

export async function run() {
  let command = 'unknown'
  try {
    command = core.getInput('command', { required: true })
    const handler = COMMANDS[command]
    if (!handler) {
      core.setFailed(
        `Unknown command: "${command}". Available: ${Object.keys(COMMANDS).join(', ')}`,
      )
      return
    }
    console.log(`[dealsync] command=${command} starting`)
    const result = await handler()
    console.log(`[dealsync] command=${command} success`)
    core.setOutput('success', 'true')
    // If result is already a string (e.g. encrypted output), don't double-stringify
    core.setOutput('result', typeof result === 'string' ? result : JSON.stringify(result))
  } catch (error) {
    console.log(`[dealsync] command=${command} FAILED: ${error.message}`)
    console.log(`[dealsync] stack: ${error.stack}`)
    core.setOutput('success', 'false')
    core.setOutput('error', error.message)
    core.setOutput('error_stack', error.stack || '')
    core.setFailed(error.message)
  }
}
