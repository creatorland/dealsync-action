import * as core from '@actions/core'
import { runFilter } from './filter.js'
import { runBuildPrompt } from './build-prompt.js'
import { runClassify } from './classify.js'
import { runDispatch } from './dispatch.js'
import { runExtractMetadata } from './extract-metadata.js'
import { runSxtQuery } from './sxt-query.js'
import { runFetchContent } from './fetch-content.js'
import { runWorkflowTriggers } from './workflow-triggers.js'
import { runFetchAndFilter } from './fetch-and-filter.js'
import { runRetriggerStuck } from './retrigger-stuck.js'
import { runFetchAndClassify } from './fetch-and-classify.js'
import { runSaveEvals } from './save-evals.js'
import { runSaveDeals } from './save-deals.js'
import { runUpdateDealStates } from './update-deal-states.js'

const COMMANDS = {
  filter: runFilter,
  'build-prompt': runBuildPrompt,
  classify: runClassify,
  dispatch: runDispatch,
  'extract-metadata': runExtractMetadata,
  'sxt-query': runSxtQuery,
  'sxt-execute': runSxtQuery,
  'fetch-content': runFetchContent,
  'workflow-triggers': runWorkflowTriggers,
  'fetch-and-filter': runFetchAndFilter,
  'retrigger-stuck': runRetriggerStuck,
  'fetch-and-classify': runFetchAndClassify,
  'save-evals': runSaveEvals,
  'save-deals': runSaveDeals,
  'update-deal-states': runUpdateDealStates,
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
