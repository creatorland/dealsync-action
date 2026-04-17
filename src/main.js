import * as core from '@actions/core'
import { runSyncDealStates } from './commands/sync-deal-states.js'
import { runEval } from './commands/eval.js'
import { runEvalCompare } from './commands/eval-compare.js'
import { runFilterPipeline } from './commands/run-filter-pipeline.js'
import { runClassifyPipeline } from './commands/run-classify-pipeline.js'
import { runRecoveryPipeline } from './commands/run-recovery-pipeline.js'
import { runEmitScanCompleteWebhooks } from './commands/emit-scan-complete-webhooks.js'

const COMMANDS = {
  'sync-deal-states': runSyncDealStates,
  eval: runEval,
  'eval-compare': runEvalCompare,
  'run-filter-pipeline': runFilterPipeline,
  'run-classify-pipeline': runClassifyPipeline,
  'run-recovery-pipeline': runRecoveryPipeline,
  'emit-scan-complete-webhooks': runEmitScanCompleteWebhooks,
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
    throw error
  }
}
