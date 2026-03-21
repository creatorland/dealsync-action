import * as core from '@actions/core'
import { runFilter } from './filter.js'
import { runBuildPrompt } from './build-prompt.js'
import { runClassify } from './classify.js'
import { runDispatch } from './dispatch.js'
import { runExtractMetadata } from './extract-metadata.js'
import { runSxtQuery } from './sxt-query.js'
import { runFetchContent } from './fetch-content.js'
import { runWorkflowTriggers } from './workflow-triggers.js'

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
}

export async function run() {
  try {
    const command = core.getInput('command', { required: true })
    const handler = COMMANDS[command]
    if (!handler) {
      core.setFailed(
        `Unknown command: "${command}". Available: ${Object.keys(COMMANDS).join(', ')}`,
      )
      return
    }
    const result = await handler()
    core.setOutput('success', 'true')
    core.setOutput('result', JSON.stringify(result))
  } catch (error) {
    core.setOutput('success', 'false')
    core.setOutput('error', error.message)
    core.setOutput('error_stack', error.stack || '')
    core.setFailed(error.message)
  }
}
