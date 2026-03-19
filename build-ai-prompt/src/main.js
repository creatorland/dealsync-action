import * as core from '@actions/core'

import { buildPrompt } from './prompt-template.js'

export async function run() {
  try {
    const emailsJson = core.getInput('emails')
    if (!emailsJson || emailsJson === '[]') {
      core.setOutput('system-prompt', '')
      core.setOutput('user-prompt', '')
      core.setOutput('success', 'true')
      return
    }

    const emails = JSON.parse(emailsJson)
    const { systemPrompt, userPrompt } = buildPrompt(emails)

    core.setOutput('system-prompt', systemPrompt)
    core.setOutput('user-prompt', userPrompt)
    core.setOutput('success', 'true')
  } catch (error) {
    core.setOutput('success', 'false')
    core.setFailed(error.message)
  }
}
