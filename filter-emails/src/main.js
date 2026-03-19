import * as core from '@actions/core'

import {
  checkAuthenticationResults,
  checkSender,
  checkBulkHeaders,
  checkSubject,
  checkSenderName,
  checkFreeEmail,
} from './rules.js'

function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`Invalid ID format: ${id}`)
  }
  return id
}

export async function run() {
  try {
    const emailsJson = core.getInput('emails')
    if (!emailsJson || emailsJson === '[]') {
      core.setOutput('filtered_ids', '')
      core.setOutput('rejected_ids', '')
      core.setOutput('success', 'true')
      return
    }

    const emails = JSON.parse(emailsJson)
    const filteredIds = []
    const rejectedIds = []

    for (const email of emails) {
      const rejected =
        checkAuthenticationResults(email) ||
        checkSender(email) ||
        checkBulkHeaders(email) ||
        checkSubject(email) ||
        checkSenderName(email) ||
        checkFreeEmail(email)

      if (rejected) {
        rejectedIds.push(email.id)
      } else {
        filteredIds.push(email.id)
      }
    }

    core.setOutput(
      'filtered_ids',
      filteredIds.map((id) => `'${sanitizeId(id)}'`).join(','),
    )
    core.setOutput(
      'rejected_ids',
      rejectedIds.map((id) => `'${sanitizeId(id)}'`).join(','),
    )
    core.setOutput('success', 'true')
  } catch (error) {
    core.setOutput('success', 'false')
    core.setFailed(error.message)
  }
}
