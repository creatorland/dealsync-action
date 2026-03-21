import * as core from '@actions/core'
import { workflowTriggers, sanitizeSchema, sanitizeString } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

/**
 * Append or update workflow_triggers trail on deal_states claimed by a trigger hash.
 *
 * Actions:
 *   - "start": Append a new entry with success=false (called at processor start)
 *   - "complete": Update the last entry matching this trigger_hash to success=true
 */
export async function runWorkflowTriggers() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))

  const action = core.getInput('trigger-action') // 'start' or 'complete'
  const triggerHash = core.getInput('trigger-hash')
  const parentTriggerHash = core.getInput('parent-trigger-hash') || ''
  const triggerType = core.getInput('trigger-type') || 'filter' // 'filter' or 'detection'

  if (!triggerHash) throw new Error('trigger-hash is required')
  if (!['start', 'complete'].includes(action)) {
    throw new Error(`trigger-action must be "start" or "complete", got: ${action}`)
  }

  const jwt = await authenticate(authUrl, authSecret)

  // Fetch current workflow_triggers for claimed rows
  const rows = await executeSql(
    apiUrl,
    jwt,
    biscuit,
    workflowTriggers.fetchByTriggerHash(schema, triggerHash),
  )

  let updated = 0
  for (const row of rows) {
    const emailMetadataId = row.EMAIL_METADATA_ID
    let triggers = []
    try {
      triggers = row.WORKFLOW_TRIGGERS ? JSON.parse(row.WORKFLOW_TRIGGERS) : []
    } catch {
      triggers = []
    }

    if (action === 'start') {
      triggers.push({
        type: triggerType,
        trigger_hash: triggerHash,
        parent_trigger_hash: parentTriggerHash,
        timestamp: new Date().toISOString(),
        success: false,
      })
    } else {
      // complete: find last entry with this trigger_hash and set success=true
      for (let i = triggers.length - 1; i >= 0; i--) {
        if (triggers[i].trigger_hash === triggerHash) {
          triggers[i].success = true
          break
        }
      }
    }

    const serialized = sanitizeString(JSON.stringify(triggers))
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      workflowTriggers.update(schema, emailMetadataId, serialized),
    )
    updated++
  }

  core.info(`workflow-triggers ${action}: updated ${updated} deal_states`)
  return { updated }
}
