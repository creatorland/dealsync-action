import * as core from '@actions/core'
import { orchestrator, sanitizeSchema } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

/**
 * Find stuck batches (>10min, attempts<3) and retrigger them.
 * The processor resumes from audit checkpoint — no status reset needed.
 */
export async function runRetriggerStuck() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const w3RpcUrl = core.getInput('w3-rpc-url')
  const processorName = core.getInput('processor-name') || 'Dealsync Processor'

  console.log('[retrigger] checking for stuck batches...')
  const jwt = await authenticate(authUrl, authSecret)

  const stuckBatches = await executeSql(apiUrl, jwt, biscuit, orchestrator.findStuckBatches(schema))

  if (stuckBatches.length === 0) {
    console.log('[retrigger] no stuck batches')
    return { retriggered: 0 }
  }

  console.log(`[retrigger] found ${stuckBatches.length} stuck batch(es)`)

  let retriggered = 0
  for (const stuck of stuckBatches) {
    const inputs = {}
    if (stuck.BATCH_TYPE === 'filter') {
      inputs.filter_batch_id = stuck.BATCH_ID
    } else {
      inputs.classify_batch_id = stuck.BATCH_ID
    }

    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      retriggered++
      console.log(`[retrigger] ${stuck.BATCH_TYPE}: ${stuck.BATCH_ID}`)
    } catch (err) {
      console.log(`[retrigger] failed ${stuck.BATCH_ID}: ${err.message}`)
    }
  }

  console.log(`[retrigger] done: ${retriggered}/${stuckBatches.length}`)
  return { retriggered, total: stuckBatches.length }
}
