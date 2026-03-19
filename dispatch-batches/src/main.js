import * as core from '@actions/core'

import { decryptValue } from '../../shared/crypto.js'
import { validatePositiveInt } from './validators.js'

async function executeSql(sxtApiUrl, accessToken, biscuit, sqlText) {
  const resp = await fetch(`${sxtApiUrl}/v1/sql`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ sqlText, biscuits: [biscuit] }),
  })
  if (!resp.ok) throw new Error(`SxT ${resp.status}: ${await resp.text()}`)
  return resp.json()
}

function sanitizeSchema(schema) {
  if (!/^[a-zA-Z0-9_]+$/.test(schema)) {
    throw new Error(`Invalid schema: ${schema}`)
  }
  return schema
}

async function claimFilterBatch(
  sxtApiUrl,
  accessToken,
  biscuitDml,
  biscuitSelect,
  schema,
  transitionStage,
  batchSize,
) {
  // Atomic claim: move stage-2 emails into transition stage
  await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitDml,
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${transitionStage} WHERE ID IN (SELECT ID FROM ${schema}.EMAIL_METADATA WHERE STAGE = 2 LIMIT ${batchSize})`,
  )

  // Verify how many were claimed
  const rows = await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitSelect,
    `SELECT COUNT(*) AS CNT FROM ${schema}.EMAIL_METADATA WHERE STAGE = ${transitionStage}`,
  )
  return rows[0]?.CNT ?? 0
}

async function claimDetectBatch(
  sxtApiUrl,
  accessToken,
  biscuitDml,
  biscuitSelect,
  schema,
  transitionStage,
  batchSize,
) {
  // Atomic claim with thread-completeness check
  await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitDml,
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${transitionStage} WHERE ID IN (SELECT em.ID FROM ${schema}.EMAIL_METADATA em WHERE em.STAGE = 3 AND NOT EXISTS (SELECT 1 FROM ${schema}.EMAIL_METADATA m2 WHERE m2.THREAD_ID = em.THREAD_ID AND m2.USER_ID = em.USER_ID AND m2.STAGE IN (1, 2)) LIMIT ${batchSize})`,
  )

  // Verify how many were claimed
  const rows = await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitSelect,
    `SELECT COUNT(*) AS CNT FROM ${schema}.EMAIL_METADATA WHERE STAGE = ${transitionStage}`,
  )
  return rows[0]?.CNT ?? 0
}

async function writeDispatchLog(
  sxtApiUrl,
  accessToken,
  biscuitDml,
  schema,
  transitionStage,
  batchType,
) {
  await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitDml,
    `DELETE FROM ${schema}.DISPATCH_LOG WHERE TRANSITION_STAGE = ${transitionStage}`,
  )
  await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitDml,
    `INSERT INTO ${schema}.DISPATCH_LOG (TRANSITION_STAGE, TRIGGER_HASH, BATCH_TYPE, CREATED_AT) VALUES (${transitionStage}, '', '${batchType}', CURRENT_TIMESTAMP)`,
  )
}

async function triggerWorkflow(
  w3RpcUrl,
  callIndex,
  batchType,
  transitionStage,
  resetStage,
  previousTriggerHash,
) {
  const resp = await fetch(w3RpcUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0',
      method: 'w3_triggerWorkflow',
      params: {
        workflowName: 'dealsync-processor',
        body: {
          batch_type: batchType,
          transition_stage: String(transitionStage),
          reset_stage: resetStage,
          previous_trigger_hash: previousTriggerHash,
        },
      },
      id: callIndex,
    }),
  })

  const result = await resp.json()
  if (result.error) {
    throw new Error(
      `W3 RPC error ${result.error.code}: ${result.error.message}`,
    )
  }
  return result.result.triggerHash
}

async function lookupPreviousTriggerHash(
  sxtApiUrl,
  accessToken,
  biscuitSelect,
  schema,
  transitionStage,
) {
  const rows = await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitSelect,
    `SELECT TRIGGER_HASH FROM ${schema}.DISPATCH_LOG WHERE TRANSITION_STAGE = ${transitionStage}`,
  )
  return rows[0]?.TRIGGER_HASH || ''
}

async function resetClaimedEmails(
  sxtApiUrl,
  accessToken,
  biscuitDml,
  schema,
  transitionStage,
  resetStage,
) {
  await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitDml,
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${resetStage} WHERE STAGE = ${transitionStage}`,
  )
}

async function updateDispatchLogHash(
  sxtApiUrl,
  accessToken,
  biscuitDml,
  schema,
  transitionStage,
  triggerHash,
) {
  await executeSql(
    sxtApiUrl,
    accessToken,
    biscuitDml,
    `UPDATE ${schema}.DISPATCH_LOG SET TRIGGER_HASH = '${triggerHash}' WHERE TRANSITION_STAGE = ${transitionStage}`,
  )
}

export async function run() {
  try {
    const encryptionKey = core.getInput('encryption-key')
    const sxtApiUrl = core.getInput('sxt-api-url')
    const schema = sanitizeSchema(core.getInput('sxt-schema'))
    const w3RpcUrl = core.getInput('w3-rpc-url')

    // Decrypt sensitive tokens
    const accessToken = decryptValue(
      core.getInput('sxt-access-token'),
      encryptionKey,
    )
    const biscuitSelect = decryptValue(
      core.getInput('sxt-biscuit-select'),
      encryptionKey,
    )
    const biscuitDml = decryptValue(
      core.getInput('sxt-biscuit-dml'),
      encryptionKey,
    )

    // Parse numeric inputs
    const activeFilter = validatePositiveInt(
      core.getInput('active-filter'),
      'active-filter',
    )
    const activeDetect = validatePositiveInt(
      core.getInput('active-detect'),
      'active-detect',
    )
    const pendingFilter = validatePositiveInt(
      core.getInput('pending-filter'),
      'pending-filter',
    )
    const pendingDetect = validatePositiveInt(
      core.getInput('pending-detect'),
      'pending-detect',
    )
    const maxFilter = validatePositiveInt(
      core.getInput('max-filter'),
      'max-filter',
    )
    const maxDetect = validatePositiveInt(
      core.getInput('max-detect'),
      'max-detect',
    )
    const filterBatchSize = validatePositiveInt(
      core.getInput('filter-batch-size'),
      'filter-batch-size',
    )
    const detectBatchSize = validatePositiveInt(
      core.getInput('detect-batch-size'),
      'detect-batch-size',
    )

    // Calculate available slots
    let filterSlots = maxFilter - activeFilter
    let detectSlots = maxDetect - activeDetect

    // Early exit if nothing to do
    if (pendingFilter === 0 && pendingDetect === 0) {
      core.info('No pending emails to dispatch')
      core.setOutput('success', 'true')
      core.setOutput('dispatched_filter_count', '0')
      core.setOutput('dispatched_detect_count', '0')
      return
    }

    // Track claimed batches
    const filterBatches = [] // { transitionStage, count }
    const detectBatches = [] // { transitionStage, count }

    // Claim filter batches
    let filterBatchIndex = 0
    while (filterSlots > 0 && pendingFilter > 0) {
      const transitionStage = 1000 + filterBatchIndex * 10
      const claimed = await claimFilterBatch(
        sxtApiUrl,
        accessToken,
        biscuitDml,
        biscuitSelect,
        schema,
        transitionStage,
        filterBatchSize,
      )
      if (claimed === 0) break
      filterBatches.push({ transitionStage, count: claimed })
      filterSlots -= claimed
      filterBatchIndex++
    }

    // Claim detection batches
    let detectBatchIndex = 0
    while (detectSlots > 0 && pendingDetect > 0) {
      const transitionStage = 11000 + detectBatchIndex * 10
      const claimed = await claimDetectBatch(
        sxtApiUrl,
        accessToken,
        biscuitDml,
        biscuitSelect,
        schema,
        transitionStage,
        detectBatchSize,
      )
      if (claimed === 0) break
      detectBatches.push({ transitionStage, count: claimed })
      detectSlots -= claimed
      detectBatchIndex++
    }

    // Write dispatch logs and trigger workflows
    let callIndex = 1
    let dispatchedFilterCount = 0
    let dispatchedDetectCount = 0

    // Dispatch filter batches
    for (const batch of filterBatches) {
      const previousTriggerHash = await lookupPreviousTriggerHash(
        sxtApiUrl,
        accessToken,
        biscuitSelect,
        schema,
        batch.transitionStage,
      )

      await writeDispatchLog(
        sxtApiUrl,
        accessToken,
        biscuitDml,
        schema,
        batch.transitionStage,
        'filter',
      )

      try {
        const triggerHash = await triggerWorkflow(
          w3RpcUrl,
          callIndex++,
          'filter',
          batch.transitionStage,
          '2',
          previousTriggerHash,
        )
        await updateDispatchLogHash(
          sxtApiUrl,
          accessToken,
          biscuitDml,
          schema,
          batch.transitionStage,
          triggerHash,
        )
        dispatchedFilterCount++
        core.info(
          `Filter batch ${batch.transitionStage}: claimed ${batch.count}, trigger=${triggerHash}`,
        )
      } catch (err) {
        core.error(
          `Filter batch ${batch.transitionStage} trigger failed: ${err.message}`,
        )
        await resetClaimedEmails(
          sxtApiUrl,
          accessToken,
          biscuitDml,
          schema,
          batch.transitionStage,
          2,
        )
      }

      // Rate limit: 100ms gap between triggers
      await new Promise((r) => setTimeout(r, 100))
    }

    // Dispatch detection batches
    for (const batch of detectBatches) {
      const previousTriggerHash = await lookupPreviousTriggerHash(
        sxtApiUrl,
        accessToken,
        biscuitSelect,
        schema,
        batch.transitionStage,
      )

      await writeDispatchLog(
        sxtApiUrl,
        accessToken,
        biscuitDml,
        schema,
        batch.transitionStage,
        'detect',
      )

      try {
        const triggerHash = await triggerWorkflow(
          w3RpcUrl,
          callIndex++,
          'detect',
          batch.transitionStage,
          '3',
          previousTriggerHash,
        )
        await updateDispatchLogHash(
          sxtApiUrl,
          accessToken,
          biscuitDml,
          schema,
          batch.transitionStage,
          triggerHash,
        )
        dispatchedDetectCount++
        core.info(
          `Detect batch ${batch.transitionStage}: claimed ${batch.count}, trigger=${triggerHash}`,
        )
      } catch (err) {
        core.error(
          `Detect batch ${batch.transitionStage} trigger failed: ${err.message}`,
        )
        await resetClaimedEmails(
          sxtApiUrl,
          accessToken,
          biscuitDml,
          schema,
          batch.transitionStage,
          3,
        )
      }

      // Rate limit: 100ms gap between triggers
      await new Promise((r) => setTimeout(r, 100))
    }

    core.setOutput('success', 'true')
    core.setOutput('dispatched_filter_count', String(dispatchedFilterCount))
    core.setOutput('dispatched_detect_count', String(dispatchedDetectCount))
  } catch (error) {
    core.setOutput('success', 'false')
    core.setOutput('dispatched_filter_count', '0')
    core.setOutput('dispatched_detect_count', '0')
    core.setFailed(error.message)
  }
}
