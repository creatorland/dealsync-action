import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { sanitizeSchema, sanitizeId, sanitizeString, STATUS, saveResults } from '../lib/queries.js'
import { authenticate, executeSql, acquireRateLimitToken } from '../lib/sxt-client.js'
import { callModel, parseAndValidate } from '../lib/ai-client.js'
import { buildPrompt } from '../lib/build-prompt.js'
import { fetchEmails } from '../lib/email-client.js'
import { runPool, insertBatchEvent, sweepStuckRows, sweepOrphanedRows } from '../lib/pipeline.js'
import { WriteBatcher } from '../lib/write-batcher.js'

function toSqlNullable(s) {
  return s ? `'${sanitizeString(s)}'` : 'NULL'
}

/**
 * Orchestrator that claims and processes classify batches concurrently,
 * with in-memory audit passing through save-evals, save-deals,
 * save-deal-contacts, and update-deal-states.
 */
export async function runClassifyPipeline() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const coreSchema = sanitizeSchema(core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING')
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const hyperbolicKey = core.getInput('hyperbolic-key')
  const primaryModel = core.getInput('primary-model') || 'Qwen/Qwen3-235B-A22B-Instruct-2507'
  const fallbackModel = core.getInput('fallback-model') || 'deepseek-ai/DeepSeek-V3'
  const aiApiUrl = core.getInput('ai-api-url') || 'https://api.hyperbolic.xyz/v1/chat/completions'
  const maxConcurrent = parseInt(core.getInput('max-concurrent') || '70', 10)
  const classifyBatchSize = parseInt(core.getInput('classify-batch-size') || '5', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '6', 10)
  const chunkSize = parseInt(core.getInput('chunk-size') || '10', 10)
  const fetchTimeoutMs = parseInt(core.getInput('fetch-timeout-ms') || '120000', 10)
  const flushIntervalMs = parseInt(core.getInput('flush-interval-ms') || '5000', 10)
  const flushThreshold = parseInt(core.getInput('flush-threshold') || '5', 10)

  console.log(
    `[run-classify-pipeline] starting (maxConcurrent=${maxConcurrent}, batchSize=${classifyBatchSize}, maxRetries=${maxRetries}, chunkSize=${chunkSize}, fetchTimeoutMs=${fetchTimeoutMs})`,
  )

  // 1. Authenticate to SxT once at start
  const jwt = await authenticate(authUrl, authSecret)

  // 2. Create bound exec helpers
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)
  const execNoRL = (sql) => executeSql(apiUrl, jwt, biscuit, sql, { skipRateLimit: true })

  // 3. Create write batcher (uses execNoRL — tokens acquired in bulk by workers)
  const batcher = new WriteBatcher(execNoRL, schema, {
    flushIntervalMs,
    flushThreshold,
    coreSchema,
  })

  // =========================================================================
  //  CLAIM FUNCTION (inline, same pattern as claim-classify-batch)
  // =========================================================================

  async function claimBatch() {
    const batchId = uuidv7()

    // Atomic UPDATE for pending_classification threads (thread-aware, NOT EXISTS for pending/filtering)
    const claimSql = `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.CLASSIFYING}', BATCH_ID = '${batchId}', UPDATED_AT = CURRENT_TIMESTAMP WHERE THREAD_ID IN (SELECT DISTINCT ds.THREAD_ID FROM ${schema}.DEAL_STATES ds WHERE ds.STATUS = '${STATUS.PENDING_CLASSIFICATION}' AND NOT EXISTS (SELECT 1 FROM ${schema}.DEAL_STATES ds2 WHERE ds2.THREAD_ID = ds.THREAD_ID AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID AND ds2.STATUS IN ('${STATUS.PENDING}', '${STATUS.FILTERING}')) LIMIT ${classifyBatchSize}) AND STATUS = '${STATUS.PENDING_CLASSIFICATION}'`

    await exec(claimSql)

    // SELECT the claimed rows
    const rows = await exec(
      `SELECT ds.EMAIL_METADATA_ID, ds.MESSAGE_ID, ds.USER_ID, ds.THREAD_ID, ds.SYNC_STATE_ID, ete.AI_SUMMARY AS PREVIOUS_AI_SUMMARY, ete.IS_DEAL AS PREVIOUS_IS_DEAL, uss.EMAIL AS CREATOR_EMAIL FROM ${schema}.DEAL_STATES ds LEFT JOIN ${schema}.EMAIL_THREAD_EVALUATIONS ete ON ete.THREAD_ID = ds.THREAD_ID LEFT JOIN ${schema}.USER_SYNC_SETTINGS uss ON uss.USER_ID = ds.USER_ID WHERE ds.BATCH_ID = '${batchId}'`,
    )

    const count = rows ? rows.length : 0
    console.log(`[run-classify-pipeline] claimed ${count} pending rows`)

    // If claimed > 0, insert batch event and return
    if (count > 0) {
      await insertBatchEvent(exec, schema, {
        triggerHash: batchId,
        batchId,
        batchType: 'classify',
        eventType: 'new',
      })

      return { batch_id: batchId, count, attempts: 0, rows }
    }

    // No pending rows — look for stuck batches (classifying >5min, attempts < maxRetries)
    console.log(`[run-classify-pipeline] no pending rows, checking for stuck batches`)

    const stuckBatches = await exec(
      `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS FROM ${schema}.DEAL_STATES ds LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${STATUS.CLASSIFYING}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${maxRetries} LIMIT 1`,
    )

    if (!stuckBatches || stuckBatches.length === 0) {
      console.log(`[run-classify-pipeline] no stuck batches found, nothing to do`)
      return null
    }

    // Re-claim the stuck batch
    const stuckBatchId = stuckBatches[0].BATCH_ID
    const attempts = parseInt(stuckBatches[0].ATTEMPTS, 10)

    console.log(
      `[run-classify-pipeline] re-claiming stuck batch ${stuckBatchId} (attempts=${attempts})`,
    )

    // SELECT its rows
    const stuckRows = await exec(
      `SELECT ds.EMAIL_METADATA_ID, ds.MESSAGE_ID, ds.USER_ID, ds.THREAD_ID, ds.SYNC_STATE_ID, ete.AI_SUMMARY AS PREVIOUS_AI_SUMMARY, ete.IS_DEAL AS PREVIOUS_IS_DEAL, uss.EMAIL AS CREATOR_EMAIL FROM ${schema}.DEAL_STATES ds LEFT JOIN ${schema}.EMAIL_THREAD_EVALUATIONS ete ON ete.THREAD_ID = ds.THREAD_ID LEFT JOIN ${schema}.USER_SYNC_SETTINGS uss ON uss.USER_ID = ds.USER_ID WHERE ds.BATCH_ID = '${stuckBatchId}'`,
    )

    // UPDATE UPDATED_AT to prevent other instances from grabbing it
    await exec(
      `UPDATE ${schema}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${stuckBatchId}'`,
    )

    // Insert batch event with retrigger type and new trigger hash
    const triggerHash = uuidv7()
    await insertBatchEvent(exec, schema, {
      triggerHash,
      batchId: stuckBatchId,
      batchType: 'classify',
      eventType: 'retrigger',
    })

    const stuckCount = stuckRows ? stuckRows.length : 0

    return { batch_id: stuckBatchId, count: stuckCount, attempts, rows: stuckRows }
  }

  // =========================================================================
  //  WORKER FUNCTION — does ALL steps per batch
  // =========================================================================

  async function processClassifyBatch(batch) {
    const { batch_id: batchId, rows } = batch
    const creatorEmail = rows[0].CREATOR_EMAIL || ''

    console.log(`[run-classify-pipeline] processing batch ${batchId} (${rows.length} rows)`)

    // Acquire rate limit tokens in bulk for this batch attempt
    // ~2 individual SQL calls (audit check + audit insert) — batcher handles the rest
    await acquireRateLimitToken(2)

    // -----------------------------------------------------------------------
    // Step 2: Get or create audit — check for existing audit (retry case)
    // -----------------------------------------------------------------------

    let threads = null

    const existingAudit = await execNoRL(saveResults.getAuditByBatchId(schema, batchId))

    if (existingAudit && existingAudit.length > 0 && existingAudit[0].AI_EVALUATION) {
      try {
        const parsed = JSON.parse(existingAudit[0].AI_EVALUATION)
        threads = parsed.threads || parsed || []
        console.log(
          `[run-classify-pipeline] audit exists for ${batchId} — using cached (${threads.length} threads)`,
        )
      } catch {
        console.log(`[run-classify-pipeline] existing audit has invalid JSON, re-running AI`)
      }
    }

    // -----------------------------------------------------------------------
    // Step 3: AI classification (only if no existing audit)
    // -----------------------------------------------------------------------

    let modelUsed = primaryModel

    if (!threads) {
      // a. Fetch email content via fetchEmails() (NO format param = full content)
      const metaByMessageId = new Map(rows.map((r) => [r.MESSAGE_ID, r]))
      const userId = rows[0].USER_ID
      const syncStateId = rows[0].SYNC_STATE_ID
      const messageIds = rows.map((r) => r.MESSAGE_ID)

      let allEmails
      try {
        allEmails = await fetchEmails(messageIds, metaByMessageId, {
          contentFetcherUrl,
          userId,
          syncStateId,
          chunkSize,
          fetchTimeoutMs,
        })
      } catch {
        allEmails = []
      }

      // Handle unfetchable threads — threads with zero emails returned
      const fetchedThreadIds = new Set(allEmails.map((e) => e.threadId).filter(Boolean))
      const allBatchThreadIds = [...new Set(rows.map((r) => r.THREAD_ID))]
      const unfetchableThreadIds = allBatchThreadIds.filter((tid) => !fetchedThreadIds.has(tid))

      if (unfetchableThreadIds.length > 0) {
        console.log(
          `[run-classify-pipeline] ${unfetchableThreadIds.length} unfetchable threads, checking previous evaluations`,
        )

        // Look up existing evaluations for unfetchable threads
        const quotedUnfetchable = unfetchableThreadIds.map((id) => `'${sanitizeId(id)}'`).join(',')
        const existingEvals = await execNoRL(
          `SELECT THREAD_ID, IS_DEAL FROM ${schema}.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID IN (${quotedUnfetchable})`,
        )
        const evalByThread = {}
        for (const e of existingEvals || []) {
          evalByThread[e.THREAD_ID] = e.IS_DEAL
        }

        // Mark unfetchable rows based on previous eval or default to not_deal
        const unfetchableDealIds = []
        const unfetchableNotDealIds = []
        for (const tid of unfetchableThreadIds) {
          const threadRows = rows.filter((r) => r.THREAD_ID === tid)
          const emailIds = threadRows.map((r) => r.EMAIL_METADATA_ID)
          const wasDeal = evalByThread[tid] === true || evalByThread[tid] === 'true'
          if (wasDeal) {
            unfetchableDealIds.push(...emailIds)
          } else {
            unfetchableNotDealIds.push(...emailIds)
          }
        }

        if (unfetchableDealIds.length > 0) {
          const quotedIds = unfetchableDealIds.map((id) => `'${sanitizeId(id)}'`).join(',')
          await execNoRL(
            `UPDATE ${schema}.DEAL_STATES SET STATUS = 'deal' WHERE EMAIL_METADATA_ID IN (${quotedIds})`,
          )
          console.log(
            `[run-classify-pipeline] ${unfetchableDealIds.length} unfetchable rows → deal (previous eval)`,
          )
        }
        if (unfetchableNotDealIds.length > 0) {
          const quotedIds = unfetchableNotDealIds.map((id) => `'${sanitizeId(id)}'`).join(',')
          await execNoRL(
            `UPDATE ${schema}.DEAL_STATES SET STATUS = 'not_deal' WHERE EMAIL_METADATA_ID IN (${quotedIds})`,
          )
          console.log(
            `[run-classify-pipeline] ${unfetchableNotDealIds.length} unfetchable rows → not_deal`,
          )
        }
      }

      if (allEmails.length === 0) {
        console.log(`[run-classify-pipeline] no fetchable emails, skipping AI`)
        await batcher.pushBatchEvents([
          `('${batchId}', '${batchId}', 'classify', 'complete', CURRENT_TIMESTAMP)`,
        ])
        console.log(`[run-classify-pipeline] batch ${batchId} complete (all unfetchable)`)
        return
      }

      // b. Build prompt via buildPrompt(emails)
      const { systemPrompt, userPrompt, threadOrder } = buildPrompt(allEmails, { creatorEmail })

      // c. 4-layer AI resilience pipeline
      const aiOpts = { apiUrl: aiApiUrl, apiKey: hyperbolicKey }

      const classifyMessages = [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ]

      // --- Layer 0: Primary model call ---
      let primaryRaw
      try {
        const result = await callModel(primaryModel, classifyMessages, {
          temperature: 0,
          ...aiOpts,
        })
        primaryRaw = result.content
      } catch (primaryApiError) {
        console.log(`[run-classify-pipeline] Primary model API failed: ${primaryApiError.message}`)
        primaryRaw = null
      }

      if (primaryRaw) {
        // --- Layer 1: Local JSON repair ---
        try {
          threads = parseAndValidate(primaryRaw, threadOrder)
          console.log(`[run-classify-pipeline] Primary model succeeded: ${threads.length} threads`)
        } catch (parseError) {
          console.log(`[run-classify-pipeline] Primary JSON parse failed: ${parseError.message}`)

          // --- Layer 2: Corrective retry (same model, send broken output back) ---
          try {
            console.log(`[run-classify-pipeline] Attempting corrective retry with ${primaryModel}`)
            const correctiveMessages = [
              ...classifyMessages,
              { role: 'assistant', content: primaryRaw },
              {
                role: 'user',
                content: `Your previous classification response could not be parsed as valid JSON.\n\nParse error:\n${parseError.message}\n\nPlease return the corrected classification as a valid JSON array. Fix only the JSON formatting issue. Do not change any classification decisions. Return ONLY the JSON array with no other text.`,
              },
            ]
            const corrected = await callModel(primaryModel, correctiveMessages, {
              temperature: 0,
              ...aiOpts,
            })
            const correctedRaw = corrected.content
            threads = parseAndValidate(correctedRaw, threadOrder)
            modelUsed = `${primaryModel}(corrective-retry)`
            console.log(
              `[run-classify-pipeline] Corrective retry succeeded: ${threads.length} threads`,
            )
          } catch (correctiveError) {
            console.log(
              `[run-classify-pipeline] Corrective retry failed: ${correctiveError.message}`,
            )
          }
        }
      }

      // --- Layer 3: Fallback model ---
      if (!threads) {
        console.log(`[run-classify-pipeline] Falling back to ${fallbackModel}`)
        modelUsed = fallbackModel
        try {
          const fallbackResult = await callModel(fallbackModel, classifyMessages, {
            temperature: 0.6,
            ...aiOpts,
          })
          const fallbackRaw = fallbackResult.content
          threads = parseAndValidate(fallbackRaw, threadOrder)
          console.log(`[run-classify-pipeline] Fallback model succeeded: ${threads.length} threads`)
        } catch (fallbackError) {
          console.error(
            `[run-classify-pipeline] All layers exhausted. Primary and fallback both failed.`,
          )
          throw new Error(
            `Classification failed: primary and fallback models both returned no valid JSON. Last error: ${fallbackError.message}`,
          )
        }
      }

      // d. Save audit checkpoint
      const auditId = uuidv7()
      const aiOutput = { threads }
      const evaluation = sanitizeString(JSON.stringify(aiOutput))
      try {
        await execNoRL(
          saveResults.insertAudit(schema, {
            id: auditId,
            batchId,
            threadCount: threads.length,
            emailCount: rows.length,
            cost: 0,
            inputTokens: 0,
            outputTokens: 0,
            model: modelUsed,
            evaluation,
          }),
        )
        console.log(`[run-classify-pipeline] audit saved: ${auditId} (model: ${modelUsed})`)
      } catch (err) {
        if (
          err.message.includes('integrity constraint') ||
          err.message.includes('unique') ||
          err.message.includes('duplicate')
        ) {
          console.log(
            `[run-classify-pipeline] audit already exists for batch (concurrent run), continuing`,
          )
        } else {
          throw err
        }
      }
    }

    // -----------------------------------------------------------------------
    // Step 4: Save evals via batcher
    // -----------------------------------------------------------------------

    const evalValues = threads.map((thread) => {
      const threadId = sanitizeId(thread.thread_id)
      return `('${uuidv7()}', '${threadId}', '', '${sanitizeString(thread.category || '')}', '${sanitizeString(thread.ai_summary || '')}', ${thread.is_deal ? 'true' : 'false'}, ${(thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'}, ${typeof thread.ai_score === 'number' ? thread.ai_score : 0}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
    })

    await batcher.pushEvals(evalValues)

    console.log(`[run-classify-pipeline] upserted ${threads.length} thread evaluations`)

    // -----------------------------------------------------------------------
    // Step 5: Save deals via batcher
    // -----------------------------------------------------------------------

    // Build userByThread map from batch rows
    const userByThread = {}
    for (const row of rows) {
      userByThread[row.THREAD_ID] = row.USER_ID
    }

    // Separate deal vs non-deal threads
    const dealThreads = []
    const notDealThreadIds = []

    for (const thread of threads) {
      if (thread.is_deal) {
        dealThreads.push(thread)
      } else {
        notDealThreadIds.push(sanitizeId(thread.thread_id))
      }
    }

    // Batch DELETE non-deal threads via batcher
    if (notDealThreadIds.length > 0) {
      const quoted = notDealThreadIds.map((id) => `'${id}'`)
      await batcher.pushDealDeletes(quoted)
      console.log(`[run-classify-pipeline] deleted ${notDealThreadIds.length} non-deal threads`)
    }

    // Batch upsert deals via batcher
    if (dealThreads.length > 0) {
      const dealValues = dealThreads.map((thread) => {
        const threadId = sanitizeId(thread.thread_id)
        const userId = userByThread[threadId] ? sanitizeId(userByThread[threadId]) : ''
        const dealId = threadId
        const dealName = sanitizeString(thread.deal_name || '')
        const dealType = sanitizeString(thread.deal_type || '')
        const dealValue =
          typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
        const currency = sanitizeString(thread.currency || 'USD')
        const brand = thread.main_contact ? sanitizeString(thread.main_contact.company || '') : ''
        const category = sanitizeString(thread.category || '')
        return `('${dealId}', '${userId}', '${threadId}', '', '${dealName}', '${dealType}', '${category}', ${dealValue}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
      })

      await batcher.pushDeals(dealValues)

      console.log(`[run-classify-pipeline] ${dealThreads.length} deals upserted`)
    }

    // -----------------------------------------------------------------------
    // Step 6: Save deal contacts via batcher (two-table upsert)
    // -----------------------------------------------------------------------

    if (dealThreads.length > 0) {
      const coreContactValues = []
      const dealContactValues = []

      for (const thread of dealThreads) {
        const mc = thread.main_contact
        if (!mc) continue
        const email = (mc.email || '').trim().toLowerCase()
        if (!email) continue

        // Skip creator's own email — AI sometimes ignores the "external only" rule
        if (creatorEmail && email === creatorEmail.trim().toLowerCase()) continue

        const threadId = sanitizeId(thread.thread_id)
        const userId = userByThread[threadId] ? sanitizeId(userByThread[threadId]) : ''
        const contactEmail = sanitizeString(email)
        const nameVal = toSqlNullable(mc.name)
        const companyVal = toSqlNullable(mc.company)
        const titleVal = toSqlNullable(mc.title)
        const phoneVal = toSqlNullable(mc.phone_number)

        // Core contacts — COALESCE preserves existing non-null values
        coreContactValues.push(
          `('${userId}', '${contactEmail}', ${nameVal}, ${companyVal}, ${titleVal}, ${phoneVal}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        )

        // Deal contacts — simplified 4-column relationship upsert
        dealContactValues.push(
          `('${threadId}', '${userId}', '${contactEmail}', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        )
      }

      if (coreContactValues.length > 0) {
        try {
          await batcher.pushCoreContacts(coreContactValues)
        } catch (err) {
          console.error(
            `[run-classify-pipeline] core contacts upsert failed (non-fatal): ${err.message}`,
          )
        }
      }

      if (dealContactValues.length > 0) {
        await batcher.pushContacts(dealContactValues)
      }

      console.log(
        `[run-classify-pipeline] ${dealContactValues.length} contacts saved (core + deal)`,
      )
    }

    // -----------------------------------------------------------------------
    // Step 7: Update deal states to terminal via batcher
    // -----------------------------------------------------------------------

    // Build metadataByThread map from batch rows
    const metadataByThread = {}
    for (const row of rows) {
      if (!metadataByThread[row.THREAD_ID]) metadataByThread[row.THREAD_ID] = []
      metadataByThread[row.THREAD_ID].push(row)
    }

    // Collect dealEmailIds and notDealEmailIds from AI results
    const dealEmailIds = []
    const notDealEmailIds = []
    const classifiedThreadIds = new Set()

    for (const thread of threads) {
      if (!thread.thread_id) continue
      const threadId = sanitizeId(thread.thread_id)
      classifiedThreadIds.add(threadId)
      const threadEmails = metadataByThread[threadId] || []
      if (threadEmails.length === 0) continue

      const emailIds = threadEmails.map((e) => e.EMAIL_METADATA_ID)
      if (thread.is_deal) {
        dealEmailIds.push(...emailIds)
      } else {
        notDealEmailIds.push(...emailIds)
      }
    }

    // Handle rows whose threads weren't in the AI response
    // Use previous evaluation if exists, otherwise default to not_deal
    for (const [threadId, threadRows] of Object.entries(metadataByThread)) {
      if (classifiedThreadIds.has(threadId)) continue
      // Already handled by unfetchable logic earlier
      if (threadRows.every((r) => r.STATUS === 'deal' || r.STATUS === 'not_deal')) continue

      const emailIds = threadRows.map((r) => r.EMAIL_METADATA_ID)
      // Check if any row has a previous eval indicating deal
      const wasDeal = threadRows.some(
        (r) => r.PREVIOUS_IS_DEAL === true || r.PREVIOUS_IS_DEAL === 'true',
      )
      if (wasDeal) {
        dealEmailIds.push(...emailIds)
        console.log(
          `[run-classify-pipeline] unclassified thread ${threadId} → deal (previous eval)`,
        )
      } else {
        notDealEmailIds.push(...emailIds)
        console.log(`[run-classify-pipeline] unclassified thread ${threadId} → not_deal (default)`)
      }
    }

    // Write state updates directly (not through batcher) to ensure they commit
    if (dealEmailIds.length > 0) {
      const quotedIds = dealEmailIds.map((id) => `'${sanitizeId(id)}'`).join(',')
      await execNoRL(
        `UPDATE ${schema}.DEAL_STATES SET STATUS = 'deal' WHERE EMAIL_METADATA_ID IN (${quotedIds})`,
      )
    }
    if (notDealEmailIds.length > 0) {
      const quotedIds = notDealEmailIds.map((id) => `'${sanitizeId(id)}'`).join(',')
      await execNoRL(
        `UPDATE ${schema}.DEAL_STATES SET STATUS = 'not_deal' WHERE EMAIL_METADATA_ID IN (${quotedIds})`,
      )
    }

    console.log(
      `[run-classify-pipeline] states: ${dealEmailIds.length} -> deal, ${notDealEmailIds.length} -> not_deal`,
    )

    // -----------------------------------------------------------------------
    // Step 8: Record completion via batcher
    // -----------------------------------------------------------------------

    await batcher.pushBatchEvents([
      `('${batchId}', '${batchId}', 'classify', 'complete', CURRENT_TIMESTAMP)`,
    ])

    console.log(`[run-classify-pipeline] batch ${batchId} complete`)
  }

  // =========================================================================
  //  RUN POOL
  // =========================================================================

  async function onDeadLetter(batch) {
    const bid = batch.batch_id
    if (!bid) return
    const safeBid = sanitizeId(bid)
    await execNoRL(
      `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FAILED}', UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${safeBid}' AND STATUS = '${STATUS.CLASSIFYING}'`,
    )
    await insertBatchEvent(execNoRL, schema, {
      triggerHash: uuidv7(),
      batchId: bid,
      batchType: 'classify',
      eventType: 'dead_letter',
    })
    console.log(`[run-classify-pipeline] dead-lettered batch ${bid} → status=failed`)
  }

  const poolResults = await runPool(claimBatch, processClassifyBatch, {
    maxConcurrent,
    maxRetries,
    onDeadLetter,
  })

  // Drain all pending writes
  await batcher.drain()

  const stuckFailed = await sweepStuckRows(exec, schema, {
    activeStatus: STATUS.CLASSIFYING,
    batchType: 'classify',
    maxRetries,
  })

  const orphanFailed = await sweepOrphanedRows(exec, schema, {
    statuses: [STATUS.PENDING_CLASSIFICATION],
    staleMinutes: 30,
  })

  console.log(
    `[run-classify-pipeline] done — batches_processed=${poolResults.processed}, batches_failed=${poolResults.failed}, stuck_failed=${stuckFailed}, orphan_failed=${orphanFailed}`,
  )

  return {
    batches_processed: poolResults.processed,
    batches_failed: poolResults.failed,
    stuck_failed: stuckFailed,
    orphan_failed: orphanFailed,
  }
}
