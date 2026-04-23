import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { authenticate, executeSql, acquireRateLimitToken } from '../lib/db.js'
import { callModel, parseAndValidate, buildPrompt } from '../lib/ai.js'
import { fetchEmails, deriveFallbackMainContact, isBlockedSenderAddress } from '../lib/emails.js'
import { runPool, insertBatchEvent, sweepStuckRows, sweepOrphanedRows } from '../lib/pipeline.js'
import { WriteBatcher } from '../lib/batcher.js'
import {
  sanitizeSchema,
  sanitizeId,
  sanitizeString,
  toSqlNullable,
  STATUS,
  dealStates as dealStatesSql,
  audits as auditsSql,
  evaluations as evalSql,
  deals as dealsSql,
  emailSenders as emailSendersSql,
} from '../lib/sql/index.js'

/**
 * Orchestrator that claims and processes classify batches concurrently,
 * with in-memory audit passing through eval upserts, deal upserts,
 * contact inserts, and terminal state updates.
 */
export async function runClassifyPipeline() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const schema = sanitizeSchema(core.getInput('sxt-schema'))
  const coreSchema = sanitizeSchema(core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING')
  const contentFetcherUrl = core.getInput('email-content-fetcher-url')
  const emailProvider = core.getInput('email-provider') || ''
  const emailServiceUrl = core.getInput('email-service-url')
  const hyperbolicKey = core.getInput('ai-api-key')
  const primaryModel = core.getInput('ai-primary-model') || ''
  const fallbackModel = core.getInput('ai-fallback-model') || ''
  const aiApiUrl = core.getInput('ai-api-url') || ''
  const maxConcurrent = parseInt(core.getInput('pipeline-classify-max-concurrent') || core.getInput('pipeline-max-concurrent') || '70', 10)
  const classifyBatchSize = parseInt(core.getInput('pipeline-classify-batch-size') || '5', 10)
  const claimSize = parseInt(core.getInput('pipeline-claim-size') || '5', 10)
  const maxRetries = parseInt(core.getInput('pipeline-max-retries') || '6', 10)
  const fetchChunkSize = parseInt(core.getInput('pipeline-fetch-chunk-size') || '10', 10)
  const fetchTimeoutMs = parseInt(core.getInput('pipeline-fetch-timeout-ms') || '120000', 10)
  const flushIntervalMs = parseInt(core.getInput('pipeline-flush-interval-ms') || '5000', 10)
  const flushThreshold = parseInt(core.getInput('pipeline-flush-threshold') || '5', 10)

  console.log(
    `[run-classify-pipeline] starting (maxConcurrent=${maxConcurrent}, batchSize=${classifyBatchSize}, claimSize=${claimSize}, maxRetries=${maxRetries}, fetchChunkSize=${fetchChunkSize}, fetchTimeoutMs=${fetchTimeoutMs})`,
  )
  console.log(
    `[run-classify-pipeline] config: primaryModel=${primaryModel}, fallbackModel=${fallbackModel}, aiApiUrl=${aiApiUrl ? aiApiUrl.substring(0, 40) : '(empty)'}, schema=${schema}, emailProvider=${emailProvider || '(empty)'}, apiKey=${hyperbolicKey ? hyperbolicKey.substring(0, 10) + '...' : '(empty)'}`,
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

  /**
   * Shared mega-split logic: group rows by THREAD_ID, chunk into sub-batches
   * of classifyBatchSize threads, restamp in DB, push batch events.
   * Returns an array of sub-batch objects for runPool.
   */
  async function megaSplit(megaBatchId, allRows, attempts) {
    // Group rows by THREAD_ID
    const rowsByThread = {}
    for (const row of allRows) {
      if (!rowsByThread[row.THREAD_ID]) rowsByThread[row.THREAD_ID] = []
      rowsByThread[row.THREAD_ID].push(row)
    }
    const threadIds = Object.keys(rowsByThread)

    // Chunk thread groups into groups of classifyBatchSize
    const chunks = []
    for (let i = 0; i < threadIds.length; i += classifyBatchSize) {
      chunks.push(threadIds.slice(i, i + classifyBatchSize))
    }

    // Generate sub-batch IDs and build restamp groups
    const groups = chunks.map((chunkThreadIds) => ({
      subBatchId: uuidv7(),
      threadIds: chunkThreadIds,
    }))

    // Restamp sub-batches in DB
    await exec(dealStatesSql.restampSubBatches(schema, megaBatchId, groups))

    // Insert batch events for each sub-batch
    const batchEventValues = groups.map(
      ({ subBatchId }) =>
        `('${subBatchId}', '${subBatchId}', 'classify', 'new', CURRENT_TIMESTAMP)`,
    )
    await batcher.pushBatchEvents(batchEventValues)

    // Build sub-batch objects
    const subBatches = groups.map(({ subBatchId, threadIds: tids }) => {
      const subRows = tids.flatMap((tid) => rowsByThread[tid])
      return { batch_id: subBatchId, count: subRows.length, attempts, rows: subRows }
    })

    console.log(
      `[run-classify-pipeline] mega-split ${megaBatchId}: ${allRows.length} rows → ${subBatches.length} sub-batches (${threadIds.length} threads)`,
    )

    return subBatches
  }

  async function claimBatch() {
    const claimStart = Date.now()
    const useMegaClaim = claimSize > classifyBatchSize

    if (useMegaClaim) {
      // --- Mega-claim path ---
      const megaId = uuidv7()
      const megaBatchId = `mega:${megaId}`

      await exec(dealStatesSql.claimClassifyBatch(schema, megaBatchId, claimSize))

      const rows = await exec(dealStatesSql.selectEmailsWithEvalAndCreator(schema, megaBatchId))

      const count = rows ? rows.length : 0
      console.log(`[run-classify-pipeline] mega-claimed ${count} pending rows in ${Date.now() - claimStart}ms`)

      if (count > 0) {
        return await megaSplit(megaBatchId, rows, 0)
      }
    } else {
      // --- Standard single-batch claim path ---
      const batchId = uuidv7()

      await exec(dealStatesSql.claimClassifyBatch(schema, batchId, classifyBatchSize))

      const rows = await exec(dealStatesSql.selectEmailsWithEvalAndCreator(schema, batchId))

      const count = rows ? rows.length : 0
      console.log(`[run-classify-pipeline] claimed ${count} pending rows in ${Date.now() - claimStart}ms`)

      if (count > 0) {
        await insertBatchEvent(exec, schema, {
          triggerHash: batchId,
          batchId,
          batchType: 'classify',
          eventType: 'new',
        })

        return { batch_id: batchId, count, attempts: 0, rows }
      }
    }

    // No pending rows — look for stuck batches (classifying >5min, attempts < maxRetries)
    console.log(`[run-classify-pipeline] no pending rows, checking for stuck batches`)

    const stuckBatches = await exec(
      dealStatesSql.findStuckBatches(schema, STATUS.CLASSIFYING, 5, maxRetries),
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

    // Check if this is a stuck mega-batch
    if (stuckBatchId.startsWith('mega:')) {
      // SELECT all rows for the mega batch
      const stuckRows = await exec(dealStatesSql.selectEmailsWithEvalAndCreator(schema, stuckBatchId))

      // UPDATE UPDATED_AT to prevent other instances from grabbing it
      await exec(dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId))

      const stuckCount = stuckRows ? stuckRows.length : 0
      if (stuckCount > 0) {
        return await megaSplit(stuckBatchId, stuckRows, attempts)
      }
      return null
    }

    // Standard stuck batch recovery
    const stuckRows = await exec(dealStatesSql.selectEmailsWithEvalAndCreator(schema, stuckBatchId))

    // UPDATE UPDATED_AT to prevent other instances from grabbing it
    await exec(dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId))

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
    const alreadyEvaluatedThreadIds = new Set()
    const batchStart = Date.now()
    const timings = {}

    console.log(`[run-classify-pipeline] processing batch ${batchId} (${rows.length} rows)`)

    // Acquire rate limit tokens in bulk for this batch attempt
    // ~2 individual SQL calls (audit check + audit insert) — batcher handles the rest
    let t0 = Date.now()
    await acquireRateLimitToken(2)
    timings.rateLimit = Date.now() - t0

    // -----------------------------------------------------------------------
    // Step 2: Get or create audit — check for existing audit (retry case)
    // -----------------------------------------------------------------------

    let threads = null
    // Hoisted so Step 6 can derive a main_contact fallback from the thread's
    // latest external sender when the AI did not return one. Remains null on
    // cached-audit retries — Step 6 falls back to a DB lookup in that case.
    let allEmails = null

    t0 = Date.now()
    const existingAudit = await execNoRL(auditsSql.selectByBatch(schema, batchId))

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

    timings.auditCheck = Date.now() - t0

    if (!threads) {
      // a. Fetch email content via fetchEmails() (NO format param = full content)
      const metaByMessageId = new Map(rows.map((r) => [r.MESSAGE_ID, r]))
      const userId = rows[0].USER_ID
      const syncStateId = rows[0].SYNC_STATE_ID
      const messageIds = rows.map((r) => r.MESSAGE_ID)

      t0 = Date.now()
      try {
        allEmails = await fetchEmails(messageIds, metaByMessageId, {
          contentFetcherUrl,
          emailProvider,
          emailServiceUrl,
          userId,
          syncStateId,
          chunkSize: fetchChunkSize,
          fetchTimeoutMs,
        })
      } catch {
        allEmails = []
      }
      timings.fetch = Date.now() - t0
      console.log(
        `[run-classify-pipeline] batch ${batchId}: fetched ${allEmails.length}/${messageIds.length} emails in ${timings.fetch}ms`,
      )

      // Handle unfetchable threads — threads with zero emails returned
      const fetchedThreadIds = new Set(allEmails.map((e) => e.threadId).filter(Boolean))
      const allBatchThreadIds = [...new Set(rows.map((r) => r.THREAD_ID))]
      const unfetchableThreadIds = allBatchThreadIds.filter((tid) => !fetchedThreadIds.has(tid))

      if (unfetchableThreadIds.length > 0) {
        console.log(
          `[run-classify-pipeline] ${unfetchableThreadIds.length} unfetchable threads, checking previous evaluations`,
        )

        // Look up existing evaluations for unfetchable threads
        const quotedUnfetchable = unfetchableThreadIds.map((id) => `'${sanitizeId(id)}'`)
        const existingEvals = await execNoRL(evalSql.selectByThreadIds(schema, quotedUnfetchable))
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
          const quotedDealIds = unfetchableDealIds.map((id) => `'${sanitizeId(id)}'`)
          await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedDealIds, STATUS.DEAL))
          console.log(
            `[run-classify-pipeline] ${unfetchableDealIds.length} unfetchable rows → deal (previous eval)`,
          )
        }
        if (unfetchableNotDealIds.length > 0) {
          const quotedNotDealIds = unfetchableNotDealIds.map((id) => `'${sanitizeId(id)}'`)
          await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedNotDealIds, STATUS.NOT_DEAL))
          console.log(
            `[run-classify-pipeline] ${unfetchableNotDealIds.length} unfetchable rows → not_deal`,
          )
        }
      }

      // ---------------------------------------------------------------
      // Already-evaluated skip: threads with existing deals + no newer emails
      // ---------------------------------------------------------------

      const remainingThreadIds = [...new Set(allEmails.map((e) => e.threadId).filter(Boolean))]

      if (remainingThreadIds.length > 0) {
        const quotedFetched = remainingThreadIds.map((id) => `'${sanitizeId(id)}'`)
        const existingDeals = await execNoRL(dealsSql.selectByThreadIds(schema, quotedFetched))

        if (existingDeals && existingDeals.length > 0) {
          const dealByThread = {}
          for (const d of existingDeals) {
            dealByThread[d.THREAD_ID] = d.UPDATED_AT
          }

          // Group emails by thread and find latest date per thread
          const emailsByThread = {}
          for (const email of allEmails) {
            if (!email.threadId) continue
            if (!emailsByThread[email.threadId]) emailsByThread[email.threadId] = []
            emailsByThread[email.threadId].push(email)
          }

          const skippedEmailIds = []
          const skippedThreadIds = []

          for (const [threadId, dealUpdatedAt] of Object.entries(dealByThread)) {
            const threadEmails = emailsByThread[threadId]
            if (!threadEmails || threadEmails.length === 0) continue

            const emailDates = threadEmails
              .map((e) => new Date(e.date))
              .filter((d) => !isNaN(d.getTime()))

            // No valid dates — can't determine, classify normally
            if (emailDates.length === 0) continue

            const latestEmailDate = emailDates.reduce(
              (latest, d) => (d > latest ? d : latest),
              new Date(0),
            )

            if (latestEmailDate <= new Date(dealUpdatedAt)) {
              // All emails are older than the deal — skip classification
              alreadyEvaluatedThreadIds.add(threadId)
              skippedThreadIds.push(threadId)
              const threadRows = rows.filter((r) => r.THREAD_ID === threadId)
              skippedEmailIds.push(...threadRows.map((r) => r.EMAIL_METADATA_ID))
              // Remove these emails from allEmails so they don't go to AI
              allEmails = allEmails.filter((e) => e.threadId !== threadId)
            }
          }

          if (skippedEmailIds.length > 0) {
            const quotedSkipped = skippedEmailIds.map((id) => `'${sanitizeId(id)}'`)
            await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedSkipped, STATUS.DEAL))
            console.log(
              `[run-classify-pipeline] ${skippedEmailIds.length} rows skipped → deal (already evaluated, ${skippedThreadIds.length} threads)`,
            )
          }
        }
      }

      if (allEmails.length === 0) {
        console.log(`[run-classify-pipeline] no emails to classify, skipping AI`)
        await batcher.pushBatchEvents([
          `('${batchId}', '${batchId}', 'classify', 'complete', CURRENT_TIMESTAMP)`,
        ])
        console.log(`[run-classify-pipeline] batch ${batchId} complete (no emails to classify)`)
        return
      }

      // b. Build prompt via buildPrompt(emails)
      t0 = Date.now()
      const { systemPrompt, userPrompt, threadOrder } = buildPrompt(allEmails, { creatorEmail, model: primaryModel })

      // c. 4-layer AI resilience pipeline
      const aiOpts = { apiUrl: aiApiUrl, apiKey: hyperbolicKey }

      const classifyMessages = [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ]

      // --- Layer 0: Primary model call ---
      let primaryRaw
      const aiStart = Date.now()
      try {
        const result = await callModel(primaryModel, classifyMessages, {
          temperature: 0,
          ...aiOpts,
        })
        primaryRaw = result.content
        timings.primaryModel = Date.now() - aiStart
        console.log(`[run-classify-pipeline] batch ${batchId}: primary model responded in ${timings.primaryModel}ms`)
      } catch (primaryApiError) {
        timings.primaryModel = Date.now() - aiStart
        console.log(`[run-classify-pipeline] Primary model API failed after ${timings.primaryModel}ms: ${primaryApiError.message}`)
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
            const correctiveStart = Date.now()
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
            timings.correctiveRetry = Date.now() - correctiveStart
            console.log(
              `[run-classify-pipeline] Corrective retry succeeded in ${timings.correctiveRetry}ms: ${threads.length} threads`,
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
        const fallbackStart = Date.now()
        console.log(`[run-classify-pipeline] Falling back to ${fallbackModel}`)
        modelUsed = fallbackModel
        try {
          const fallbackResult = await callModel(fallbackModel, classifyMessages, {
            temperature: 0.6,
            ...aiOpts,
          })
          const fallbackRaw = fallbackResult.content
          threads = parseAndValidate(fallbackRaw, threadOrder)
          timings.fallbackModel = Date.now() - fallbackStart
          console.log(`[run-classify-pipeline] Fallback model succeeded in ${timings.fallbackModel}ms: ${threads.length} threads`)
        } catch (fallbackError) {
          console.error(
            `[run-classify-pipeline] All layers exhausted. Primary and fallback both failed.`,
          )
          throw new Error(
            `Classification failed: primary and fallback models both returned no valid JSON. Last error: ${fallbackError.message}`,
          )
        }
      }

      timings.aiTotal = Date.now() - t0

      // d. Save audit checkpoint
      t0 = Date.now()
      const auditId = uuidv7()
      const aiOutput = { threads }
      const evaluation = sanitizeString(JSON.stringify(aiOutput))
      try {
        await execNoRL(
          auditsSql.insert(schema, {
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
        timings.auditSave = Date.now() - t0
        console.log(`[run-classify-pipeline] audit saved: ${auditId} (model: ${modelUsed}) in ${timings.auditSave}ms`)
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

    t0 = Date.now()
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
      // ---------------------------------------------------------------------
      // Step 6a: main_contact fallback — derive from the thread's latest
      // external sender when the AI returned a null/unusable main_contact.
      // A "usable" contact still must be: non-empty email, not the creator,
      // and not a blocked/automated sender (mailer-daemon, no-reply, etc.).
      // ---------------------------------------------------------------------

      const creatorLower = (creatorEmail || '').trim().toLowerCase()
      const isUsableContact = (mc) => {
        if (!mc) return false
        const email = (mc.email || '').trim().toLowerCase()
        if (!email) return false
        if (creatorLower && email === creatorLower) return false
        if (isBlockedSenderAddress(email)) return false
        return true
      }

      const fallbackCandidates = dealThreads.filter((t) => !isUsableContact(t.main_contact))

      if (fallbackCandidates.length > 0) {
        // Pass 1: use fetched email bodies (fresh-classification path).
        if (Array.isArray(allEmails) && allEmails.length > 0) {
          const emailsByThread = {}
          for (const email of allEmails) {
            if (!email.threadId) continue
            if (!emailsByThread[email.threadId]) emailsByThread[email.threadId] = []
            emailsByThread[email.threadId].push(email)
          }

          for (const thread of fallbackCandidates) {
            if (isUsableContact(thread.main_contact)) continue
            const threadEmails = emailsByThread[thread.thread_id]
            const derived = deriveFallbackMainContact(threadEmails, creatorEmail)
            if (derived && isUsableContact(derived)) thread.main_contact = derived
          }
        }

        // Pass 2: DB lookup via EMAIL_SENDERS for threads still missing a
        // contact — covers the cached-audit retry path where allEmails is null.
        // One query per thread so a global LIMIT + ORDER BY RECEIVED_AT DESC cannot
        // starve quieter threads when another thread dominates the newest rows.
        // Queries run in parallel; per-thread try/catch keeps one failure from aborting the rest.
        const stillMissing = fallbackCandidates.filter((t) => !isUsableContact(t.main_contact))
        if (stillMissing.length > 0) {
          const resolveOne = async (thread) => {
            const tid = thread.thread_id
            try {
              const threadKey = sanitizeId(String(tid))
              const batchUserId = userByThread[threadKey]
              if (!batchUserId) {
                console.error(
                  `[run-classify-pipeline] main_contact DB fallback skipped for thread ${tid}: no USER_ID on claimed batch rows`,
                )
                return
              }
              const senderRows = await execNoRL(
                emailSendersSql.selectForThreadUser(coreSchema, threadKey, batchUserId),
              )
              for (const row of senderRows || []) {
                const addr = (row.SENDER_EMAIL || '').trim().toLowerCase()
                if (!addr) continue
                if (creatorLower && addr === creatorLower) continue
                if (isBlockedSenderAddress(addr)) continue
                const name = (row.SENDER_NAME || '').trim()
                thread.main_contact = {
                  email: addr,
                  name: name || null,
                  company: null,
                  title: null,
                  phone_number: null,
                }
                return
              }
            } catch (err) {
              console.error(
                `[run-classify-pipeline] main_contact DB fallback for thread ${tid} failed (non-fatal): ${err.message}`,
              )
            }
          }
          await Promise.all(stillMissing.map(resolveOne))
        }

        const recovered = fallbackCandidates.filter((t) => isUsableContact(t.main_contact)).length
        console.log(
          `[run-classify-pipeline] main_contact fallback: ${recovered}/${fallbackCandidates.length} recovered (${fallbackCandidates.length - recovered} still unresolved)`,
        )
      }

      const coreContactValues = []
      const dealContactValues = []

      for (const thread of dealThreads) {
        const mc = thread.main_contact
        // Same bar as Step 6a (creator, blocked/automated senders, empty email)
        if (!isUsableContact(mc)) continue

        const email = (mc.email || '').trim().toLowerCase()

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
      if (alreadyEvaluatedThreadIds.has(threadId)) continue
      // Already handled by unfetchable logic earlier
      if (threadRows.every((r) => r.STATUS === STATUS.DEAL || r.STATUS === STATUS.NOT_DEAL))
        continue

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
      const quotedIds = dealEmailIds.map((id) => `'${sanitizeId(id)}'`)
      await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedIds, STATUS.DEAL))
    }
    if (notDealEmailIds.length > 0) {
      const quotedNDIds = notDealEmailIds.map((id) => `'${sanitizeId(id)}'`)
      await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedNDIds, STATUS.NOT_DEAL))
    }

    timings.dbWrites = Date.now() - t0

    console.log(
      `[run-classify-pipeline] states: ${dealEmailIds.length} -> deal, ${notDealEmailIds.length} -> not_deal`,
    )

    // -----------------------------------------------------------------------
    // Step 8: Record completion via batcher
    // -----------------------------------------------------------------------

    await batcher.pushBatchEvents([
      `('${batchId}', '${batchId}', 'classify', 'complete', CURRENT_TIMESTAMP)`,
    ])

    timings.total = Date.now() - batchStart
    console.log(
      `[run-classify-pipeline] batch ${batchId} complete — timings: ${JSON.stringify(timings)}`,
    )
  }

  // =========================================================================
  //  RUN POOL
  // =========================================================================

  async function onDeadLetter(batch) {
    const bid = batch.batch_id
    if (!bid) return
    const safeBid = sanitizeId(bid)
    await execNoRL(
      dealStatesSql.updateStatusByBatch(schema, safeBid, STATUS.CLASSIFYING, STATUS.FAILED),
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
