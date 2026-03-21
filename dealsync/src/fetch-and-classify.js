import * as crypto from 'crypto'
import * as core from '@actions/core'
import { buildPrompt } from './build-prompt.js'
import {
  saveResults,
  detection,
  processor,
  STATUS,
  sanitizeId,
  sanitizeString,
  sanitizeSchema,
  toSqlIdList,
} from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

function resolveStatus(thread) {
  if (thread.is_deal) return STATUS.DEAL
  return STATUS.NOT_DEAL
}

/**
 * Combined fetch-content + build-prompt + AI call + classify + save command.
 *
 * Does everything in one step — no data passing between W3 steps.
 * Inputs: batch-id, SxT auth, content-fetcher-url, AI config
 */
export async function runFetchAndClassify() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = core.getInput('batch-id')
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const hyperbolicKey = core.getInput('hyperbolic-key')
  const primaryModel = core.getInput('primary-model') || 'deepseek-ai/DeepSeek-V3'
  const fallbackModel = core.getInput('fallback-model') || 'Qwen/Qwen2.5-72B-Instruct'
  const aiApiUrl = core.getInput('ai-api-url') || 'https://api.hyperbolic.xyz/v1/chat/completions'

  if (!batchId) throw new Error('batch-id is required')

  console.log(`[fetch-and-classify] starting for batch ${batchId}`)

  // 1. Authenticate + fetch metadata from SxT
  const jwt = await authenticate(authUrl, authSecret)
  const metadataRows = await executeSql(
    apiUrl, jwt, biscuit,
    `SELECT ds.EMAIL_METADATA_ID, ds.MESSAGE_ID, ds.USER_ID, ds.THREAD_ID, ds.SYNC_STATE_ID,
      latest_eval.AI_SUMMARY AS PREVIOUS_AI_SUMMARY,
      d.ID AS EXISTING_DEAL_ID
    FROM ${schema}.DEAL_STATES ds
    LEFT JOIN (
      SELECT THREAD_ID, AI_SUMMARY,
        ROW_NUMBER() OVER (PARTITION BY THREAD_ID ORDER BY UPDATED_AT DESC) AS RN
      FROM ${schema}.EMAIL_THREAD_EVALUATIONS
    ) latest_eval ON latest_eval.THREAD_ID = ds.THREAD_ID AND latest_eval.RN = 1
    LEFT JOIN ${schema}.DEALS d ON d.THREAD_ID = ds.THREAD_ID AND d.USER_ID = ds.USER_ID
    WHERE ds.BATCH_ID = '${batchId}'`,
  )

  if (!metadataRows || metadataRows.length === 0) {
    console.log('[fetch-and-classify] no rows found for batch')
    return { deals_created: 0, emails_classified: 0 }
  }

  console.log(`[fetch-and-classify] found ${metadataRows.length} deal_states`)

  // 2. Increment attempts
  await executeSql(apiUrl, jwt, biscuit, processor.incrementAttempts(schema, batchId))

  // 3. Fetch full email content
  const userId = metadataRows[0].USER_ID
  const syncStateId = metadataRows[0].SYNC_STATE_ID
  const messageIds = metadataRows.map((r) => r.MESSAGE_ID)

  const MAX_PER_BATCH = 50
  const allEmails = []

  for (let i = 0; i < messageIds.length; i += MAX_PER_BATCH) {
    const chunk = messageIds.slice(i, i + MAX_PER_BATCH)
    try {
      const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, syncStateId, messageIds: chunk }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const result = await resp.json()
      const emails = result.data || result

      for (const email of emails) {
        const meta = metadataRows.find((r) => r.MESSAGE_ID === email.messageId)
        if (meta) {
          email.id = meta.EMAIL_METADATA_ID
          email.threadId = meta.THREAD_ID
          if (meta.PREVIOUS_AI_SUMMARY) email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
          if (meta.EXISTING_DEAL_ID) email.existingDealId = meta.EXISTING_DEAL_ID
        }
        allEmails.push(email)
      }
    } catch (err) {
      console.log(`[fetch-and-classify] content fetch failed: ${err.message}`)
    }
  }

  if (allEmails.length === 0) {
    throw new Error('No email content fetched')
  }

  console.log(`[fetch-and-classify] fetched ${allEmails.length} emails, building prompt`)

  // 4. Build AI prompt
  const { systemPrompt, userPrompt } = buildPrompt(allEmails)

  // 5. Call AI (primary + fallback)
  let aiResponseRaw = null

  for (const model of [primaryModel, fallbackModel]) {
    try {
      console.log(`[fetch-and-classify] calling AI: ${model}`)
      const resp = await fetch(aiApiUrl, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${hyperbolicKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userPrompt },
          ],
          response_format: { type: 'json_object' },
        }),
      })

      if (!resp.ok) {
        const body = await resp.text()
        console.log(`[fetch-and-classify] AI ${model} failed: HTTP ${resp.status} ${body.substring(0, 200)}`)
        continue
      }

      const result = await resp.json()
      aiResponseRaw = result.choices?.[0]?.message?.content
      if (aiResponseRaw) {
        console.log(`[fetch-and-classify] AI ${model} responded (${aiResponseRaw.length} chars)`)
        break
      }
    } catch (err) {
      console.log(`[fetch-and-classify] AI ${model} error: ${err.message}`)
    }
  }

  if (!aiResponseRaw) {
    throw new Error('Both AI models failed to respond')
  }

  // 6. Parse AI response (strip markdown fences)
  aiResponseRaw = aiResponseRaw.replace(/^```(?:json)?\s*\n?/i, '').replace(/\n?```\s*$/i, '')
  const aiOutput = JSON.parse(aiResponseRaw)

  // 7. Save results — same logic as classify.js
  const metadataByThread = {}
  for (const row of metadataRows) {
    const tid = row.THREAD_ID
    if (!metadataByThread[tid]) metadataByThread[tid] = []
    metadataByThread[tid].push(row)
  }

  const threads = aiOutput.threads || []
  let dealsCreated = 0
  let emailsClassified = 0
  let failedThreads = 0
  const dealIdList = []
  const notDealIdList = []

  for (const thread of threads) {
    try {
      const threadId = sanitizeId(thread.thread_id)
      const threadEmails = metadataByThread[threadId] || []
      const emailCount = threadEmails.length
      const threadUserId = threadEmails.length > 0 ? sanitizeId(threadEmails[0].USER_ID) : ''

      // a. INSERT AI_EVALUATION_AUDITS
      const auditId = crypto.randomUUID()
      const rawJson = sanitizeString(JSON.stringify(thread).substring(0, 6400))
      await executeSql(apiUrl, jwt, biscuit,
        saveResults.insertAudit(schema, {
          id: auditId, threadCount: threads.length, emailCount,
          cost: 0, inputTokens: 0, outputTokens: 0, model: '', evaluation: rawJson,
        }))

      // b. DELETE + INSERT EMAIL_THREAD_EVALUATIONS
      const evalId = crypto.randomUUID()
      const category = sanitizeString(thread.category || '')
      const aiSummary = sanitizeString(thread.ai_summary || '')
      const isDeal = thread.is_deal ? 'true' : 'false'
      const isLikelyScam = (thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'
      const aiScore = typeof thread.ai_score === 'number' ? thread.ai_score : 0

      await executeSql(apiUrl, jwt, biscuit, saveResults.deleteThreadEvaluation(schema, threadId))
      await executeSql(apiUrl, jwt, biscuit,
        saveResults.insertThreadEvaluation(schema, {
          id: evalId, threadId, auditId, category, summary: aiSummary,
          isDeal, likelyScam: isLikelyScam, score: aiScore,
        }))

      // c. If is_deal — create deal + deal_contact
      if (thread.is_deal) {
        const dealId = crypto.randomUUID()
        const dealName = sanitizeString(thread.deal_name || '')
        const dealType = sanitizeString(thread.deal_type || '')
        const dealValue = typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
        const currency = sanitizeString(thread.currency || 'USD')
        const contactEmail = thread.main_contact ? sanitizeString(thread.main_contact.email || '') : ''
        const brand = thread.main_contact ? sanitizeString(thread.main_contact.company || '') : ''

        await executeSql(apiUrl, jwt, biscuit, saveResults.deleteDeal(schema, threadId))
        await executeSql(apiUrl, jwt, biscuit,
          saveResults.insertDeal(schema, {
            id: dealId, userId: threadUserId, threadId, evalId, dealName, dealType,
            category, value: dealValue, currency, brand,
          }))

        if (contactEmail) {
          await executeSql(apiUrl, jwt, biscuit, saveResults.deleteDealContact(schema, dealId))
          await executeSql(apiUrl, jwt, biscuit,
            saveResults.insertDealContact(schema, { id: crypto.randomUUID(), dealId, contactEmail }))
        }

        dealsCreated++
      }

      // d. Update DEAL_STATES status
      if (threadEmails.length > 0) {
        const newStatus = resolveStatus(thread)
        const emailIds = threadEmails.map((e) => e.EMAIL_METADATA_ID)
        const sqlQuotedIds = toSqlIdList(emailIds)

        if (newStatus === STATUS.DEAL) {
          await executeSql(apiUrl, jwt, biscuit, detection.updateDeals(schema, sqlQuotedIds))
          dealIdList.push(...emailIds)
        } else {
          await executeSql(apiUrl, jwt, biscuit, detection.updateNotDeal(schema, sqlQuotedIds))
          notDealIdList.push(...emailIds)
        }
        emailsClassified += threadEmails.length
      }
    } catch (err) {
      failedThreads++
      core.error(`Failed to process thread ${thread.thread_id}: ${err.message}`)
    }
  }

  if (failedThreads > 0 && emailsClassified === 0) {
    throw new Error(`All ${failedThreads} thread(s) failed to classify`)
  }

  console.log(`[fetch-and-classify] done: ${dealsCreated} deals, ${emailsClassified} classified, ${failedThreads} failed`)

  return {
    deals_created: dealsCreated,
    emails_classified: emailsClassified,
    failed_threads: failedThreads,
    deal_ids: dealIdList.length > 0 ? toSqlIdList(dealIdList) : '',
    not_deal_ids: notDealIdList.length > 0 ? toSqlIdList(notDealIdList) : '',
  }
}
