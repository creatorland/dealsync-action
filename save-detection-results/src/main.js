import * as crypto from 'crypto'

import * as core from '@actions/core'

import { decryptValue, tryDecrypt } from '../../shared/crypto.js'
import {
  saveResults,
  detection,
  sanitizeId,
  sanitizeString,
  sanitizeSchema,
  toSqlIdList,
} from '../../shared/queries.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

async function authenticate(sxtApiUrl, authUrl, authSecret) {
  const resp = await fetch(authUrl, {
    method: 'GET',
    headers: { 'x-shared-secret': authSecret },
  })
  if (!resp.ok) throw new Error(`Auth failed: ${resp.status}`)
  const data = await resp.json()
  return data.data || data.accessToken || data
}

function decryptInput(value, encryptionKey) {
  if (!encryptionKey) return value
  const decrypted = tryDecrypt(value, encryptionKey)
  return decrypted !== null ? decrypted : value
}

function resolveStage(thread) {
  if (thread.language && thread.language.toLowerCase() !== 'en') return 107
  if (thread.is_deal) return 4
  return 106
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

export async function run() {
  try {
    const encryptionKey = core.getInput('encryption-key')

    const sxtApiUrl = core.getInput('sxt-api-url')
    const schema = sanitizeSchema(core.getInput('sxt-schema'))
    const authUrl = core.getInput('sxt-auth-url')
    const authSecret = decryptInput(core.getInput('sxt-auth-secret'), encryptionKey)

    const aiOutputRaw = decryptInput(core.getInput('ai-output'), encryptionKey)
    const aiModel = sanitizeString(core.getInput('ai-model'))
    const aiPromptTokens = parseInt(core.getInput('ai-prompt-tokens'), 10) || 0
    const aiCompletionTokens = parseInt(core.getInput('ai-completion-tokens'), 10) || 0
    const metadataRaw = decryptInput(core.getInput('metadata'), encryptionKey)

    const aiOutput = JSON.parse(aiOutputRaw)
    const metadata = JSON.parse(metadataRaw)

    const metadataByThread = {}
    for (const row of metadata) {
      const tid = row.THREAD_ID
      if (!metadataByThread[tid]) metadataByThread[tid] = []
      metadataByThread[tid].push(row)
    }

    const threads = aiOutput.threads || []
    if (threads.length === 0) {
      core.info('No threads to process')
      core.setOutput('success', 'true')
      core.setOutput('deals_created', '0')
      core.setOutput('emails_classified', '0')
      return
    }

    // Auth via proxy
    const accessToken = await authenticate(sxtApiUrl, authUrl, authSecret)
    // TODO: generate biscuit with sxt-nodejs-sdk instead of fetching pre-generated
    // For now this action needs to be refactored to use sxt-nodejs-sdk like dispatch-batches
    const biscuit = '' // placeholder — will be addressed when this action is deployed

    let dealsCreated = 0
    let emailsClassified = 0

    const totalTokens = aiPromptTokens + aiCompletionTokens
    const inferenceCost = (totalTokens / 1000) * 0.001

    for (const thread of threads) {
      try {
        const threadId = sanitizeId(thread.thread_id)
        const threadEmails = metadataByThread[threadId] || []
        const emailCount = threadEmails.length
        const userId = threadEmails.length > 0 ? sanitizeId(threadEmails[0].USER_ID) : ''

        // a. INSERT AI_EVALUATION_AUDITS
        const auditId = crypto.randomUUID()
        const rawJson = sanitizeString(JSON.stringify(thread).substring(0, 6400))
        await executeSql(sxtApiUrl, accessToken, biscuit,
          saveResults.insertAudit(schema, {
            id: auditId,
            threadCount: threads.length,
            emailCount,
            cost: inferenceCost,
            inputTokens: aiPromptTokens,
            outputTokens: aiCompletionTokens,
            model: aiModel,
            evaluation: rawJson,
          }))

        // b. DELETE + INSERT EMAIL_THREAD_EVALUATIONS
        const evalId = crypto.randomUUID()
        const category = sanitizeString(thread.category || '')
        const aiSummary = sanitizeString(thread.ai_summary || '')
        const isDeal = thread.is_deal ? 'true' : 'false'
        const isLikelyScam = (thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'
        const aiScore = typeof thread.ai_score === 'number' ? thread.ai_score : 0

        await executeSql(sxtApiUrl, accessToken, biscuit,
          saveResults.deleteThreadEvaluation(schema, threadId))
        await executeSql(sxtApiUrl, accessToken, biscuit,
          saveResults.insertThreadEvaluation(schema, {
            id: evalId,
            threadId,
            auditId,
            category,
            summary: aiSummary,
            isDeal,
            likelyScam: isLikelyScam,
            score: aiScore,
          }))

        // c. If is_deal and main_contact exists
        if (thread.is_deal && thread.main_contact) {
          const contact = thread.main_contact
          const contactEmail = sanitizeString(contact.email || '')
          const contactName = sanitizeString(contact.name || '')
          const contactCompany = sanitizeString(contact.company || '')
          const contactTitle = sanitizeString(contact.title || '')
          const contactId = crypto.randomUUID()

          await executeSql(sxtApiUrl, accessToken, biscuit,
            saveResults.deleteContact(schema, contactEmail))
          await executeSql(sxtApiUrl, accessToken, biscuit,
            saveResults.insertContact(schema, {
              id: contactId,
              email: contactEmail,
              name: contactName,
              company: contactCompany,
              title: contactTitle,
            }))

          const dealId = crypto.randomUUID()
          const dealName = sanitizeString(thread.deal_name || '')
          const dealType = sanitizeString(thread.deal_type || '')
          const dealValue = typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
          const currency = sanitizeString(thread.currency || 'USD')

          await executeSql(sxtApiUrl, accessToken, biscuit,
            saveResults.deleteDeal(schema, threadId, userId))
          await executeSql(sxtApiUrl, accessToken, biscuit,
            saveResults.insertDeal(schema, {
              id: dealId,
              userId,
              threadId,
              evalId,
              dealName,
              dealType,
              category,
              value: dealValue,
              currency,
              brand: contactCompany,
            }))

          await executeSql(sxtApiUrl, accessToken, biscuit,
            saveResults.deleteDealContact(schema, dealId, contactId))
          await executeSql(sxtApiUrl, accessToken, biscuit,
            saveResults.insertDealContact(schema, {
              id: crypto.randomUUID(),
              dealId,
              contactId,
            }))

          dealsCreated++
        }

        // d. Update EMAIL_METADATA stages
        if (threadEmails.length > 0) {
          const newStage = resolveStage(thread)
          const sqlQuotedIds = toSqlIdList(threadEmails.map((e) => e.EMAIL_METADATA_ID))

          if (newStage === 4) {
            await executeSql(sxtApiUrl, accessToken, biscuit,
              detection.updateDeals(schema, sqlQuotedIds))
          } else if (newStage === 107) {
            await executeSql(sxtApiUrl, accessToken, biscuit,
              detection.updateNonEnglish(schema, sqlQuotedIds))
          } else {
            await executeSql(sxtApiUrl, accessToken, biscuit,
              detection.updateRejected(schema, sqlQuotedIds))
          }
          emailsClassified += threadEmails.length
        }
      } catch (err) {
        core.error(`Failed to process thread ${thread.thread_id}: ${err.message}`)
      }
    }

    core.setOutput('success', 'true')
    core.setOutput('deals_created', String(dealsCreated))
    core.setOutput('emails_classified', String(emailsClassified))
  } catch (error) {
    core.setOutput('success', 'false')
    core.setOutput('deals_created', '0')
    core.setOutput('emails_classified', '0')
    core.setFailed(error.message)
  }
}
