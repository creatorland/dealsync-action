import * as crypto from 'crypto'
import * as core from '@actions/core'
import { tryDecrypt } from '../../shared/crypto.js'
import {
  saveResults,
  detection,
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

export async function runClassify() {
  const encrypt = core.getInput('encrypt') !== 'false'
  const encryptionKey = encrypt ? core.getInput('encryption-key') : null

  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))

  // Decrypt inputs
  let aiResponseRaw = core.getInput('ai-response')
  let metadataRaw = core.getInput('metadata')
  if (encryptionKey) {
    const decryptedAi = tryDecrypt(aiResponseRaw, encryptionKey)
    if (decryptedAi !== null) aiResponseRaw = decryptedAi
    const decryptedMeta = tryDecrypt(metadataRaw, encryptionKey)
    if (decryptedMeta !== null) metadataRaw = decryptedMeta
  }

  // Strip markdown code fences if AI wrapped response in ```json ... ```
  aiResponseRaw = aiResponseRaw.replace(/^```(?:json)?\s*\n?/i, '').replace(/\n?```\s*$/i, '')

  const aiOutput = JSON.parse(aiResponseRaw)
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
    return {
      deals_created: 0,
      emails_classified: 0,
      deal_ids: '',
      not_deal_ids: '',
    }
  }

  const jwt = await authenticate(authUrl, authSecret)

  let dealsCreated = 0
  let emailsClassified = 0
  const dealIdList = []
  const notDealIdList = []

  for (const thread of threads) {
    try {
      const threadId = sanitizeId(thread.thread_id)
      const threadEmails = metadataByThread[threadId] || []
      const emailCount = threadEmails.length
      const userId = threadEmails.length > 0 ? sanitizeId(threadEmails[0].USER_ID) : ''

      // a. INSERT AI_EVALUATION_AUDITS
      const auditId = crypto.randomUUID()
      const rawJson = sanitizeString(JSON.stringify(thread).substring(0, 6400))
      await executeSql(
        apiUrl,
        jwt,
        biscuit,
        saveResults.insertAudit(schema, {
          id: auditId,
          threadCount: threads.length,
          emailCount,
          cost: 0,
          inputTokens: 0,
          outputTokens: 0,
          model: '',
          evaluation: rawJson,
        }),
      )

      // b. DELETE + INSERT EMAIL_THREAD_EVALUATIONS
      const evalId = crypto.randomUUID()
      const category = sanitizeString(thread.category || '')
      const aiSummary = sanitizeString(thread.ai_summary || '')
      const isDeal = thread.is_deal ? 'true' : 'false'
      const isLikelyScam =
        (thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'
      const aiScore = typeof thread.ai_score === 'number' ? thread.ai_score : 0

      await executeSql(apiUrl, jwt, biscuit, saveResults.deleteThreadEvaluation(schema, threadId))
      await executeSql(
        apiUrl,
        jwt,
        biscuit,
        saveResults.insertThreadEvaluation(schema, {
          id: evalId,
          threadId,
          auditId,
          category,
          summary: aiSummary,
          isDeal,
          likelyScam: isLikelyScam,
          score: aiScore,
        }),
      )

      // c. If is_deal and main_contact exists
      if (thread.is_deal && thread.main_contact) {
        const contact = thread.main_contact
        const contactEmail = sanitizeString(contact.email || '')
        const contactName = sanitizeString(contact.name || '')
        const contactCompany = sanitizeString(contact.company || '')
        const contactTitle = sanitizeString(contact.title || '')
        const contactId = crypto.randomUUID()

        await executeSql(apiUrl, jwt, biscuit, saveResults.deleteContact(schema, contactEmail))
        await executeSql(
          apiUrl,
          jwt,
          biscuit,
          saveResults.insertContact(schema, {
            id: contactId,
            email: contactEmail,
            name: contactName,
            company: contactCompany,
            title: contactTitle,
          }),
        )

        const dealId = crypto.randomUUID()
        const dealName = sanitizeString(thread.deal_name || '')
        const dealType = sanitizeString(thread.deal_type || '')
        const dealValue =
          typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
        const currency = sanitizeString(thread.currency || 'USD')

        await executeSql(apiUrl, jwt, biscuit, saveResults.deleteDeal(schema, threadId, userId))
        await executeSql(
          apiUrl,
          jwt,
          biscuit,
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
          }),
        )

        await executeSql(
          apiUrl,
          jwt,
          biscuit,
          saveResults.deleteDealContact(schema, dealId, contactId),
        )
        await executeSql(
          apiUrl,
          jwt,
          biscuit,
          saveResults.insertDealContact(schema, {
            id: crypto.randomUUID(),
            dealId,
            contactId,
          }),
        )

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
      core.error(`Failed to process thread ${thread.thread_id}: ${err.message}`)
    }
  }

  return {
    deals_created: dealsCreated,
    emails_classified: emailsClassified,
    deal_ids: dealIdList.length > 0 ? toSqlIdList(dealIdList) : '',
    not_deal_ids: notDealIdList.length > 0 ? toSqlIdList(notDealIdList) : '',
  }
}
