import * as crypto from 'crypto'

import * as core from '@actions/core'

import { decryptValue, tryDecrypt } from '../../shared/crypto.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) throw new Error(`Invalid ID: ${id}`)
  return id
}

function sanitizeString(s) {
  return (s || '').replace(/'/g, "''")
}

function sanitizeSchema(schema) {
  if (!/^[a-zA-Z0-9_]+$/.test(schema)) {
    throw new Error(`Invalid schema: ${schema}`)
  }
  return schema
}

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

// ---------------------------------------------------------------------------
// SxT Auth
// ---------------------------------------------------------------------------

async function authenticate(sxtApiUrl, userId, password) {
  const loginResp = await fetch(`${sxtApiUrl}/v1/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId, password }),
  })
  if (!loginResp.ok) {
    throw new Error(`SxT auth login ${loginResp.status}: ${await loginResp.text()}`)
  }
  const loginData = (await loginResp.json())[0]
  const accessToken = loginData.ACCESSTOKEN
  const sessionId = loginData.SESSIONID

  const biscuitResp = await fetch(
    `${sxtApiUrl}/v1/biscuits/generated/dealsync-dml`,
    { headers: { sessionId } },
  )
  if (!biscuitResp.ok) {
    throw new Error(`SxT biscuit ${biscuitResp.status}: ${await biscuitResp.text()}`)
  }
  const biscuitData = (await biscuitResp.json())[0]
  const biscuit = biscuitData.BISCUIT

  return { accessToken, biscuit }
}

// ---------------------------------------------------------------------------
// Decrypt helper – tries to decrypt; if encryption-key is empty, returns raw
// ---------------------------------------------------------------------------

function decryptInput(value, encryptionKey) {
  if (!encryptionKey) return value
  const decrypted = tryDecrypt(value, encryptionKey)
  return decrypted !== null ? decrypted : value
}

// ---------------------------------------------------------------------------
// Stage mapping
// ---------------------------------------------------------------------------

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

    // Read and optionally decrypt inputs
    const sxtApiUrl = core.getInput('sxt-api-url')
    const schema = sanitizeSchema(core.getInput('sxt-schema'))
    const sxtUserId = decryptInput(core.getInput('sxt-user-id'), encryptionKey)
    const sxtPassword = decryptInput(
      core.getInput('sxt-password'),
      encryptionKey,
    )

    const aiOutputRaw = decryptInput(core.getInput('ai-output'), encryptionKey)
    const aiModel = sanitizeString(core.getInput('ai-model'))
    const aiPromptTokens = parseInt(core.getInput('ai-prompt-tokens'), 10) || 0
    const aiCompletionTokens =
      parseInt(core.getInput('ai-completion-tokens'), 10) || 0
    const metadataRaw = decryptInput(core.getInput('metadata'), encryptionKey)
    const transitionStage = core.getInput('transition-stage')

    // Parse JSON inputs
    const aiOutput = JSON.parse(aiOutputRaw)
    const metadata = JSON.parse(metadataRaw)

    // Build metadata lookup: thread_id -> array of email metadata rows
    const metadataByThread = {}
    for (const row of metadata) {
      const tid = row.THREAD_ID
      if (!metadataByThread[tid]) metadataByThread[tid] = []
      metadataByThread[tid].push(row)
    }

    // Handle empty threads
    const threads = aiOutput.threads || []
    if (threads.length === 0) {
      core.info('No threads to process')
      core.setOutput('success', 'true')
      core.setOutput('deals_created', '0')
      core.setOutput('emails_classified', '0')
      return
    }

    // Re-authenticate to SxT for fresh tokens
    const { accessToken, biscuit } = await authenticate(
      sxtApiUrl,
      sxtUserId,
      sxtPassword,
    )

    let dealsCreated = 0
    let emailsClassified = 0

    const totalTokens = aiPromptTokens + aiCompletionTokens
    const inferenceCost = (totalTokens / 1000) * 0.001

    for (const thread of threads) {
      try {
        const threadId = sanitizeId(thread.thread_id)
        const threadEmails = metadataByThread[threadId] || []
        const emailCount = threadEmails.length
        const userId =
          threadEmails.length > 0 ? sanitizeId(threadEmails[0].USER_ID) : ''

        // ----- a. INSERT AI_EVALUATION_AUDITS -----
        const auditId = crypto.randomUUID()
        const rawJson = sanitizeString(
          JSON.stringify(thread).substring(0, 6400),
        )
        await executeSql(
          sxtApiUrl,
          accessToken,
          biscuit,
          `INSERT INTO ${schema}.AI_EVALUATION_AUDITS (ID, THREAD_COUNT, EMAIL_COUNT, INFERENCE_COST, INPUT_TOKENS, OUTPUT_TOKENS, MODEL_USED, AI_EVALUATION, CREATED_AT, UPDATED_AT) VALUES ('${auditId}', ${threads.length}, ${emailCount}, ${inferenceCost}, ${aiPromptTokens}, ${aiCompletionTokens}, '${aiModel}', '${rawJson}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        )

        // ----- b. DELETE + INSERT EMAIL_THREAD_EVALUATIONS -----
        const evalId = crypto.randomUUID()
        const category = sanitizeString(thread.category || '')
        const aiSummary = sanitizeString(thread.ai_summary || '')
        const isDeal = thread.is_deal ? 'true' : 'false'
        const isLikelyScam =
          (thread.category || '').toLowerCase() === 'likely_scam'
            ? 'true'
            : 'false'
        const aiScore = typeof thread.ai_score === 'number' ? thread.ai_score : 0

        await executeSql(
          sxtApiUrl,
          accessToken,
          biscuit,
          `DELETE FROM ${schema}.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID = '${threadId}'`,
        )
        await executeSql(
          sxtApiUrl,
          accessToken,
          biscuit,
          `INSERT INTO ${schema}.EMAIL_THREAD_EVALUATIONS (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT) VALUES ('${evalId}', '${threadId}', '${auditId}', '${category}', '${aiSummary}', ${isDeal}, ${isLikelyScam}, ${aiScore}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        )

        // ----- c. If is_deal and main_contact exists -----
        if (thread.is_deal && thread.main_contact) {
          const contact = thread.main_contact
          const contactEmail = sanitizeString(contact.email || '')
          const contactName = sanitizeString(contact.name || '')
          const contactCompany = sanitizeString(contact.company || '')
          const contactRole = sanitizeString(contact.role || '')
          const contactId = crypto.randomUUID()

          // DELETE + INSERT CONTACTS (keyed by email)
          await executeSql(
            sxtApiUrl,
            accessToken,
            biscuit,
            `DELETE FROM ${schema}.CONTACTS WHERE EMAIL = '${contactEmail}'`,
          )
          await executeSql(
            sxtApiUrl,
            accessToken,
            biscuit,
            `INSERT INTO ${schema}.CONTACTS (ID, EMAIL, NAME, COMPANY, ROLE, CREATED_AT, UPDATED_AT) VALUES ('${contactId}', '${contactEmail}', '${contactName}', '${contactCompany}', '${contactRole}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          )

          // DELETE + INSERT DEALS (keyed by thread_id + user_id)
          const dealId = crypto.randomUUID()
          const dealTitle = sanitizeString(thread.deal_title || thread.ai_summary || '')
          const dealValue = typeof thread.deal_value === 'number' ? thread.deal_value : 0

          await executeSql(
            sxtApiUrl,
            accessToken,
            biscuit,
            `DELETE FROM ${schema}.DEALS WHERE THREAD_ID = '${threadId}' AND USER_ID = '${userId}'`,
          )
          await executeSql(
            sxtApiUrl,
            accessToken,
            biscuit,
            `INSERT INTO ${schema}.DEALS (ID, THREAD_ID, USER_ID, TITLE, VALUE, CREATED_AT, UPDATED_AT) VALUES ('${dealId}', '${threadId}', '${userId}', '${dealTitle}', ${dealValue}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          )

          // DELETE + INSERT DEAL_CONTACTS
          const dealContactId = crypto.randomUUID()
          await executeSql(
            sxtApiUrl,
            accessToken,
            biscuit,
            `DELETE FROM ${schema}.DEAL_CONTACTS WHERE DEAL_ID IN (SELECT ID FROM ${schema}.DEALS WHERE THREAD_ID = '${threadId}' AND USER_ID = '${userId}')`,
          )
          await executeSql(
            sxtApiUrl,
            accessToken,
            biscuit,
            `INSERT INTO ${schema}.DEAL_CONTACTS (ID, DEAL_ID, CONTACT_ID, CREATED_AT) VALUES ('${dealContactId}', '${dealId}', '${contactId}', CURRENT_TIMESTAMP)`,
          )

          dealsCreated++
        }

        // ----- d. Update EMAIL_METADATA stages -----
        if (threadEmails.length > 0) {
          const newStage = resolveStage(thread)
          const sqlQuotedIds = threadEmails
            .map((e) => `'${sanitizeId(e.ID)}'`)
            .join(',')

          await executeSql(
            sxtApiUrl,
            accessToken,
            biscuit,
            `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${newStage} WHERE ID IN (${sqlQuotedIds})`,
          )
          emailsClassified += threadEmails.length
        }
      } catch (err) {
        core.error(
          `Failed to process thread ${thread.thread_id}: ${err.message}`,
        )
        // Best-effort: continue with remaining threads
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
