import * as core from '@actions/core'

function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) throw new Error(`Invalid ID: ${id}`)
  return id
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

async function fetchWithRetry(url, options, maxRetries = 3) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, options)
      if (!response.ok) {
        if (attempt < maxRetries && [429, 500, 502, 503, 504].includes(response.status)) {
          await sleep(1000 * Math.pow(2, attempt))
          continue
        }
        throw new Error(`HTTP ${response.status}: ${await response.text()}`)
      }
      return response
    } catch (err) {
      if (attempt < maxRetries && !err.message?.startsWith('HTTP ')) {
        await sleep(1000 * Math.pow(2, attempt))
        continue
      }
      throw err
    }
  }
}

async function getSyncStateForUser(authUrl, authSecret, apiUrl, privateKey, schema, userId) {
  // Dynamic import for WASM
  const { SpaceAndTime } = await import('sxt-nodejs-sdk')

  // Auth
  const authResp = await fetch(authUrl, {
    method: 'GET',
    headers: { 'x-shared-secret': authSecret },
  })
  const token = (await authResp.json()).data

  // Generate biscuit
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  const resource = `${schema}.sync_states`
  const biscuit = auth.CreateBiscuitToken(
    [{ operation: 'dql_select', resource }],
    privateKey,
  )

  // Query most recent completed sync state for user
  const sqlResp = await fetch(`${apiUrl}/v1/sql`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sqlText: `SELECT ID FROM ${schema.toUpperCase()}.SYNC_STATES WHERE USER_ID = '${userId}' ORDER BY CREATED_AT DESC LIMIT 1`,
      biscuits: [biscuit.data[0]],
      resources: [resource],
    }),
  })
  const rows = await sqlResp.json()
  return rows[0]?.ID || ''
}

export async function run() {
  try {
    const metadataJson = core.getInput('metadata')
    const contentFetcherUrl = core.getInput('content-fetcher-url')
    const authUrl = core.getInput('auth-url')
    const authSecret = core.getInput('auth-secret')
    const apiUrl = core.getInput('api-url')
    const emailCoreKey = core.getInput('email-core-private-key')
    const emailCoreSchema = core.getInput('email-core-schema')

    if (!metadataJson || metadataJson === '[]') {
      core.setOutput('emails', '[]')
      core.setOutput('failed_ids', '')
      core.setOutput('success', 'true')
      return
    }

    const metadata = JSON.parse(metadataJson)

    // Group by USER_ID
    const userGroups = {}
    for (const row of metadata) {
      const userId = row.USER_ID
      if (!userGroups[userId]) userGroups[userId] = []
      userGroups[userId].push(row)
    }

    const allEmails = []
    const failedIds = []

    for (const [userId, rows] of Object.entries(userGroups)) {
      const messageIds = rows.map((r) => r.MESSAGE_ID)

      // Get syncStateId from email_core
      core.info(`Looking up sync state for user ${userId}`)
      const syncStateId = await getSyncStateForUser(
        authUrl, authSecret, apiUrl, emailCoreKey, emailCoreSchema, userId,
      )
      if (!syncStateId) {
        core.warning(`No sync state found for user ${userId}, skipping`)
        failedIds.push(...rows.map((r) => r.ID))
        continue
      }
      core.info(`Using syncStateId: ${syncStateId}`)

      const response = await fetchWithRetry(
        `${contentFetcherUrl}/email-content/fetch`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ userId, messageIds, syncStateId }),
        },
      )

      const result = await response.json()
      const contentItems = result.data || result || []

      const contentByMessageId = {}
      for (const item of contentItems) {
        contentByMessageId[item.messageId] = item
      }

      for (const row of rows) {
        const content = contentByMessageId[row.MESSAGE_ID]
        if (!content) {
          failedIds.push(row.ID)
          continue
        }
        allEmails.push({
          id: row.ID,
          messageId: row.MESSAGE_ID,
          userId: row.USER_ID,
          threadId: row.THREAD_ID || undefined,
          previousAiSummary: row.PREVIOUS_AI_SUMMARY || undefined,
          existingDealId: row.EXISTING_DEAL_ID || undefined,
          topLevelHeaders: content.topLevelHeaders || [],
          labelIds: content.labelIds || undefined,
          body: content.body || undefined,
          replyBody: content.replyBody || undefined,
        })
      }
    }

    core.setOutput('emails', JSON.stringify(allEmails))
    core.setOutput(
      'failed_ids',
      failedIds.length > 0 ? failedIds.map((id) => `'${sanitizeId(id)}'`).join(',') : '',
    )
    core.setOutput('success', 'true')
  } catch (error) {
    core.setOutput('success', 'false')
    core.setFailed(error.message)
  }
}
