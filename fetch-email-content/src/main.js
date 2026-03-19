import * as core from '@actions/core'

function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) throw new Error(`Invalid ID: ${id}`)
  return id
}

async function fetchWithRetry(url, options, maxRetries = 3) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, options)
      if (!response.ok) {
        if (
          attempt < maxRetries &&
          [429, 500, 502, 503, 504].includes(response.status)
        ) {
          await new Promise((r) => setTimeout(r, 1000 * Math.pow(2, attempt)))
          continue
        }
        throw new Error(
          `Content fetcher HTTP ${response.status}: ${await response.text()}`,
        )
      }
      return response
    } catch (err) {
      if (
        attempt < maxRetries &&
        !err.message?.includes('Content fetcher HTTP')
      ) {
        await new Promise((r) => setTimeout(r, 1000 * Math.pow(2, attempt)))
        continue
      }
      throw err
    }
  }
}

export async function run() {
  try {
    const metadataJson = core.getInput('metadata')
    const contentFetcherUrl = core.getInput('content-fetcher-url')
    const fieldsStr = core.getInput('fields')

    if (!metadataJson || metadataJson === '[]') {
      core.setOutput('emails', '[]')
      core.setOutput('failed_ids', '')
      core.setOutput('success', 'true')
      return
    }

    const metadata = JSON.parse(metadataJson)
    const fields = fieldsStr.split(',').map((f) => f.trim())

    // Group by USER_ID
    const userGroups = {}
    for (const row of metadata) {
      const userId = row.USER_ID
      if (!userGroups[userId]) userGroups[userId] = []
      userGroups[userId].push(row)
    }

    const allEmails = []
    const failedIds = []

    // Call content fetcher per user
    for (const [userId, rows] of Object.entries(userGroups)) {
      const messageIds = rows.map((r) => r.MESSAGE_ID)

      const response = await fetchWithRetry(
        `${contentFetcherUrl}/email-content/fetch`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            userId,
            messageIds,
            syncStateId: `w3-workflow-${Date.now()}`,
          }),
        },
      )

      const result = await response.json()
      const contentItems = result.data || result || []

      // Build lookup by messageId
      const contentByMessageId = {}
      for (const item of contentItems) {
        contentByMessageId[item.messageId] = item
      }

      // Join with SxT metadata
      for (const row of rows) {
        const content = contentByMessageId[row.MESSAGE_ID]
        if (!content) {
          // Content fetcher returned no data for this message
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
      failedIds.length > 0
        ? failedIds.map((id) => `'${sanitizeId(id)}'`).join(',')
        : '',
    )
    core.setOutput('success', 'true')
  } catch (error) {
    core.setOutput('success', 'false')
    core.setFailed(error.message)
  }
}
