import * as core from '@actions/core'

/**
 * Extract userId, messageIds, and syncStateId from SxT metadata rows.
 * Replaces the GHA-only `fromJSON(x).*.FIELD` wildcard syntax that W3 doesn't support.
 *
 * Input: metadata (JSON array of rows with USER_ID, MESSAGE_ID, SYNC_STATE_ID)
 * Output: { userId, messageIds, syncStateId, metadata } — ready to POST to content fetcher
 */
export async function runExtractMetadata() {
  const metadataRaw = core.getInput('metadata')
  if (!metadataRaw || metadataRaw === '[]') {
    return { userId: '', messageIds: [], syncStateId: '', metadata: '[]' }
  }

  const rows = JSON.parse(metadataRaw)
  if (rows.length === 0) {
    return { userId: '', messageIds: [], syncStateId: '', metadata: '[]' }
  }

  const userId = rows[0].USER_ID
  const syncStateId = rows[0].SYNC_STATE_ID
  const messageIds = rows.map((r) => r.MESSAGE_ID)

  return { userId, messageIds, syncStateId, metadata: metadataRaw }
}
