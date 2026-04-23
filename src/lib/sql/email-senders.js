// src/lib/sql/email-senders.js
//
// Cross-schema read of EMAIL_CORE.EMAIL_METADATA + EMAIL_SENDERS, used
// by the classify pipeline's main_contact fallback when no fetched email
// payload is available (e.g. cached-audit retries). Classify calls this once
// per (thread, user) so THREAD_ID need not be globally unique across users.
//
// SxT constraints (same as deal-states.js):
//   - No CTEs
//   - LEFT/INNER JOIN on single column
//   - Rows are returned ORDER BY RECEIVED_AT DESC so the caller can pick
//     the latest usable sender per thread in JS.

import { sanitizeSchema, sanitizeId } from './sanitize.js'

/** Max rows scanned per thread; caller stops at first usable sender. */
const PER_THREAD_SENDER_SCAN_LIMIT = 500

export const emailSenders = {
  /**
   * @param {string} coreSchema - schema name (validated)
   * @param {string} threadId - raw thread id, e.g. th-1
   * @param {string} userId - raw user id from EMAIL_METADATA / batch rows
   */
  selectForThreadUser: (coreSchema, threadId, userId) => {
    const s = sanitizeSchema(coreSchema)
    const qt = `'${sanitizeId(String(threadId))}'`
    const qu = `'${sanitizeId(String(userId))}'`
    return `SELECT em.THREAD_ID, em.RECEIVED_AT, es.SENDER_EMAIL, es.SENDER_NAME FROM ${s}.EMAIL_METADATA em INNER JOIN ${s}.EMAIL_SENDERS es ON es.EMAIL_METADATA_ID = em.ID WHERE em.THREAD_ID = ${qt} AND em.USER_ID = ${qu} ORDER BY em.RECEIVED_AT DESC LIMIT ${PER_THREAD_SENDER_SCAN_LIMIT}`
  },
}
