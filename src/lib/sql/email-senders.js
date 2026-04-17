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

import { sanitizeSchema } from './sanitize.js'

/** Max rows scanned per thread; caller stops at first usable sender. */
const PER_THREAD_SENDER_SCAN_LIMIT = 500

export const emailSenders = {
  /**
   * @param {string} coreSchema - sanitized schema name
   * @param {string} quotedThreadId - already-quoted thread id literal, e.g. `'th-1'`
   * @param {string} quotedUserId - already-quoted user id literal, e.g. `'u-1'`
   */
  selectForThreadUser: (coreSchema, quotedThreadId, quotedUserId) => {
    const s = sanitizeSchema(coreSchema)
    return `SELECT em.THREAD_ID, em.RECEIVED_AT, es.SENDER_EMAIL, es.SENDER_NAME FROM ${s}.EMAIL_METADATA em INNER JOIN ${s}.EMAIL_SENDERS es ON es.EMAIL_METADATA_ID = em.ID WHERE em.THREAD_ID = ${quotedThreadId} AND em.USER_ID = ${quotedUserId} ORDER BY em.RECEIVED_AT DESC LIMIT ${PER_THREAD_SENDER_SCAN_LIMIT}`
  },
}
