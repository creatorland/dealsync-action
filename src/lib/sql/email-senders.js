// src/lib/sql/email-senders.js
//
// Cross-schema read of EMAIL_CORE.EMAIL_METADATA + EMAIL_SENDERS, used
// by the classify pipeline's main_contact fallback when no fetched email
// payload is available (e.g. cached-audit retries).
//
// SxT constraints (same as deal-states.js):
//   - No CTEs
//   - LEFT/INNER JOIN on single column
//   - Rows are returned ORDER BY RECEIVED_AT DESC so the caller can pick
//     the latest usable sender per thread in JS.

import { sanitizeSchema } from './sanitize.js'

export const emailSenders = {
  selectByThreadIds: (coreSchema, quotedThreadIds) => {
    const s = sanitizeSchema(coreSchema)
    return `SELECT em.THREAD_ID, em.RECEIVED_AT, es.SENDER_EMAIL, es.SENDER_NAME FROM ${s}.EMAIL_METADATA em INNER JOIN ${s}.EMAIL_SENDERS es ON es.EMAIL_METADATA_ID = em.ID WHERE em.THREAD_ID IN (${quotedThreadIds.join(',')}) ORDER BY em.RECEIVED_AT DESC LIMIT 10000`
  },
}
