import { sanitizeSchema } from './sanitize.js'

export const evaluations = {
  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.EMAIL_THREAD_EVALUATIONS (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (THREAD_ID) DO UPDATE SET AI_EVALUATION_AUDIT_ID = EXCLUDED.AI_EVALUATION_AUDIT_ID, AI_INSIGHT = EXCLUDED.AI_INSIGHT, AI_SUMMARY = EXCLUDED.AI_SUMMARY, IS_DEAL = EXCLUDED.IS_DEAL, LIKELY_SCAM = EXCLUDED.LIKELY_SCAM, AI_SCORE = EXCLUDED.AI_SCORE, UPDATED_AT = CURRENT_TIMESTAMP`
  },

  selectByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `SELECT THREAD_ID, IS_DEAL FROM ${s}.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },
}
