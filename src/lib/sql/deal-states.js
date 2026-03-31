// src/lib/sql/deal-states.js
//
// DEAL_STATES table SQL builders.
// Pure functions: params in, SQL string out. No DB connection, no side effects.
//
// SxT constraints:
//   - No CTEs (WITH ... AS)
//   - No division operator
//   - INTERVAL syntax: INTERVAL 'N' MINUTE
//   - ON CONFLICT (...) DO UPDATE supported
//   - LEFT JOIN on single column only

import { sanitizeId, sanitizeString, sanitizeSchema } from './sanitize.js'

export const STATUS = {
  PENDING: 'pending',
  FILTERING: 'filtering',
  PENDING_CLASSIFICATION: 'pending_classification',
  CLASSIFYING: 'classifying',
  DEAL: 'deal',
  NOT_DEAL: 'not_deal',
  FILTER_REJECTED: 'filter_rejected',
  FAILED: 'failed',
}

export const dealStates = {
  claimFilterBatch: (schema, batchId, batchSize) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'filtering', BATCH_ID = '${bid}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (SELECT EMAIL_METADATA_ID FROM ${s}.DEAL_STATES WHERE STATUS = 'pending' LIMIT ${Number(batchSize)})`
  },

  claimClassifyBatch: (schema, batchId, batchSize) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'classifying', BATCH_ID = '${bid}', UPDATED_AT = CURRENT_TIMESTAMP WHERE THREAD_ID IN (SELECT DISTINCT ds.THREAD_ID FROM ${s}.DEAL_STATES ds WHERE ds.STATUS = 'pending_classification' AND NOT EXISTS (SELECT 1 FROM ${s}.DEAL_STATES ds2 WHERE ds2.THREAD_ID = ds.THREAD_ID AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID AND ds2.STATUS IN ('pending', 'filtering')) LIMIT ${Number(batchSize)}) AND STATUS = 'pending_classification'`
  },

  selectEmailsByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  selectEmailsWithEvalAndCreator: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT ds.EMAIL_METADATA_ID, ds.MESSAGE_ID, ds.USER_ID, ds.THREAD_ID, ds.SYNC_STATE_ID, ete.AI_SUMMARY AS PREVIOUS_AI_SUMMARY, ete.IS_DEAL AS PREVIOUS_IS_DEAL, uss.EMAIL AS CREATOR_EMAIL FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.EMAIL_THREAD_EVALUATIONS ete ON ete.THREAD_ID = ds.THREAD_ID LEFT JOIN ${s}.USER_SYNC_SETTINGS uss ON uss.USER_ID = ds.USER_ID WHERE ds.BATCH_ID = '${bid}'`
  },

  selectEmailAndThreadIdsByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT EMAIL_METADATA_ID, THREAD_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  selectDistinctThreadUsers: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT DISTINCT THREAD_ID, USER_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  updateStatusByIds: (schema, quotedIds, status) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = '${st}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (${quotedIds.join(',')})`
  },

  updateStatusByBatch: (schema, batchId, fromStatus, toStatus) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    const from = sanitizeString(fromStatus)
    const to = sanitizeString(toStatus)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = '${to}', UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${bid}' AND STATUS = '${from}'`
  },

  refreshBatchTimestamp: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${bid}'`
  },

  findStuckBatches: (schema, status, intervalMinutes, maxRetries) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${st}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(intervalMinutes)}' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${Number(maxRetries)} LIMIT 1`
  },

  findDeadBatches: (schema, status, intervalMinutes, maxRetries) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `SELECT ds.BATCH_ID FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${st}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(intervalMinutes)}' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) >= ${Number(maxRetries)}`
  },

  countByBatchAndStatus: (schema, batchId, status) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    const st = sanitizeString(status)
    return `SELECT COUNT(*) AS C FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}' AND STATUS = '${st}'`
  },

  countOrphaned: (schema, statuses, staleMinutes) => {
    const s = sanitizeSchema(schema)
    const literals = statuses.map((st) => `'${sanitizeString(st)}'`).join(',')
    return `SELECT COUNT(*) AS C FROM ${s}.DEAL_STATES WHERE STATUS IN (${literals}) AND BATCH_ID IS NULL AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(staleMinutes)}' MINUTE`
  },

  markOrphanedAsFailed: (schema, statuses, staleMinutes) => {
    const s = sanitizeSchema(schema)
    const literals = statuses.map((st) => `'${sanitizeString(st)}'`).join(',')
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'failed', UPDATED_AT = CURRENT_TIMESTAMP WHERE STATUS IN (${literals}) AND BATCH_ID IS NULL AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(staleMinutes)}' MINUTE`
  },

  syncFromEmailMetadata: (schema, emailCoreSchema) => {
    const s = sanitizeSchema(schema)
    const ecs = sanitizeSchema(emailCoreSchema)
    return `INSERT INTO ${s}.DEAL_STATES (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT) SELECT gen_random_uuid(), em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM ${ecs}.EMAIL_METADATA em WHERE NOT EXISTS (SELECT 1 FROM ${s}.DEAL_STATES ds WHERE ds.EMAIL_METADATA_ID = em.ID) ON CONFLICT (EMAIL_METADATA_ID) DO UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP`
  },
}
