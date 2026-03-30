/**
 * Shared SQL queries for Dealsync W3 workflow actions.
 *
 * All queries target DEAL_STATES (not EMAIL_METADATA).
 * All column names UPPERCASE (SxT convention).
 * Schema is passed as a parameter — never hardcoded.
 * IDs must be sanitized before interpolation.
 *
 * Status-based state machine:
 *   pending → filtering → pending_classification → classifying → deal | not_deal
 *   filtering → filter_rejected (terminal)
 *   filtering | classifying | pending_classification → failed (terminal: dead-letter, stuck sweep, orphan sweep)
 */

import { audits } from './sql/index.js'

// ============================================================
// STATUS CONSTANTS
// ============================================================

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

// ============================================================
// DETECTION PROCESSOR QUERIES
// ============================================================

export const detection = {
  /** Move deal deal_states to deal status */
  updateDeals: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.DEAL}' WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move non-deal deal_states to not_deal status */
  updateNotDeal: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.NOT_DEAL}' WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,
}

// ============================================================
// SAVE RESULTS QUERIES (detection pipeline DML)
// ============================================================

export const saveResults = {
  getAuditByBatchId: (schema, batchId) => audits.selectByBatch(schema, batchId),
  insertAudit: (schema, params) => audits.insert(schema, params),
}

// ============================================================
// UTILITIES
// ============================================================

export function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`Invalid ID format: ${id}`)
  }
  return id
}

export function sanitizeString(s) {
  return (s || '')
    .replace(/[\u2018\u2019\u201A\u201B]/g, "'") // curly single quotes → straight
    .replace(/[\u201C\u201D\u201E\u201F]/g, '"') // curly double quotes → straight
    .replace(/'/g, "''") // escape single quotes for SQL
    .replace(/\\/g, '\\\\') // escape backslashes
}

export function toSqlIdList(ids) {
  return ids.map((id) => `'${sanitizeId(id)}'`).join(',')
}

export function sanitizeSchema(schema) {
  if (!/^[a-zA-Z0-9_]+$/.test(schema)) {
    throw new Error(`Invalid schema: ${schema}`)
  }
  return schema
}
