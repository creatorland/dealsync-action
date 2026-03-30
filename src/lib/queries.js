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

import { audits } from './sql/audits.js'

// Re-export sanitization utilities from their canonical location.
export { sanitizeId, sanitizeString, toSqlIdList, sanitizeSchema } from './sql/sanitize.js'

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
// SAVE RESULTS QUERIES (detection pipeline DML)
// ============================================================

export const saveResults = {
  getAuditByBatchId: (schema, batchId) => audits.selectByBatch(schema, batchId),
  insertAudit: (schema, params) => audits.insert(schema, params),
}
