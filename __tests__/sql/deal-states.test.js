import { dealStates } from '../../src/lib/sql/deal-states.js'

describe('dealStates', () => {
  const S = 'TEST_SCHEMA'

  describe('claimFilterBatch', () => {
    it('produces UPDATE with subquery LIMIT', () => {
      const sql = dealStates.claimFilterBatch(S, 'batch-123', 200)
      expect(sql).toContain(`UPDATE ${S}.DEAL_STATES`)
      expect(sql).toContain("SET STATUS = 'filtering'")
      expect(sql).toContain("BATCH_ID = 'batch-123'")
      expect(sql).toContain("STATUS = 'pending'")
      expect(sql).toContain('LIMIT 200')
    })
  })

  describe('claimClassifyBatch', () => {
    it('produces thread-aware UPDATE with NOT EXISTS', () => {
      const sql = dealStates.claimClassifyBatch(S, 'batch-456', 5)
      expect(sql).toContain("SET STATUS = 'classifying'")
      expect(sql).toContain("BATCH_ID = 'batch-456'")
      expect(sql).toContain("STATUS = 'pending_classification'")
      expect(sql).toContain('NOT EXISTS')
      expect(sql).toContain('LIMIT 5')
    })
  })

  describe('selectEmailsByBatch', () => {
    it('selects 5 columns by batch ID', () => {
      const sql = dealStates.selectEmailsByBatch(S, 'batch-123')
      expect(sql).toContain('SELECT EMAIL_METADATA_ID')
      expect(sql).toContain(`FROM ${S}.DEAL_STATES`)
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })
  })

  describe('selectEmailsWithEvalAndCreator', () => {
    it('includes LEFT JOINs for evaluations and user sync settings', () => {
      const sql = dealStates.selectEmailsWithEvalAndCreator(S, 'batch-789')
      expect(sql).toContain('LEFT JOIN')
      expect(sql).toContain('EMAIL_THREAD_EVALUATIONS')
      expect(sql).toContain('USER_SYNC_SETTINGS')
      expect(sql).toContain("BATCH_ID = 'batch-789'")
    })
  })

  describe('selectEmailAndThreadIdsByBatch', () => {
    it('selects EMAIL_METADATA_ID and THREAD_ID', () => {
      const sql = dealStates.selectEmailAndThreadIdsByBatch(S, 'batch-123')
      expect(sql).toContain('SELECT EMAIL_METADATA_ID, THREAD_ID')
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })
  })

  describe('selectDistinctThreadUsers', () => {
    it('selects DISTINCT THREAD_ID and USER_ID', () => {
      const sql = dealStates.selectDistinctThreadUsers(S, 'batch-123')
      expect(sql).toContain('SELECT DISTINCT THREAD_ID, USER_ID')
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })
  })

  describe('updateStatusByIds', () => {
    it('updates status with UPDATED_AT for a list of IDs', () => {
      const sql = dealStates.updateStatusByIds(S, ["'id-1'", "'id-2'"], 'deal')
      expect(sql).toContain("SET STATUS = 'deal'")
      expect(sql).toContain('UPDATED_AT = CURRENT_TIMESTAMP')
      expect(sql).toContain("'id-1'")
      expect(sql).toContain("'id-2'")
    })
  })

  describe('updateStatusByBatch', () => {
    it('updates status filtered by batch and current status', () => {
      const sql = dealStates.updateStatusByBatch(S, 'batch-1', 'filtering', 'failed')
      expect(sql).toContain("SET STATUS = 'failed'")
      expect(sql).toContain("BATCH_ID = 'batch-1'")
      expect(sql).toContain("STATUS = 'filtering'")
    })
  })

  describe('refreshBatchTimestamp', () => {
    it('updates UPDATED_AT for a batch', () => {
      const sql = dealStates.refreshBatchTimestamp(S, 'batch-1')
      expect(sql).toContain('SET UPDATED_AT = CURRENT_TIMESTAMP')
      expect(sql).toContain("BATCH_ID = 'batch-1'")
    })
  })

  describe('findStuckBatches', () => {
    it('finds batches with attempts < maxRetries', () => {
      const sql = dealStates.findStuckBatches(S, 'classifying', 5, 6)
      expect(sql).toContain("STATUS = 'classifying'")
      expect(sql).toContain("INTERVAL '5' MINUTE")
      expect(sql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) < 6')
      expect(sql).toContain('LIMIT 1')
    })
  })

  describe('findDeadBatches', () => {
    it('finds batches with attempts >= maxRetries', () => {
      const sql = dealStates.findDeadBatches(S, 'filtering', 5, 6)
      expect(sql).toContain("STATUS = 'filtering'")
      expect(sql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) >= 6')
    })
  })

  describe('countByBatchAndStatus', () => {
    it('counts rows by batch and status', () => {
      const sql = dealStates.countByBatchAndStatus(S, 'batch-1', 'filtering')
      expect(sql).toContain('SELECT COUNT(*) AS C')
      expect(sql).toContain("BATCH_ID = 'batch-1'")
      expect(sql).toContain("STATUS = 'filtering'")
    })
  })

  describe('countOrphaned', () => {
    it('counts stale unbatched rows', () => {
      const sql = dealStates.countOrphaned(S, ['pending_classification'], 30)
      expect(sql).toContain("STATUS IN ('pending_classification')")
      expect(sql).toContain('BATCH_ID IS NULL')
      expect(sql).toContain("INTERVAL '30' MINUTE")
    })
  })

  describe('markOrphanedAsFailed', () => {
    it('marks stale unbatched rows as failed', () => {
      const sql = dealStates.markOrphanedAsFailed(S, ['pending_classification'], 30)
      expect(sql).toContain("SET STATUS = 'failed'")
      expect(sql).toContain('BATCH_ID IS NULL')
      expect(sql).toContain("INTERVAL '30' MINUTE")
    })
  })

  describe('syncFromEmailMetadata', () => {
    it('inserts missing rows from EMAIL_METADATA', () => {
      const sql = dealStates.syncFromEmailMetadata(S, 'EMAIL_CORE_STAGING')
      expect(sql).toContain(`INSERT INTO ${S}.DEAL_STATES`)
      expect(sql).toContain('FROM EMAIL_CORE_STAGING.EMAIL_METADATA')
      expect(sql).toContain('NOT EXISTS')
      expect(sql).toContain('ON CONFLICT (EMAIL_METADATA_ID)')
    })
  })

  describe('SQL injection prevention', () => {
    it('sanitizeId rejects malicious batch IDs', () => {
      expect(() => dealStates.claimFilterBatch(S, "'; DROP TABLE --", 10)).toThrow('Invalid ID')
    })

    it('sanitizeSchema rejects malicious schema names', () => {
      expect(() => dealStates.claimFilterBatch('BAD SCHEMA; --', 'b1', 10)).toThrow(
        'Invalid schema',
      )
    })
  })
})
