/**
 * Regression tests for buildSxtUpsert — Story 0.2 Phase 0 spike.
 *
 * Covers the safety invariants the worker depends on:
 *   - Aggregate → SxT table mapping (5 canonical aggregates + unknown rejection)
 *   - Empty-payload rejection (Copilot review finding #4 on PR #22)
 *   - Column-name identifier validation (Copilot review finding #5)
 *   - String / number / null value serialization
 *   - Single-quote escaping in string values + aggregate_id
 *   - Diagnostic SQL comment (`-- outbox_id: N`) ride-through
 *   - ON CONFLICT (ID) DO UPDATE SET clause shape
 */

import { jest } from '@jest/globals'

const { buildSxtUpsert } = await import('../src/outbox-settlement-worker/index.js')

function row(overrides = {}) {
  return {
    id: 42,
    aggregate: 'deal',
    aggregate_id: 'deal-abc',
    operation: 'update',
    payload: { category: 'booking', updated_at: '2026-05-27T05:00:00Z' },
    ...overrides,
  }
}

describe('buildSxtUpsert', () => {
  describe('aggregate table mapping', () => {
    it.each([
      ['deal', 'DEALSYNC_STG_V1.DEALS'],
      ['contact', 'DEALSYNC_STG_V1.CONTACTS'],
      ['email_thread_eval', 'DEALSYNC_STG_V1.EMAIL_THREAD_EVALUATIONS'],
      ['ai_eval_audit', 'DEALSYNC_STG_V1.AI_EVALUATION_AUDITS'],
      ['thread_user_state', 'DEALSYNC_STG_V1.THREAD_USER_STATE'],
    ])('maps aggregate %s → %s', (aggregate, expectedTable) => {
      const sql = buildSxtUpsert(row({ aggregate }))
      expect(sql).toContain(`INSERT INTO ${expectedTable}`)
    })

    it('rejects unknown aggregate', () => {
      expect(() => buildSxtUpsert(row({ aggregate: 'unknown_aggregate' }))).toThrow(
        /unknown aggregate unknown_aggregate/,
      )
    })
  })

  describe('safety invariants', () => {
    it('rejects empty payload (no columns to update)', () => {
      // Without this guard, `DO UPDATE SET ` (empty) would be invalid SQL.
      expect(() => buildSxtUpsert(row({ payload: {} }))).toThrow(/payload is empty/)
    })

    it('rejects payload missing entirely', () => {
      expect(() => buildSxtUpsert(row({ payload: undefined }))).toThrow(/payload is empty/)
    })

    it.each([
      ['category; DROP TABLE deals --', 'SQL keyword/comment injection'],
      ['cat,gory', 'comma in column name'],
      ['CAT) VALUES (', 'paren in column name'],
      ['1category', 'leading digit'],
      ["cat'gory", 'single quote'],
      ['', 'empty string key'],
    ])('rejects malformed column name "%s" (%s)', (key) => {
      expect(() => buildSxtUpsert(row({ payload: { [key]: 'x' } }))).toThrow(
        /not a valid SQL identifier/,
      )
    })

    it('accepts strict snake_case column names', () => {
      expect(() =>
        buildSxtUpsert(row({ payload: { simple: 'a', with_underscore: 'b', _leading: 'c' } })),
      ).not.toThrow()
    })

    it('rejects empty aggregate_id', () => {
      expect(() => buildSxtUpsert(row({ aggregate_id: '' }))).toThrow(
        /aggregate_id must be a non-empty string/,
      )
    })

    it('rejects non-string aggregate_id', () => {
      expect(() => buildSxtUpsert(row({ aggregate_id: 123 }))).toThrow(
        /aggregate_id must be a non-empty string/,
      )
    })
  })

  describe('SQL shape per architecture spec', () => {
    it('leads with the `-- outbox_id: N` diagnostic comment', () => {
      const sql = buildSxtUpsert(row({ id: 99 }))
      // Architecture §"Idempotency contract with SxT" item 2:
      // SQL comment is informational, does NOT participate in conflict key
      expect(sql.startsWith('-- outbox_id: 99\n')).toBe(true)
    })

    it('uses ON CONFLICT (ID) DO UPDATE keyed on natural id (not outbox_id)', () => {
      // Architecture §"Idempotency contract with SxT" item 1: keyed on
      // business row's natural id, NOT outbox_id (using outbox_id would
      // create a new SxT row per update instead of updating canonical row).
      const sql = buildSxtUpsert(row())
      expect(sql).toMatch(/ON CONFLICT \(ID\) DO UPDATE SET/)
    })

    it('uppercases column names per SxT convention', () => {
      const sql = buildSxtUpsert(row({ payload: { category: 'booking' } }))
      expect(sql).toContain('(ID, CATEGORY)')
      expect(sql).toContain('CATEGORY = EXCLUDED.CATEGORY')
    })

    it('serializes string values with single quotes', () => {
      const sql = buildSxtUpsert(row({ payload: { category: 'booking' } }))
      expect(sql).toContain("'booking'")
    })

    it('escapes single quotes in string values', () => {
      const sql = buildSxtUpsert(row({ payload: { note: "O'Reilly" } }))
      expect(sql).toContain("'O''Reilly'")
    })

    it('escapes single quotes in aggregate_id', () => {
      const sql = buildSxtUpsert(row({ aggregate_id: "deal-O'Reilly" }))
      expect(sql).toContain("'deal-O''Reilly'")
    })

    it('serializes numbers without quotes', () => {
      const sql = buildSxtUpsert(row({ payload: { score: 42 } }))
      expect(sql).toContain('VALUES (')
      expect(sql).toContain(', 42)')
    })

    it('serializes null as literal NULL', () => {
      const sql = buildSxtUpsert(row({ payload: { category: null } }))
      expect(sql).toContain(', NULL)')
    })

    it('emits multiple columns + multiple EXCLUDED assignments', () => {
      const sql = buildSxtUpsert(
        row({ payload: { category: 'booking', updated_at: '2026-05-27T05:00:00Z' } }),
      )
      expect(sql).toContain('(ID, CATEGORY, UPDATED_AT)')
      expect(sql).toMatch(/CATEGORY = EXCLUDED.CATEGORY, UPDATED_AT = EXCLUDED.UPDATED_AT/)
    })
  })
})
