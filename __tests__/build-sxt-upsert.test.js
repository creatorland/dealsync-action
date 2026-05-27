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

const { buildSxtUpsert, buildSxtMergedUpsert, mergeExistingRowWithPayload } = await import(
  '../src/outbox-settlement-worker/index.js'
)

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

/**
 * Story 0.1 Task 7 (added 2026-05-27 during substrate-validation):
 * SxT requires all NOT NULL columns in the INSERT clause even when the
 * conflict-path-UPDATE branch will be taken. The worker resolves this
 * by reading the existing SxT row + merging the patch payload onto it
 * before building a full-row UPSERT. These tests cover the merge logic;
 * fetchExistingSxtRow is exercised end-to-end against staging in
 * Story 0.1 Task 7.4.
 */
describe('mergeExistingRowWithPayload (Story 0.1 Task 7)', () => {
  it('returns payload unchanged when existing row is null', () => {
    expect(mergeExistingRowWithPayload(null, { category: 'booking' })).toEqual({
      category: 'booking',
    })
  })

  it('returns payload unchanged when existing row is undefined', () => {
    expect(mergeExistingRowWithPayload(undefined, { category: 'booking' })).toEqual({
      category: 'booking',
    })
  })

  it('returns empty object when both inputs are empty/null', () => {
    expect(mergeExistingRowWithPayload(null, null)).toEqual({})
    expect(mergeExistingRowWithPayload(null, {})).toEqual({})
  })

  it('merges existing row + payload; payload wins on overlap', () => {
    const existing = {
      user_id: 'user-1',
      thread_id: 'thread-1',
      category: 'in_progress',
      brand: null,
      updated_at: '2026-01-01T00:00:00Z',
    }
    const payload = { category: 'not_interested', updated_at: '2026-05-27T05:00:00Z' }
    expect(mergeExistingRowWithPayload(existing, payload)).toEqual({
      user_id: 'user-1',
      thread_id: 'thread-1',
      category: 'not_interested', // payload wins
      brand: null, // preserved from existing (null is a valid value)
      updated_at: '2026-05-27T05:00:00Z', // payload wins
    })
  })

  it("strips 'id' from existing row (buildSxtUpsert uses row.aggregate_id as canonical ID)", () => {
    // Without this, the merged payload would have an 'id' key that buildSxtUpsert
    // would emit as a duplicate ID column in the INSERT clause.
    const existing = { id: 'existing-id-value', user_id: 'u1', category: 'in_progress' }
    const payload = { category: 'completed' }
    const merged = mergeExistingRowWithPayload(existing, payload)
    expect(merged).not.toHaveProperty('id')
    expect(merged).toEqual({ user_id: 'u1', category: 'completed' })
  })

  it('keeps payload null values (intentional clears) over existing non-null', () => {
    const existing = { user_id: 'u1', brand: 'Acme', category: 'in_progress' }
    const payload = { brand: null }
    expect(mergeExistingRowWithPayload(existing, payload)).toEqual({
      user_id: 'u1',
      brand: null, // payload's null wins
      category: 'in_progress',
    })
  })

})

/**
 * Story 0.1 Task 7 (PR #24 Copilot review round 2): the worker uses
 * `buildSxtMergedUpsert` to write a SxT statement whose INSERT/VALUES
 * clause carries the FULL merged row (satisfying SxT's NOT NULL
 * validation on the INSERT branch) but whose `DO UPDATE SET` clause
 * touches ONLY the original patch keys — so concurrent out-of-order
 * outbox rows for the same aggregate cannot clobber each other's
 * untouched columns with stale values read from the existing row.
 */
describe('buildSxtMergedUpsert (Story 0.1 Task 7)', () => {
  function realisticMergedScenario() {
    const existing = {
      // id stripped by mergeExistingRowWithPayload (test that flow too)
      user_id: 'user-1',
      thread_id: 'thread-1',
      email_thread_evaluation_id: null,
      deal_name: 'Black Friday',
      deal_type: 'affiliate',
      category: 'in_progress',
      value: null,
      currency: null,
      brand: 'Wellnesse',
      is_ai_sorted: true,
      created_at: '2026-02-12T01:10:58Z',
      updated_at: '2026-02-12T01:10:58Z',
    }
    const patch = {
      category: 'not_interested',
      updated_at: '2026-05-27T05:00:00Z',
      is_ai_sorted: false,
    }
    return { existing, patch, merged: { ...existing, ...patch } }
  }

  it('emits INSERT/VALUES carrying the full merged row', () => {
    const { existing, patch, merged } = realisticMergedScenario()
    const sql = buildSxtMergedUpsert(
      { id: 11, aggregate: 'deal', aggregate_id: 'deal-abc', payload: patch },
      merged,
    )
    // All 12 cols from the merged row show up in the INSERT clause
    expect(sql).toContain('USER_ID')
    expect(sql).toContain('THREAD_ID')
    expect(sql).toContain('BRAND')
    expect(sql).toContain('DEAL_NAME')
    expect(sql).toContain('CREATED_AT')
    expect(sql).toContain('UPDATED_AT')
    expect(sql).toContain('CATEGORY')
    // Existing-row values preserved in VALUES:
    expect(sql).toContain("'Wellnesse'")
    expect(sql).toContain("'Black Friday'")
    // Patch-wins values in VALUES:
    expect(sql).toContain("'not_interested'")
    expect(sql).toContain("'2026-05-27T05:00:00Z'")
  })

  it('emits DO UPDATE SET clause containing ONLY the original patch keys', () => {
    const { patch, merged } = realisticMergedScenario()
    const sql = buildSxtMergedUpsert(
      { id: 11, aggregate: 'deal', aggregate_id: 'deal-abc', payload: patch },
      merged,
    )
    // Extract the DO UPDATE SET clause
    const m = sql.match(/ON CONFLICT \(ID\) DO UPDATE SET ([^\n]+)$/m)
    expect(m).toBeTruthy()
    const updateSet = m[1]
    // The 3 patch keys MUST be in UPDATE SET
    expect(updateSet).toContain('CATEGORY = EXCLUDED.CATEGORY')
    expect(updateSet).toContain('UPDATED_AT = EXCLUDED.UPDATED_AT')
    expect(updateSet).toContain('IS_AI_SORTED = EXCLUDED.IS_AI_SORTED')
    // Untouched merged cols MUST NOT appear in UPDATE SET — that's the
    // race-condition guard against stale-clobber on out-of-order processing.
    expect(updateSet).not.toContain('USER_ID')
    expect(updateSet).not.toContain('THREAD_ID')
    expect(updateSet).not.toContain('BRAND')
    expect(updateSet).not.toContain('DEAL_NAME')
    expect(updateSet).not.toContain('DEAL_TYPE')
    expect(updateSet).not.toContain('CREATED_AT')
    expect(updateSet).not.toContain('VALUE')
    expect(updateSet).not.toContain('CURRENCY')
    expect(updateSet).not.toContain('EMAIL_THREAD_EVALUATION_ID')
  })

  it('rejects empty row.payload (nothing to update)', () => {
    expect(() =>
      buildSxtMergedUpsert(
        { id: 11, aggregate: 'deal', aggregate_id: 'deal-abc', payload: {} },
        { user_id: 'u1', category: 'in_progress' },
      ),
    ).toThrow(/row\.payload is empty/)
  })

  it('rejects null mergedPayload', () => {
    expect(() =>
      buildSxtMergedUpsert(
        { id: 11, aggregate: 'deal', aggregate_id: 'deal-abc', payload: { category: 'x' } },
        null,
      ),
    ).toThrow(/mergedPayload must be a non-null object/)
  })

  it('rejects empty mergedPayload', () => {
    expect(() =>
      buildSxtMergedUpsert(
        { id: 11, aggregate: 'deal', aggregate_id: 'deal-abc', payload: { category: 'x' } },
        {},
      ),
    ).toThrow(/mergedPayload is empty/)
  })

  it('rejects when a patch key is missing from merged (caller bug)', () => {
    expect(() =>
      buildSxtMergedUpsert(
        {
          id: 11,
          aggregate: 'deal',
          aggregate_id: 'deal-abc',
          payload: { category: 'x', some_new_col: 'y' },
        },
        { user_id: 'u1', category: 'in_progress' }, // missing some_new_col
      ),
    ).toThrow(/patch key "some_new_col" missing from mergedPayload/)
  })

  it('rejects unknown aggregate', () => {
    expect(() =>
      buildSxtMergedUpsert(
        {
          id: 11,
          aggregate: 'mystery_aggregate',
          aggregate_id: 'x',
          payload: { category: 'x' },
        },
        { category: 'x', user_id: 'u1' },
      ),
    ).toThrow(/unknown aggregate mystery_aggregate/)
  })

  it("does NOT emit an `ID = EXCLUDED.ID` SET assignment (id is excluded from patch by design)", () => {
    const { patch, merged } = realisticMergedScenario()
    const sql = buildSxtMergedUpsert(
      { id: 11, aggregate: 'deal', aggregate_id: 'deal-abc', payload: patch },
      merged,
    )
    expect(sql).not.toContain('ID = EXCLUDED.ID')
  })
})
