import { evaluations } from '../../src/lib/sql/evaluations.js'

describe('evaluations', () => {
  const S = 'TEST_SCHEMA'

  it('upsert with pre-built VALUE tuples', () => {
    const sql = evaluations.upsert(S, [
      "('id1', 'th-1', '', 'cat', 'summary', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.EMAIL_THREAD_EVALUATIONS`)
    expect(sql).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
    expect(sql).toContain('th-1')
  })

  it('selectByThreadIds selects existing evaluations', () => {
    const sql = evaluations.selectByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain('SELECT THREAD_ID, IS_DEAL')
    expect(sql).toContain(`FROM ${S}.EMAIL_THREAD_EVALUATIONS`)
    expect(sql).toContain("'th-1'")
  })
})
