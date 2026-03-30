import { audits } from '../../src/lib/sql/audits.js'

describe('audits', () => {
  const S = 'TEST_SCHEMA'

  it('selectByBatch selects audit by batch ID', () => {
    const sql = audits.selectByBatch(S, 'batch-1')
    expect(sql).toContain('SELECT AI_EVALUATION')
    expect(sql).toContain(`FROM ${S}.AI_EVALUATION_AUDITS`)
    expect(sql).toContain("BATCH_ID = 'batch-1'")
  })

  it('insert creates audit row', () => {
    const sql = audits.insert(S, {
      id: 'aud-1',
      batchId: 'batch-1',
      threadCount: 5,
      emailCount: 12,
      cost: 0,
      inputTokens: 100,
      outputTokens: 200,
      model: 'test-model',
      evaluation: '{threads:[]}',
    })
    expect(sql).toContain(`INSERT INTO ${S}.AI_EVALUATION_AUDITS`)
    expect(sql).toContain("'aud-1'")
    expect(sql).toContain("'batch-1'")
    expect(sql).toContain("'test-model'")
  })
})
