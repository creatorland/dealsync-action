import { batchEvents } from '../../src/lib/sql/batch-events.js'

describe('batchEvents', () => {
  const S = 'TEST_SCHEMA'

  it('upsert produces INSERT with ON CONFLICT for single event', () => {
    const sql = batchEvents.upsert(S, 'trigger-1', 'batch-1', 'classify', 'new')
    expect(sql).toContain(`INSERT INTO ${S}.BATCH_EVENTS`)
    expect(sql).toContain("'trigger-1'")
    expect(sql).toContain("'batch-1'")
    expect(sql).toContain('ON CONFLICT (TRIGGER_HASH)')
  })

  it('upsertBulk produces VALUES for pre-built tuples', () => {
    const sql = batchEvents.upsertBulk(S, [
      "('hash1', 'batch1', 'classify', 'complete', CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.BATCH_EVENTS`)
    expect(sql).toContain('hash1')
    expect(sql).toContain('ON CONFLICT (TRIGGER_HASH)')
  })
})
