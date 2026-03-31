import { deals, dealContacts, contacts } from '../../src/lib/sql/deals.js'

describe('deals', () => {
  const S = 'TEST_SCHEMA'

  it('deleteByThreadIds', () => {
    const sql = deals.deleteByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain(`DELETE FROM ${S}.DEALS`)
    expect(sql).toContain("'th-1'")
  })

  it('upsert', () => {
    const sql = deals.upsert(S, [
      "('d1', 'u1', 'th-1', '', 'Deal', 'brand', 'cat', 100, 'USD', 'Co', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.DEALS`)
    expect(sql).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
  })

  it('selectByThreadIds', () => {
    const sql = deals.selectByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain('SELECT ID, THREAD_ID, USER_ID, UPDATED_AT')
    expect(sql).toContain(`FROM ${S}.DEALS`)
    expect(sql).toContain("'th-1'")
  })
})

describe('dealContacts', () => {
  const S = 'TEST_SCHEMA'

  it('deleteByDealIds', () => {
    const sql = dealContacts.deleteByDealIds(S, ["'d-1'", "'d-2'"])
    expect(sql).toContain(`DELETE FROM ${S}.DEAL_CONTACTS`)
    expect(sql).toContain("'d-1'")
  })

  it('upsert', () => {
    const sql = dealContacts.upsert(S, [
      "('d1', 'u1', 'test@co.com', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.DEAL_CONTACTS`)
    expect(sql).toContain('ON CONFLICT (DEAL_ID, USER_ID, EMAIL)')
  })
})

describe('contacts', () => {
  it('upsert', () => {
    const sql = contacts.upsert('CORE_SCHEMA', [
      "('u1', 'alice@co.com', 'Alice', NULL, 'CEO', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain('INSERT INTO CORE_SCHEMA.CONTACTS')
    expect(sql).toContain('ON CONFLICT (USER_ID, EMAIL)')
  })
})
