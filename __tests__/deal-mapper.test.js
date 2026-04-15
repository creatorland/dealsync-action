import { threadToDealTuple } from '../src/lib/deal-mapper.js'

const baseThread = {
  thread_id: 'thread-abc',
  is_deal: true,
  category: 'in_progress',
  deal_type: 'brand_collaboration',
  deal_name: 'Spring Campaign',
  deal_value: 2500,
  deal_currency: 'USD',
  main_contact: { company: 'Acme Inc.' },
}

describe('threadToDealTuple', () => {
  test('happy path writes numeric deal_value and currency into tuple', () => {
    const tuple = threadToDealTuple(baseThread, { userId: 'user-1' })
    expect(tuple).toContain('2500')
    expect(tuple).toContain("'USD'")
    expect(tuple).toContain("'thread-abc'")
    expect(tuple).toContain("'user-1'")
    expect(tuple).toContain("'Acme Inc.'")
    expect(tuple).toContain("'in_progress'")
    expect(tuple).toContain("'brand_collaboration'")
  })

  test('null deal_value falls back to 0', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_value: null }, { userId: 'u' })
    expect(tuple).toMatch(/, 0, /)
  })

  test('undefined deal_value falls back to 0', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_value: undefined }, { userId: 'u' })
    expect(tuple).toMatch(/, 0, /)
  })

  test('NaN deal_value falls back to 0', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_value: NaN }, { userId: 'u' })
    expect(tuple).toMatch(/, 0, /)
  })

  test('negative deal_value falls back to 0', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_value: -100 }, { userId: 'u' })
    expect(tuple).toMatch(/, 0, /)
  })

  test('non-USD deal_currency lands in tuple', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_currency: 'EUR' }, { userId: 'u' })
    expect(tuple).toContain("'EUR'")
  })

  test('null deal_currency falls back to USD', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_currency: null }, { userId: 'u' })
    expect(tuple).toContain("'USD'")
  })

  test('whitespace-only deal_currency falls back to USD', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_currency: '   ' }, { userId: 'u' })
    expect(tuple).toContain("'USD'")
  })

  test('apostrophe in deal_name is SQL-escaped', () => {
    const tuple = threadToDealTuple(
      { ...baseThread, deal_name: "O'Brien's Deal" },
      { userId: 'u' }
    )
    expect(tuple).toContain("'O''Brien''s Deal'")
  })

  test('null main_contact → empty brand', () => {
    const tuple = threadToDealTuple({ ...baseThread, main_contact: null }, { userId: 'u' })
    // 10th positional value (brand) should be empty-string literal ''
    expect(tuple).toContain("''")
  })

  test('missing company on main_contact → empty brand', () => {
    const tuple = threadToDealTuple({ ...baseThread, main_contact: {} }, { userId: 'u' })
    expect(tuple).toContain("''")
  })

  test('dealId mirrors threadId', () => {
    const tuple = threadToDealTuple(baseThread, { userId: 'u' })
    // first two quoted strings should be equal (dealId, userId, threadId — positions 1 and 3)
    const matches = tuple.match(/'[^']*'/g)
    expect(matches[0]).toBe("'thread-abc'") // dealId
    expect(matches[2]).toBe("'thread-abc'") // threadId
  })

  test('missing userId produces empty userId literal', () => {
    const tuple = threadToDealTuple(baseThread, { userId: '' })
    expect(tuple).toMatch(/'thread-abc', '', 'thread-abc'/)
  })
})
