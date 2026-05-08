import { parsePositiveIntegerInput, parseStrictBoolean } from '../src/lib/inputs.js'

describe('parsePositiveIntegerInput', () => {
  it('parses valid positive integers', () => {
    expect(parsePositiveIntegerInput('1', 'x')).toBe(1)
    expect(parsePositiveIntegerInput('75', 'x')).toBe(75)
    expect(parsePositiveIntegerInput('  500  ', 'x')).toBe(500)
  })

  it('rejects zero, negatives, decimals, and non-numerics', () => {
    expect(() => parsePositiveIntegerInput('0', 'x')).toThrow(/must be a positive integer/)
    expect(() => parsePositiveIntegerInput('-1', 'x')).toThrow(/must be a positive integer/)
    expect(() => parsePositiveIntegerInput('1.5', 'x')).toThrow(/must be a positive integer/)
    expect(() => parsePositiveIntegerInput('abc', 'x')).toThrow(/must be a positive integer/)
    expect(() => parsePositiveIntegerInput('', 'x')).toThrow(/must be a positive integer/)
  })

  it('enforces optional upper bound — fail fast on misconfig', () => {
    expect(parsePositiveIntegerInput('500', 'batch-size', { max: 500 })).toBe(500)
    expect(() =>
      parsePositiveIntegerInput('501', 'batch-size', { max: 500 }),
    ).toThrow(/must be ≤ 500/)
    expect(() =>
      parsePositiveIntegerInput('999999999', 'batch-size', { max: 500 }),
    ).toThrow(/must be ≤ 500/)
  })

  it('without a max, accepts arbitrarily large positive integers (backwards-compat)', () => {
    expect(parsePositiveIntegerInput('999999', 'x')).toBe(999999)
  })
})

describe('parseStrictBoolean', () => {
  it('returns default when input is empty / unset', () => {
    expect(parseStrictBoolean('', 'x', false)).toBe(false)
    expect(parseStrictBoolean(undefined, 'x', true)).toBe(true)
    expect(parseStrictBoolean(null, 'x', false)).toBe(false)
    expect(parseStrictBoolean('   ', 'x', true)).toBe(true)
  })

  it('accepts whitelisted truthy values (case-insensitive)', () => {
    expect(parseStrictBoolean('true', 'x', false)).toBe(true)
    expect(parseStrictBoolean('TRUE', 'x', false)).toBe(true)
    expect(parseStrictBoolean('1', 'x', false)).toBe(true)
    expect(parseStrictBoolean('yes', 'x', false)).toBe(true)
    expect(parseStrictBoolean('  Yes  ', 'x', false)).toBe(true)
  })

  it('accepts whitelisted falsy values (case-insensitive)', () => {
    expect(parseStrictBoolean('false', 'x', true)).toBe(false)
    expect(parseStrictBoolean('FALSE', 'x', true)).toBe(false)
    expect(parseStrictBoolean('0', 'x', true)).toBe(false)
    expect(parseStrictBoolean('no', 'x', true)).toBe(false)
  })

  it('throws on typos rather than silently falling back to default — operational-safety', () => {
    // The whole point of this helper: a typo on a destructive flag like
    // `backfill-dry-run` should fail fast at config-parse, not silently
    // fall through to a live destructive run.
    expect(() => parseStrictBoolean('treu', 'backfill-dry-run', false)).toThrow(
      /must be one of: true, false, 1, 0, yes, no/,
    )
    expect(() => parseStrictBoolean('flase', 'backfill-dry-run', false)).toThrow(
      /must be one of: true, false, 1, 0, yes, no/,
    )
    expect(() => parseStrictBoolean('Y', 'backfill-dry-run', false)).toThrow(
      /must be one of: true, false, 1, 0, yes, no/,
    )
    expect(() => parseStrictBoolean('on', 'backfill-dry-run', false)).toThrow(
      /must be one of: true, false, 1, 0, yes, no/,
    )
  })
})
