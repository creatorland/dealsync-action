import {
  normalizeOptionalProjectId,
  parsePositiveIntegerInput,
} from '../src/commands/emit-scan-complete-webhooks.js'

describe('parsePositiveIntegerInput', () => {
  it('accepts valid positive integer values', () => {
    expect(parsePositiveIntegerInput('5', 'scan-complete-webhook-concurrency')).toBe(5)
    expect(parsePositiveIntegerInput(' 12 ', 'scan-complete-webhook-concurrency')).toBe(12)
  })

  it('rejects invalid values', () => {
    expect(() => parsePositiveIntegerInput('', 'scan-complete-webhook-concurrency')).toThrow(
      'scan-complete-webhook-concurrency must be a positive integer',
    )
    expect(() => parsePositiveIntegerInput('abc', 'scan-complete-webhook-concurrency')).toThrow(
      'scan-complete-webhook-concurrency must be a positive integer',
    )
    expect(() => parsePositiveIntegerInput('0', 'scan-complete-webhook-concurrency')).toThrow(
      'scan-complete-webhook-concurrency must be a positive integer',
    )
    expect(() => parsePositiveIntegerInput('5.5', 'scan-complete-webhook-concurrency')).toThrow(
      'scan-complete-webhook-concurrency must be a positive integer',
    )
    expect(() => parsePositiveIntegerInput('5e2', 'scan-complete-webhook-concurrency')).toThrow(
      'scan-complete-webhook-concurrency must be a positive integer',
    )
  })
})

describe('normalizeOptionalProjectId', () => {
  it('trims whitespace-only input to empty string', () => {
    expect(normalizeOptionalProjectId('   ')).toBe('')
  })

  it('preserves non-empty project id values', () => {
    expect(normalizeOptionalProjectId(' creatorland-prod ')).toBe('creatorland-prod')
  })
})
