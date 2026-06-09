import { DEALSYNC_IDENTITY_NAMESPACE, deriveSupabaseUserId } from '../src/lib/identity.js'

// ===========================================================================
// AC4 — Namespace parity (cross-repo invariant)
// ===========================================================================

describe('DEALSYNC_IDENTITY_NAMESPACE', () => {
  it('equals the canonical ADR-008 literal (namespace parity, AC4)', () => {
    // If this changes, every cross-repo UUID derivation diverges permanently.
    expect(DEALSYNC_IDENTITY_NAMESPACE).toBe('5ced37e1-0ede-40a0-98aa-ae066dac4ce1')
  })
})

// ===========================================================================
// deriveSupabaseUserId
// ===========================================================================

describe('deriveSupabaseUserId', () => {
  it('golden vector: deriveSupabaseUserId("38Jeic1UdHYI8wwJQyrPu") matches the ADR-008 shared value (AC4)', () => {
    // Notation: ADR-008/Postgres write this as UUIDv5(namespace, name); the Node
    // `uuid` lib (and this helper) call it v5(name, namespace) — same value,
    // reversed arg order (see identity.js). The expected is a hardcoded literal —
    // NOT recomputed through the same helper. A self-referential test (helper on
    // both sides) passes even when the arg order is swapped; the literal catches
    // that class of bug. Canonical value pinned by ADR-008 and Story 2.11 (shared
    // across Stories 1.10, 4.11, 6.2).
    expect(deriveSupabaseUserId('38Jeic1UdHYI8wwJQyrPu')).toBe(
      '216139fd-5fca-5375-8b38-6352cb35d12a',
    )
  })

  it('trims leading/trailing whitespace before hashing — padded uid === golden vector', () => {
    // Pin the trimmed result to the external golden literal (not just helper-to-
    // helper): a padded Firestore uid must derive the SAME canonical value as the
    // trimmed sub, otherwise the row lands under a UUID no session can reproduce
    // (RLS orphan). Asserting against the literal also catches a regression that
    // swaps trim for some other (still symmetric) normalization.
    expect(deriveSupabaseUserId('  38Jeic1UdHYI8wwJQyrPu  ')).toBe(
      '216139fd-5fca-5375-8b38-6352cb35d12a',
    )
  })

  it('throws on blank string', () => {
    expect(() => deriveSupabaseUserId('')).toThrow('non-blank')
  })

  it('throws on whitespace-only string — must not hash "" into a phantom tenant', () => {
    expect(() => deriveSupabaseUserId('   ')).toThrow('non-blank')
  })

  it('throws a TypeError with a type-specific message on non-string input', () => {
    // A null/undefined/numeric uid is a caller type error, not a blank string —
    // the message must name the real fault rather than misreport "non-blank".
    expect(() => deriveSupabaseUserId(null)).toThrow(TypeError)
    expect(() => deriveSupabaseUserId(undefined)).toThrow('must be a string')
    expect(() => deriveSupabaseUserId(123)).toThrow('got number')
  })

  it('is deterministic — same uid always yields same UUID', () => {
    const uid = '38Jeic1UdHYI8wwJQyrPu'
    expect(deriveSupabaseUserId(uid)).toBe(deriveSupabaseUserId(uid))
  })

  it('produces different UUIDs for different uids', () => {
    expect(deriveSupabaseUserId('user-a')).not.toBe(deriveSupabaseUserId('user-b'))
  })
})
