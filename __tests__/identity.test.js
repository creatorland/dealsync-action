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
  it('golden vector: UUIDv5(NS, "38Jeic1UdHYI8wwJQyrPu") matches ADR-008 shared value (AC4)', () => {
    // Expected is a hardcoded literal — NOT recomputed through the same helper.
    // A self-referential test (helper on both sides) passes even when arg order
    // is swapped; the literal catches that class of bug. Canonical value is
    // pinned by ADR-008 and Story 2.11 (shared across Stories 1.10, 4.11, 6.2).
    expect(deriveSupabaseUserId('38Jeic1UdHYI8wwJQyrPu')).toBe(
      '216139fd-5fca-5375-8b38-6352cb35d12a',
    )
  })

  it('trims leading/trailing whitespace before hashing — padded uid === trimmed uid', () => {
    // A padded Firestore uid must hash to the same value as its trimmed form,
    // otherwise the row lands under a UUID no session can reproduce (RLS orphan).
    expect(deriveSupabaseUserId('  38Jeic1UdHYI8wwJQyrPu  ')).toBe(
      deriveSupabaseUserId('38Jeic1UdHYI8wwJQyrPu'),
    )
  })

  it('throws on blank string', () => {
    expect(() => deriveSupabaseUserId('')).toThrow('non-blank')
  })

  it('throws on whitespace-only string — must not hash "" into a phantom tenant', () => {
    expect(() => deriveSupabaseUserId('   ')).toThrow('non-blank')
  })

  it('is deterministic — same uid always yields same UUID', () => {
    const uid = '38Jeic1UdHYI8wwJQyrPu'
    expect(deriveSupabaseUserId(uid)).toBe(deriveSupabaseUserId(uid))
  })

  it('produces different UUIDs for different uids', () => {
    expect(deriveSupabaseUserId('user-a')).not.toBe(deriveSupabaseUserId('user-b'))
  })
})
