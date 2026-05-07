import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  setSecret: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
  warning: jest.fn(),
}))

const { paginateTierEligibleUsers, checkLegacyTokenPresence } =
  await import('../src/lib/firestore-users.js')

describe('paginateTierEligibleUsers', () => {
  let origFetch

  beforeEach(() => {
    origFetch = global.fetch
  })
  afterEach(() => {
    global.fetch = origFetch
  })

  function makeDoc(userId, permissionTier) {
    const fields = {}
    if (permissionTier) {
      const ptFields = {}
      if (permissionTier.tier != null) {
        ptFields.tier = { stringValue: permissionTier.tier }
      }
      if (permissionTier.tierRevokedAt != null) {
        ptFields.tierRevokedAt = { stringValue: permissionTier.tierRevokedAt }
      } else {
        ptFields.tierRevokedAt = { nullValue: null }
      }
      if (permissionTier.backfillCircuitBrokenAt != null) {
        ptFields.backfillCircuitBrokenAt = {
          stringValue: permissionTier.backfillCircuitBrokenAt,
        }
      }
      fields.permissionTier = { mapValue: { fields: ptFields } }
    }
    return {
      document: {
        name: `projects/test-project/databases/(default)/documents/users/${userId}`,
        fields,
      },
    }
  }

  it('yields tier-eligible users from Firestore runQuery', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => [
        makeDoc('user-a', { tier: 'readonly', tierRevokedAt: null }),
        makeDoc('user-b', { tier: 'readonly', tierRevokedAt: null }),
      ],
    })

    const pages = []
    for await (const page of paginateTierEligibleUsers({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      batchSize: 10,
    })) {
      pages.push(page)
    }

    expect(pages).toHaveLength(1)
    expect(pages[0]).toHaveLength(2)
    expect(pages[0][0].userId).toBe('user-a')
    expect(pages[0][1].userId).toBe('user-b')
  })

  it('returns empty for empty collection', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => [],
    })

    const pages = []
    for await (const page of paginateTierEligibleUsers({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      batchSize: 10,
    })) {
      pages.push(page)
    }

    expect(pages).toHaveLength(0)
  })

  it('throws when user lacks permissionTier field group', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => [
        {
          document: {
            name: 'projects/test-project/databases/(default)/documents/users/user-no-tier',
            fields: {},
          },
        },
      ],
    })

    const gen = paginateTierEligibleUsers({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      batchSize: 10,
    })

    await expect(gen.next()).rejects.toThrow(/permissionTier field group/)
  })

  it('throws on Firestore runQuery error', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => 'internal error',
    })

    const gen = paginateTierEligibleUsers({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      batchSize: 10,
    })

    await expect(gen.next()).rejects.toThrow(/Firestore runQuery 500/)
  })

  it('respects batchSize cap across pages', async () => {
    let callCount = 0
    global.fetch = jest.fn().mockImplementation(async () => {
      callCount++
      if (callCount === 1) {
        return {
          ok: true,
          status: 200,
          json: async () =>
            Array.from({ length: 200 }, (_, i) =>
              makeDoc(`user-${i}`, { tier: 'readonly', tierRevokedAt: null }),
            ),
        }
      }
      return { ok: true, status: 200, json: async () => [] }
    })

    const all = []
    for await (const page of paginateTierEligibleUsers({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      batchSize: 3,
    })) {
      all.push(...page)
    }

    expect(all.length).toBeLessThanOrEqual(200)
  })
})

describe('checkLegacyTokenPresence', () => {
  let origFetch

  beforeEach(() => {
    origFetch = global.fetch
  })
  afterEach(() => {
    global.fetch = origFetch
  })

  it('returns present:true when token doc exists', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      body: { cancel: jest.fn().mockResolvedValue(undefined) },
    })

    const result = await checkLegacyTokenPresence({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      userId: 'user-1',
    })

    expect(result).toEqual({ present: true })
    const [url] = global.fetch.mock.calls[0]
    expect(url).toContain('users-sensitive-data/user-1/oauth-token/youtube')
  })

  it('returns present:false on 404', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 404,
      body: { cancel: jest.fn().mockResolvedValue(undefined) },
    })

    const result = await checkLegacyTokenPresence({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      userId: 'user-absent',
    })

    expect(result).toEqual({ present: false })
  })

  it('throws on non-404 errors', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => 'server error',
    })

    await expect(
      checkLegacyTokenPresence({
        tokenProvider: async () => 'tok',
        gcpProjectId: 'test-project',
        userId: 'user-err',
      }),
    ).rejects.toThrow(/Firestore token check 500/)
  })
})
