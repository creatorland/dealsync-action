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

const { paginateTierEligibleUsers, checkLegacyTokenPresence, batchCheckLegacyTokenPresence } =
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
        ptFields.tierRevokedAt = { timestampValue: permissionTier.tierRevokedAt }
      } else {
        ptFields.tierRevokedAt = { nullValue: null }
      }
      if (permissionTier.backfillCircuitBrokenAt != null) {
        ptFields.backfillCircuitBrokenAt = {
          timestampValue: permissionTier.backfillCircuitBrokenAt,
        }
      }
      if (permissionTier.backfillDispatchedAt != null) {
        ptFields.backfillDispatchedAt = {
          timestampValue: permissionTier.backfillDispatchedAt,
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

    // Pin the field projection — runQuery must only fetch permissionTier so Firestore
    // doesn't ship the whole user document (email, settings, etc.) to the runner.
    const [, init] = global.fetch.mock.calls[0]
    const reqBody = JSON.parse(init.body)
    expect(reqBody.structuredQuery.select).toEqual({
      fields: [{ fieldPath: 'permissionTier' }],
    })
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

  it('caller can break early after collecting batchSize candidates — only one fetch happens', async () => {
    // batchSize=3 → fetchChunk = max(3*4, 200) = 200. Mock returns a full 200-doc chunk
    // every call so the natural-end break (docs.length < fetchChunk) does NOT fire —
    // only the caller's break should stop pagination.
    const FETCH_CHUNK = 200
    let callCount = 0
    global.fetch = jest.fn().mockImplementation(async () => {
      callCount++
      return {
        ok: true,
        status: 200,
        json: async () =>
          Array.from({ length: FETCH_CHUNK }, (_, i) =>
            makeDoc(`user-${callCount}-${i}`, { tier: 'readonly', tierRevokedAt: null }),
          ),
      }
    })

    const collected = []
    for await (const page of paginateTierEligibleUsers({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      batchSize: 3,
    })) {
      for (const c of page) {
        collected.push(c)
        if (collected.length >= 3) break
      }
      if (collected.length >= 3) break
    }

    expect(collected.length).toBe(3)
    // One fetch only — the caller's break propagates to the generator's iterator return.
    expect(callCount).toBe(1)
  })

  it('paginates beyond a single page when caller keeps consuming (no premature page cap)', async () => {
    // batchSize=75 → fetchChunk = max(75*4, 200) = 300. Page 1 returns a full chunk
    // (300 docs) so the natural-end break does NOT fire after page 1; the prior cap-on-
    // raw-page-size would have stopped here. Page 2 returns a short page (1 doc) → break.
    const FETCH_CHUNK = 300
    let callCount = 0
    global.fetch = jest.fn().mockImplementation(async () => {
      callCount++
      if (callCount === 1) {
        return {
          ok: true,
          status: 200,
          json: async () =>
            Array.from({ length: FETCH_CHUNK }, (_, i) =>
              makeDoc(`page1-user-${i}`, { tier: 'readonly', tierRevokedAt: null }),
            ),
        }
      }
      if (callCount === 2) {
        return {
          ok: true,
          status: 200,
          json: async () => [makeDoc('page2-user-0', { tier: 'readonly', tierRevokedAt: null })],
        }
      }
      return { ok: true, status: 200, json: async () => [] }
    })

    const all = []
    for await (const page of paginateTierEligibleUsers({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      batchSize: 75,
    })) {
      all.push(...page)
    }

    expect(callCount).toBe(2)
    expect(all.length).toBe(FETCH_CHUNK + 1)
    expect(all.at(-1).userId).toBe('page2-user-0')
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
    // Security guarantee — must NOT fetch full doc body. Pin the field-mask query
    // param so a future refactor can't silently start pulling plaintext OAuth fields.
    expect(url).toContain('mask.fieldPaths=__name__')
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

  it('retries on 5xx and succeeds on a later attempt (transient)', async () => {
    global.fetch = jest
      .fn()
      .mockResolvedValueOnce({
        ok: false,
        status: 503,
        text: async () => 'service unavailable',
      })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        body: { cancel: jest.fn().mockResolvedValue(undefined) },
      })

    const result = await checkLegacyTokenPresence({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      userId: 'user-flaky',
    })

    expect(result).toEqual({ present: true })
    expect(global.fetch).toHaveBeenCalledTimes(2)
  })

  it('retries on network throw (DNS / connection refused / timeout)', async () => {
    global.fetch = jest
      .fn()
      .mockRejectedValueOnce(new TypeError('fetch failed'))
      .mockResolvedValueOnce({
        ok: false,
        status: 404,
        body: { cancel: jest.fn().mockResolvedValue(undefined) },
      })

    const result = await checkLegacyTokenPresence({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      userId: 'user-net-blip',
    })

    expect(result).toEqual({ present: false })
    expect(global.fetch).toHaveBeenCalledTimes(2)
  })

  it('does NOT retry on 4xx (other than 404) — fail fast on auth/permission errors', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 401,
      text: async () => 'unauthorized',
    })

    await expect(
      checkLegacyTokenPresence({
        tokenProvider: async () => 'tok',
        gcpProjectId: 'test-project',
        userId: 'user-auth-fail',
      }),
    ).rejects.toThrow(/Firestore token check 401/)

    // 4xx must short-circuit retry — first attempt only.
    expect(global.fetch).toHaveBeenCalledTimes(1)
  })
})

describe('batchCheckLegacyTokenPresence', () => {
  let origFetch

  beforeEach(() => {
    origFetch = global.fetch
  })
  afterEach(() => {
    global.fetch = origFetch
  })

  it('returns empty Map for empty userIds (no fetch issued)', async () => {
    global.fetch = jest.fn()
    const result = await batchCheckLegacyTokenPresence({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      userIds: [],
    })
    expect(result).toBeInstanceOf(Map)
    expect(result.size).toBe(0)
    expect(global.fetch).not.toHaveBeenCalled()
  })

  it('issues a single batchGet for N userIds and pairs by index', async () => {
    global.fetch = jest.fn(async (url, init) => {
      const reqBody = JSON.parse(init.body)
      // Pin the field-mask security guarantee — batched path must not pull
      // plaintext OAuth fields either.
      expect(reqBody.mask).toEqual({ fieldPaths: ['__name__'] })
      const responses = reqBody.documents.map((docPath, i) => {
        // Even-indexed = found; odd = missing — proves response order
        // pairing is correct.
        if (i % 2 === 0) return { found: { name: docPath } }
        return { missing: docPath }
      })
      return { ok: true, status: 200, json: async () => responses }
    })

    const result = await batchCheckLegacyTokenPresence({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      userIds: ['u-a', 'u-b', 'u-c', 'u-d'],
    })

    expect(global.fetch).toHaveBeenCalledTimes(1)
    expect(result.get('u-a')).toEqual({ present: true })
    expect(result.get('u-b')).toEqual({ present: false })
    expect(result.get('u-c')).toEqual({ present: true })
    expect(result.get('u-d')).toEqual({ present: false })
  })

  it('retries on 5xx and succeeds on a later attempt', async () => {
    global.fetch = jest
      .fn()
      .mockResolvedValueOnce({ ok: false, status: 503, text: async () => 'unavailable' })
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => [{ found: { name: 'doc' } }],
      })

    const result = await batchCheckLegacyTokenPresence({
      tokenProvider: async () => 'tok',
      gcpProjectId: 'test-project',
      userIds: ['u-1'],
    })

    expect(result.get('u-1')).toEqual({ present: true })
    expect(global.fetch).toHaveBeenCalledTimes(2)
  })

  it('throws after retry exhaustion on persistent 5xx — caller charges all candidates to dispatchFailedTokenCheck', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => 'persistent server error',
    })

    await expect(
      batchCheckLegacyTokenPresence({
        tokenProvider: async () => 'tok',
        gcpProjectId: 'test-project',
        userIds: ['u-1', 'u-2'],
      }),
    ).rejects.toThrow(/Firestore batchGet 500/)
  })

  it('does NOT retry on 4xx', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 401,
      text: async () => 'unauthorized',
    })

    await expect(
      batchCheckLegacyTokenPresence({
        tokenProvider: async () => 'tok',
        gcpProjectId: 'test-project',
        userIds: ['u-1', 'u-2'],
      }),
    ).rejects.toThrow(/Firestore batchGet 401/)

    expect(global.fetch).toHaveBeenCalledTimes(1)
  })
})
