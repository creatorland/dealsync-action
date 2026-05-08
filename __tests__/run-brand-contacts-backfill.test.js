import { jest } from '@jest/globals'
import { generateKeyPairSync } from 'node:crypto'
import {
  runBrandContactsBackfill,
  runPool,
} from '../src/commands/run-brand-contacts-backfill.js'

describe('runBrandContactsBackfill orchestration', () => {
  const origFetch = global.fetch
  const savedEnv = {}
  let saJson

  beforeAll(() => {
    const { privateKey } = generateKeyPairSync('rsa', {
      modulusLength: 2048,
      privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
      publicKeyEncoding: { type: 'spki', format: 'pem' },
    })
    saJson = JSON.stringify({
      client_email: 'svc@project.iam.gserviceaccount.com',
      private_key: privateKey,
      project_id: 'test-project',
    })
  })

  function setInput(name, value) {
    const key = `INPUT_${name.replace(/ /g, '_').toUpperCase()}`
    if (!(key in savedEnv)) savedEnv[key] = process.env[key]
    process.env[key] = value
  }

  beforeEach(() => {
    setInput('dealsync-backend-base-url', 'https://api.example')
    setInput('dealsync-v2-shared-secret', 'shared-secret')
    setInput('firestore-service-account-json', saJson)
    setInput('gcp-project-id', 'test-project')
    setInput('backfill-batch-size', '75')
    setInput('backfill-concurrency', '5')
    setInput('backfill-attribution-tag', 'brand-contacts-backfill')
    setInput('backfill-dry-run', 'false')
    setInput('sxt-auth-url', '')
    setInput('sxt-auth-secret', '')
    setInput('sxt-api-url', '')
    setInput('sxt-biscuit', '')
    setInput('sxt-schema', '')
  })

  afterEach(() => {
    global.fetch = origFetch
    for (const k of Object.keys(savedEnv)) {
      if (savedEnv[k] === undefined) delete process.env[k]
      else process.env[k] = savedEnv[k]
    }
  })

  function makeFirestoreDoc(userId, permissionTier) {
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

  it('7-cohort matrix: eligible, revoked, full-tier, no-tier, no-token, circuit-broken, already-dispatched', async () => {
    const dispatchedUsers = []
    const skippedInProgress = []

    global.fetch = jest.fn(async (url, init) => {
      const u = String(url)
      const method = (init && init.method) || 'GET'

      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'oauth-tok' }) }
      }

      if (u.includes(':runQuery')) {
        return {
          ok: true,
          status: 200,
          json: async () => [
            // Cohort A — eligible
            makeFirestoreDoc('user-eligible', { tier: 'readonly', tierRevokedAt: null }),
            // Cohort B — revoked
            makeFirestoreDoc('user-revoked', {
              tier: 'readonly',
              tierRevokedAt: '2026-04-01T00:00:00Z',
            }),
            // Cohort C — full-tier (silently filtered)
            makeFirestoreDoc('user-full', { tier: 'full', tierRevokedAt: null }),
            // Cohort F — circuit-broken
            makeFirestoreDoc('user-circuit-broken', {
              tier: 'readonly',
              tierRevokedAt: null,
              backfillCircuitBrokenAt: '2026-05-01T00:00:00Z',
            }),
            // Cohort G — already-dispatched (NFR-1: persistent dispatch marker
            // prevents re-dispatching the same users every cron firing).
            makeFirestoreDoc('user-already-dispatched', {
              tier: 'readonly',
              tierRevokedAt: null,
              backfillDispatchedAt: '2026-05-07T13:00:00Z',
            }),
          ],
        }
      }

      if (u.includes('users-sensitive-data') && method === 'GET') {
        if (u.includes('user-eligible')) {
          return {
            ok: true,
            status: 200,
            body: { cancel: jest.fn().mockResolvedValue(undefined) },
          }
        }
        return {
          ok: false,
          status: 404,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      if (u.includes('/v1/dealsync-v2/sync/ingestion-trigger') && method === 'POST') {
        const body = JSON.parse(init.body)
        dispatchedUsers.push(body.userId)
        return {
          ok: true,
          status: 201,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      if (u.includes(':commit') && method === 'POST') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      throw new Error(`unexpected fetch: ${method} ${u}`)
    })

    const summary = await runBrandContactsBackfill()

    expect(summary.usersConsidered).toBe(5)
    expect(summary.usersEligible).toBe(1)
    expect(summary.usersSkippedRevoked).toBe(1)
    expect(summary.usersSkippedAlreadyInFlight).toBe(1)
    expect(summary.usersSkippedAlreadyDispatched).toBe(1)
    expect(summary.dispatched).toBe(1)
    expect(summary.dispatchFailed).toBe(0)
    expect(summary.correlationId).toBeDefined()
    expect(summary.attributionTag).toBe('brand-contacts-backfill')
    expect(dispatchedUsers).toEqual(['user-eligible'])
    // Cohort C (full-tier) is silently filtered: contributes to usersConsidered
    // but NOT to any skip counter. Sum of explicit cohort tallies must be 4 (A+B+F+G),
    // not 5 — proves user-full was not miscounted into a skip bucket.
    expect(
      summary.usersEligible +
        summary.usersSkippedRevoked +
        summary.usersSkippedAlreadyInFlight +
        summary.usersSkippedAlreadyDispatched,
    ).toBe(4)
    expect(summary.usersSkippedNoToken).toBe(0)
  })

  it('Cohort D: throws AND emits structured error log on user missing permissionTier field group', async () => {
    const stdoutWrites = []
    const origWrite = process.stdout.write.bind(process.stdout)
    process.stdout.write = (chunk, ...rest) => {
      stdoutWrites.push(String(chunk))
      return origWrite(chunk, ...rest)
    }

    global.fetch = jest.fn(async (url) => {
      const u = String(url)
      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'tok' }) }
      }
      if (u.includes(':runQuery')) {
        return {
          ok: true,
          status: 200,
          json: async () => [makeFirestoreDoc('user-no-tier', null)],
        }
      }
      if (u.includes(':commit') && method === 'POST') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      throw new Error(`unexpected fetch: ${u}`)
    })

    try {
      await expect(runBrandContactsBackfill()).rejects.toThrow(/permissionTier field group/)
    } finally {
      process.stdout.write = origWrite
    }

    const all = stdoutWrites.join('')
    expect(all).toContain('::error')
    expect(all).toContain('user-no-tier')
    expect(all).toMatch(/permissionTier/)
    expect(all).toMatch(/migration gap/)
  })

  it('Cohort E: skips user with no legacy token, counted in usersSkippedNoToken', async () => {
    global.fetch = jest.fn(async (url, init) => {
      const u = String(url)
      const method = (init && init.method) || 'GET'
      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'tok' }) }
      }
      if (u.includes(':runQuery')) {
        return {
          ok: true,
          status: 200,
          json: async () => [
            makeFirestoreDoc('user-no-token', { tier: 'readonly', tierRevokedAt: null }),
          ],
        }
      }
      if (u.includes('users-sensitive-data') && method === 'GET') {
        return {
          ok: false,
          status: 404,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }
      if (u.includes(':commit') && method === 'POST') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      throw new Error(`unexpected fetch: ${method} ${u}`)
    })

    const summary = await runBrandContactsBackfill()
    expect(summary.usersSkippedNoToken).toBe(1)
    expect(summary.dispatched).toBe(0)
  })

  it('AC4: backend 409 increments dispatchSkippedAlreadyInProgress, not dispatchFailed', async () => {
    global.fetch = jest.fn(async (url, init) => {
      const u = String(url)
      const method = (init && init.method) || 'GET'
      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'tok' }) }
      }
      if (u.includes(':runQuery')) {
        return {
          ok: true,
          status: 200,
          json: async () => [
            makeFirestoreDoc('user-in-flight', { tier: 'readonly', tierRevokedAt: null }),
          ],
        }
      }
      if (u.includes('users-sensitive-data') && method === 'GET') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }
      if (u.includes('/v1/dealsync-v2/sync/ingestion-trigger') && method === 'POST') {
        return {
          ok: false,
          status: 409,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }
      if (u.includes(':commit') && method === 'POST') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      throw new Error(`unexpected fetch: ${method} ${u}`)
    })

    const summary = await runBrandContactsBackfill()
    expect(summary.dispatchSkippedAlreadyInProgress).toBe(1)
    expect(summary.dispatchFailed).toBe(0)
    expect(summary.dispatched).toBe(0)
  })

  it('AC4: backend 5xx increments dispatchFailed and run continues', async () => {
    global.fetch = jest.fn(async (url, init) => {
      const u = String(url)
      const method = (init && init.method) || 'GET'
      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'tok' }) }
      }
      if (u.includes(':runQuery')) {
        return {
          ok: true,
          status: 200,
          json: async () => [
            makeFirestoreDoc('user-fail', { tier: 'readonly', tierRevokedAt: null }),
            makeFirestoreDoc('user-ok', { tier: 'readonly', tierRevokedAt: null }),
          ],
        }
      }
      if (u.includes('users-sensitive-data') && method === 'GET') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }
      if (u.includes('/v1/dealsync-v2/sync/ingestion-trigger') && method === 'POST') {
        const body = JSON.parse(init.body)
        if (body.userId === 'user-fail') {
          return {
            ok: false,
            status: 500,
            text: jest.fn().mockResolvedValue('internal error'),
          }
        }
        return {
          ok: true,
          status: 201,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }
      if (u.includes(':commit') && method === 'POST') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      throw new Error(`unexpected fetch: ${method} ${u}`)
    })

    const summary = await runBrandContactsBackfill()
    expect(summary.dispatchFailed).toBe(1)
    expect(summary.dispatchFailedBackend).toBe(1)
    expect(summary.dispatchFailedTokenCheck).toBe(0)
    expect(summary.dispatched).toBe(1)
  })

  it('Fix #4: token-check failures are tallied separately from backend failures', async () => {
    // user-firestore-flake: Firestore token-check returns 503 (after exhausted
    // retries) → dispatchFailedTokenCheck.
    // user-backend-fail: Firestore says token present, backend POST returns 500 →
    // dispatchFailedBackend. Aggregate `dispatchFailed` = 2 (preserves
    // backwards-compat alerting); the split fields surface which dependency
    // is broken without grepping logs.
    global.fetch = jest.fn(async (url, init) => {
      const u = String(url)
      const method = (init && init.method) || 'GET'
      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'tok' }) }
      }
      if (u.includes(':runQuery')) {
        return {
          ok: true,
          status: 200,
          json: async () => [
            makeFirestoreDoc('user-firestore-flake', {
              tier: 'readonly',
              tierRevokedAt: null,
            }),
            makeFirestoreDoc('user-backend-fail', { tier: 'readonly', tierRevokedAt: null }),
          ],
        }
      }
      if (u.includes('users-sensitive-data/user-firestore-flake') && method === 'GET') {
        // 503 across all retry attempts.
        return {
          ok: false,
          status: 503,
          text: async () => 'service unavailable',
        }
      }
      if (u.includes('users-sensitive-data') && method === 'GET') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }
      if (u.includes('/v1/dealsync-v2/sync/ingestion-trigger') && method === 'POST') {
        return {
          ok: false,
          status: 500,
          text: jest.fn().mockResolvedValue('internal error'),
        }
      }
      if (u.includes(':commit') && method === 'POST') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      throw new Error(`unexpected fetch: ${method} ${u}`)
    })

    const summary = await runBrandContactsBackfill()
    expect(summary.dispatched).toBe(0)
    expect(summary.dispatchFailedTokenCheck).toBe(1)
    expect(summary.dispatchFailedBackend).toBe(1)
    // Legacy aggregate stays accurate for backwards-compat alerts.
    expect(summary.dispatchFailed).toBe(2)
  })

  it('Fix #6: runPool surfaces escaped worker exceptions via onWorkerException callback', async () => {
    // Direct unit test of the pool's exception-surfacing contract — the
    // orchestrator's workerFn is comprehensively try/catched in production,
    // so we exercise the pool boundary directly. Without onWorkerException
    // wired, an unexpected TypeError from a future workerFn change would be
    // swallowed into a `console.log` and the action would exit 0.
    const claimed = ['u1', 'u2', 'u3']
    let claimIdx = 0
    const claimFn = async () => {
      if (claimIdx >= claimed.length) return null
      return { userId: claimed[claimIdx++] }
    }

    const exceptions = []
    const workerFn = async ({ userId }) => {
      // Simulate a programming bug that escapes whatever try/catch the worker
      // had in its previous incarnation.
      if (userId === 'u2') {
        throw new TypeError(`synthetic escape for ${userId}`)
      }
    }

    await runPool(claimFn, workerFn, {
      maxConcurrent: 1,
      onWorkerException: (err) => exceptions.push(err),
    })

    expect(exceptions).toHaveLength(1)
    expect(exceptions[0]).toBeInstanceOf(TypeError)
    expect(exceptions[0].message).toBe('synthetic escape for u2')
    // Crucially: u3 still ran (the pool didn't abort on u2's failure).
    expect(claimIdx).toBe(3)
  })

  it('body-shape locked: POST carries exactly userId, syncStrategy, origin, attributionTag, dryRun', async () => {
    let capturedBody = null

    global.fetch = jest.fn(async (url, init) => {
      const u = String(url)
      const method = (init && init.method) || 'GET'
      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'tok' }) }
      }
      if (u.includes(':runQuery')) {
        return {
          ok: true,
          status: 200,
          json: async () => [
            makeFirestoreDoc('user-shape', { tier: 'readonly', tierRevokedAt: null }),
          ],
        }
      }
      if (u.includes('users-sensitive-data') && method === 'GET') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }
      if (u.includes('/v1/dealsync-v2/sync/ingestion-trigger') && method === 'POST') {
        capturedBody = JSON.parse(init.body)
        return {
          ok: true,
          status: 201,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }
      if (u.includes(':commit') && method === 'POST') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      throw new Error(`unexpected fetch: ${method} ${u}`)
    })

    await runBrandContactsBackfill()

    expect(capturedBody).toEqual({
      userId: 'user-shape',
      syncStrategy: 'LOOKBACK',
      origin: 'backfill',
      attributionTag: 'brand-contacts-backfill',
      dryRun: false,
    })
    expect(capturedBody).not.toHaveProperty('lookbackDaysOverride')
    expect(capturedBody).not.toHaveProperty('originatingSyncStateId')
    expect(Object.keys(capturedBody).sort()).toEqual(
      ['attributionTag', 'dryRun', 'origin', 'syncStrategy', 'userId'].sort(),
    )
  })

  it('emits backfill_run_complete structured log line', async () => {
    const consoleSpy = jest.spyOn(console, 'log')

    global.fetch = jest.fn(async (url) => {
      const u = String(url)
      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'tok' }) }
      }
      if (u.includes(':runQuery')) {
        return { ok: true, status: 200, json: async () => [] }
      }
      if (u.includes(':commit') && method === 'POST') {
        return {
          ok: true,
          status: 200,
          body: { cancel: jest.fn().mockResolvedValue(undefined) },
        }
      }

      throw new Error(`unexpected fetch: ${u}`)
    })

    await runBrandContactsBackfill()

    const logCalls = consoleSpy.mock.calls.map(([arg]) => arg)
    const completeLine = logCalls.find(
      (line) => typeof line === 'string' && line.includes('backfill_run_complete'),
    )
    expect(completeLine).toBeDefined()
    const parsed = JSON.parse(completeLine)
    expect(parsed.event).toBe('backfill_run_complete')
    expect(parsed.correlationId).toBeDefined()
    expect(parsed.attributionTag).toBe('brand-contacts-backfill')

    consoleSpy.mockRestore()
  })
})
