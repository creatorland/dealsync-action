import { jest } from '@jest/globals'
import { generateKeyPairSync } from 'node:crypto'
import {
  normalizeOptionalProjectId,
  parsePositiveIntegerInput,
  runEmitScanCompleteWebhooks,
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

describe('runEmitScanCompleteWebhooks orchestration', () => {
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
      project_id: 'creatorland-prod',
    })
  })

  function setInput(name, value) {
    const key = `INPUT_${name.replace(/ /g, '_').toUpperCase()}`
    if (!(key in savedEnv)) savedEnv[key] = process.env[key]
    process.env[key] = value
  }

  beforeEach(() => {
    setInput('sxt-auth-url', 'https://auth.example/authenticate')
    setInput('sxt-auth-secret', 'auth-secret')
    setInput('sxt-api-url', 'https://sxt.example')
    setInput('sxt-biscuit', 'biscuit')
    setInput('sxt-schema', 'DEALSYNC_STG_V1')
    setInput('email-core-schema', 'EMAIL_CORE_STAGING')
    setInput('dealsync-backend-base-url', 'https://api.example')
    setInput('dealsync-v2-shared-secret', 'shared')
    setInput('firestore-service-account-json', saJson)
    setInput('scan-complete-webhook-concurrency', '5')
    setInput('scan-complete-batch-size', '500')
    setInput('sxt-rate-limiter-url', '')
    setInput('sxt-rate-limiter-api-key', '')
  })

  afterEach(() => {
    global.fetch = origFetch
    for (const k of Object.keys(savedEnv)) {
      if (savedEnv[k] === undefined) delete process.env[k]
      else process.env[k] = savedEnv[k]
    }
  })

  it('dedupes, posts, and records client errors without writing scanCompleteSentAt', async () => {
    const postedUsers = []
    const firestoreWrites = []

    global.fetch = jest.fn(async (url, init) => {
      const u = String(url)
      const method = (init && init.method) || 'GET'

      if (u.startsWith('https://auth.example/authenticate')) {
        return { ok: true, status: 200, json: async () => ({ data: 'jwt' }) }
      }
      if (u.startsWith('https://sxt.example/v1/sql')) {
        return {
          ok: true,
          status: 200,
          text: async () => '',
          json: async () => [
            { USER_ID: 'u-a', DB_NEW: 1, DB_COMPLETED: 0, CONTACTS_ADDED: 0 },
            { USER_ID: 'u-b', DB_NEW: 0, DB_COMPLETED: 2, CONTACTS_ADDED: 1 },
            { USER_ID: 'u-c', DB_NEW: 0, DB_COMPLETED: 0, CONTACTS_ADDED: 0 },
          ],
        }
      }
      if (u.startsWith('https://oauth2.googleapis.com/token')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'oauth-tok' }) }
      }
      if (u.startsWith('https://firestore.googleapis.com/')) {
        if (method === 'GET') {
          if (u.includes('/users/u-a')) {
            return {
              ok: true,
              status: 200,
              json: async () => ({
                fields: { scanCompleteSentAt: { integerValue: '1710000000' } },
              }),
            }
          }
          return { ok: false, status: 404, text: async () => 'not found' }
        }
        firestoreWrites.push({ method, url: u })
        return { ok: true, status: 200, body: null }
      }
      if (u === 'https://api.example/dealsync-v2/webhooks' && method === 'POST') {
        const body = JSON.parse(init.body)
        postedUsers.push(body.userId)
        if (body.userId === 'u-c') {
          return { ok: false, status: 400, text: async () => 'bad request' }
        }
        return { ok: true, status: 201, body: null }
      }
      throw new Error(`unexpected fetch: ${method} ${u}`)
    })

    const summary = await runEmitScanCompleteWebhooks()

    expect(summary).toEqual({
      correlationId: expect.any(String),
      scanned: 3,
      skippedDeduped: 1,
      posted: 1,
      errors: 1,
    })
    expect(postedUsers.sort()).toEqual(['u-b', 'u-c'])
    // Backend owns scanCompleteSentAt (transactional write in its scan_complete handler).
    // The action must never write this field.
    expect(firestoreWrites).toEqual([])
  })
})
