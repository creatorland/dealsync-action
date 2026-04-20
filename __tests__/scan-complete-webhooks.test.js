import { jest } from '@jest/globals'
import { generateKeyPairSync } from 'node:crypto'
import { scanCompleteEligibility } from '../src/lib/sql/index.js'
import {
  getGoogleDatastoreAccessToken,
  rowToScanCompleteWebhookBody,
  getRowUserId,
  coerceNumber,
  firestoreDocumentHasScanCompleteSentAt,
  userHasScanCompleteSentAt,
  postScanCompleteWebhook,
} from '../src/lib/scan-complete.js'

function decodeBase64UrlJson(segment) {
  const pad = segment.length % 4 === 0 ? '' : '='.repeat(4 - (segment.length % 4))
  const base64 = segment.replace(/-/g, '+').replace(/_/g, '/') + pad
  return JSON.parse(Buffer.from(base64, 'base64').toString('utf8'))
}

describe('scanCompleteEligibility.selectEligibleUsers', () => {
  it('substitutes sanitized schemas', () => {
    const sql = scanCompleteEligibility.selectEligibleUsers('EMAIL_CORE_STAGING', 'DEALSYNC_STG_V1')
    expect(sql).toContain('EMAIL_CORE_STAGING.sync_states')
    expect(sql).toContain('DEALSYNC_STG_V1.deal_states')
  })

  it('rejects invalid schema', () => {
    expect(() => scanCompleteEligibility.selectEligibleUsers('bad;schema', 'X')).toThrow(
      'Invalid schema',
    )
  })

  it('coalesces processed_messages to zero for parity', () => {
    const sql = scanCompleteEligibility.selectEligibleUsers('EMAIL_CORE_STAGING', 'DEALSYNC_STG_V1')
    expect(sql).toContain('COALESCE((')
    expect(sql).toContain('), 0) AS processed_messages')
  })
})

describe('scan-complete row → webhook body', () => {
  it('maps UPPERCASE SxT columns', () => {
    const body = rowToScanCompleteWebhookBody({
      USER_ID: 'u1',
      DB_NEW: 1,
      DB_IN_PROGRESS: 2,
      DB_COMPLETED: 3,
      DB_NOT_INTERESTED: 4,
      DB_LIKELY_SCAM: 5,
      DB_LOW_CONFIDENCE: 6,
      CONTACTS_ADDED: 7,
    })
    expect(body).toEqual({
      userId: 'u1',
      eventType: 'scan_complete',
      eventData: {
        dealCounts: {
          new: 1,
          inProgress: 2,
          completed: 3,
          likelyScam: 5,
          lowConfidence: 6,
          notInterested: 4,
        },
        contactsAdded: 7,
      },
    })
  })

  it('getRowUserId accepts user_id', () => {
    expect(getRowUserId({ user_id: 'abc' })).toBe('abc')
  })

  it('coerceNumber handles string decimals', () => {
    expect(coerceNumber('9')).toBe(9)
  })
})

describe('firestoreDocumentHasScanCompleteSentAt', () => {
  it('is false when field missing', () => {
    expect(firestoreDocumentHasScanCompleteSentAt({ fields: {} })).toBe(false)
    expect(firestoreDocumentHasScanCompleteSentAt(null)).toBe(false)
  })

  it('is true for integerValue', () => {
    expect(
      firestoreDocumentHasScanCompleteSentAt({
        fields: { scanCompleteSentAt: { integerValue: '1710000000' } },
      }),
    ).toBe(true)
  })

  it('is false for non-numeric integerValue strings', () => {
    expect(
      firestoreDocumentHasScanCompleteSentAt({
        fields: { scanCompleteSentAt: { integerValue: 'not-a-timestamp' } },
      }),
    ).toBe(false)
  })

  it('is true for doubleValue', () => {
    expect(
      firestoreDocumentHasScanCompleteSentAt({
        fields: { scanCompleteSentAt: { doubleValue: 1710000000 } },
      }),
    ).toBe(true)
  })
})

describe('userHasScanCompleteSentAt', () => {
  const origFetch = global.fetch

  afterEach(() => {
    global.fetch = origFetch
  })

  it('requests only scanCompleteSentAt via field mask', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ fields: { scanCompleteSentAt: { integerValue: '1710000000' } } }),
    })

    const hasSentAt = await userHasScanCompleteSentAt({
      projectId: 'creatorland-prod',
      userId: 'user-1',
      getAccessToken: async () => 'token',
    })

    expect(hasSentAt).toBe(true)
    expect(global.fetch).toHaveBeenCalledWith(
      'https://firestore.googleapis.com/v1/projects/creatorland-prod/databases/(default)/documents/users/user-1?mask.fieldPaths=scanCompleteSentAt',
      expect.objectContaining({
        method: 'GET',
        headers: expect.objectContaining({ Authorization: 'Bearer token' }),
      }),
    )
  })

  it('retries on 429 then succeeds', async () => {
    let call = 0
    global.fetch = jest.fn(async () => {
      call++
      if (call === 1) {
        return {
          ok: false,
          status: 429,
          headers: { get: (h) => (h.toLowerCase() === 'retry-after' ? '0' : null) },
          body: { cancel: async () => {} },
          text: async () => 'too many',
        }
      }
      return {
        ok: true,
        status: 200,
        json: async () => ({ fields: { scanCompleteSentAt: { integerValue: '1710000000' } } }),
      }
    })

    const hasSentAt = await userHasScanCompleteSentAt({
      projectId: 'p',
      userId: 'u',
      getAccessToken: async () => 'tok',
    })
    expect(hasSentAt).toBe(true)
    expect(global.fetch).toHaveBeenCalledTimes(2)
  })

  it('does not retry on 403 (non-retryable client error)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 403,
      text: async () => 'forbidden',
    })
    await expect(
      userHasScanCompleteSentAt({
        projectId: 'p',
        userId: 'u',
        getAccessToken: async () => 'tok',
      }),
    ).rejects.toThrow(/Firestore GET 403:/)
    expect(global.fetch).toHaveBeenCalledTimes(1)
  })
})

describe('postScanCompleteWebhook', () => {
  const origFetch = global.fetch

  afterEach(() => {
    global.fetch = origFetch
  })

  it('POSTs JSON with x-shared-secret', async () => {
    const text = jest.fn().mockResolvedValue('')
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 201,
      text,
    })
    const body = {
      userId: 'u',
      eventType: 'scan_complete',
      eventData: { dealCounts: {}, contactsAdded: 0 },
    }
    const r = await postScanCompleteWebhook('https://api.example.com/', 'sec', body)
    expect(r.ok).toBe(true)
    expect(global.fetch).toHaveBeenCalledWith(
      'https://api.example.com/dealsync-v2/webhooks',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({
          'Content-Type': 'application/json',
          'x-shared-secret': 'sec',
        }),
      }),
    )
    const call = global.fetch.mock.calls[0]
    expect(JSON.parse(call[1].body)).toEqual(body)
    expect(text).not.toHaveBeenCalled()
  })

  it('reads response body only on non-2xx', async () => {
    const text = jest.fn().mockResolvedValue('bad request')
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 400,
      text,
    })

    const result = await postScanCompleteWebhook('https://api.example.com', 'sec', {
      userId: 'u',
      eventType: 'scan_complete',
      eventData: { dealCounts: {}, contactsAdded: 0 },
    })

    expect(result).toEqual({ ok: false, status: 400, text: 'bad request' })
    expect(text).toHaveBeenCalledTimes(1)
  })

  it('retries on 429 then succeeds', async () => {
    let call = 0
    global.fetch = jest.fn(async () => {
      call++
      if (call === 1) {
        return {
          ok: false,
          status: 429,
          headers: { get: (h) => (h.toLowerCase() === 'retry-after' ? '0' : null) },
          text: async () => 'slow down',
        }
      }
      return { ok: true, status: 201, body: { cancel: async () => {} } }
    })

    const result = await postScanCompleteWebhook('https://api.example.com', 'sec', {
      userId: 'u',
      eventType: 'scan_complete',
      eventData: { dealCounts: {}, contactsAdded: 0 },
    })
    expect(result).toEqual({ ok: true, status: 201, text: '' })
    expect(global.fetch).toHaveBeenCalledTimes(2)
  })

  it('does not retry on 400 (client error)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 400,
      text: async () => 'bad',
    })
    const result = await postScanCompleteWebhook('https://api.example.com', 'sec', {
      userId: 'u',
      eventType: 'scan_complete',
      eventData: { dealCounts: {}, contactsAdded: 0 },
    })
    expect(result).toEqual({ ok: false, status: 400, text: 'bad' })
    expect(global.fetch).toHaveBeenCalledTimes(1)
  })
})

describe('getGoogleDatastoreAccessToken', () => {
  const origFetch = global.fetch

  afterEach(() => {
    global.fetch = origFetch
  })

  it('forms OAuth JWT bearer request with expected audience and scope', async () => {
    const { privateKey } = generateKeyPairSync('rsa', {
      modulusLength: 2048,
      privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
      publicKeyEncoding: { type: 'spki', format: 'pem' },
    })
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ access_token: 'tok-123' }),
    })

    const token = await getGoogleDatastoreAccessToken({
      client_email: 'svc@project.iam.gserviceaccount.com',
      private_key: privateKey,
    })

    expect(token).toBe('tok-123')
    expect(global.fetch).toHaveBeenCalledWith(
      'https://oauth2.googleapis.com/token',
      expect.objectContaining({
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      }),
    )

    const call = global.fetch.mock.calls[0][1]
    expect(call.body.get('grant_type')).toBe('urn:ietf:params:oauth:grant-type:jwt-bearer')
    const assertion = call.body.get('assertion')
    expect(typeof assertion).toBe('string')
    expect(assertion.length).toBeGreaterThan(20)

    const [, payloadSegment] = assertion.split('.')
    const payload = decodeBase64UrlJson(payloadSegment)
    expect(payload.aud).toBe('https://oauth2.googleapis.com/token')
    expect(payload.scope).toBe('https://www.googleapis.com/auth/datastore')
  })

  it('throws when oauth response is non-2xx', async () => {
    const { privateKey } = generateKeyPairSync('rsa', {
      modulusLength: 2048,
      privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
      publicKeyEncoding: { type: 'spki', format: 'pem' },
    })
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 403,
      json: async () => ({ error: 'forbidden' }),
    })

    await expect(
      getGoogleDatastoreAccessToken({
        client_email: 'svc@project.iam.gserviceaccount.com',
        private_key: privateKey,
      }),
    ).rejects.toThrow('OAuth token 403:')
  })

  it('throws when access_token is missing from oauth response', async () => {
    const { privateKey } = generateKeyPairSync('rsa', {
      modulusLength: 2048,
      privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
      publicKeyEncoding: { type: 'spki', format: 'pem' },
    })
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({}),
    })

    await expect(
      getGoogleDatastoreAccessToken({
        client_email: 'svc@project.iam.gserviceaccount.com',
        private_key: privateKey,
      }),
    ).rejects.toThrow('OAuth response missing access_token')
  })
})
