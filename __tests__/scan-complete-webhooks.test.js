import { jest } from '@jest/globals'
import { scanCompleteEligibility } from '../src/lib/sql/index.js'
import {
  rowToScanCompleteWebhookBody,
  getRowUserId,
  coerceNumber,
  firestoreDocumentHasScanCompleteSentAt,
  userHasScanCompleteSentAt,
  postScanCompleteWebhook,
} from '../src/lib/scan-complete.js'

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
      accessToken: 'token',
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
})

describe('postScanCompleteWebhook', () => {
  const origFetch = global.fetch

  afterEach(() => {
    global.fetch = origFetch
  })

  it('POSTs JSON with x-shared-secret', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 201,
      text: async () => '',
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
  })
})
