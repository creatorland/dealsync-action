import { jest } from '@jest/globals'

// ===========================================================================
// Mocks
// ===========================================================================

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
}))

// ===========================================================================
// Test setup
// ===========================================================================

let core
let writer

beforeAll(async () => {
  core = await import('@actions/core')
  writer = await import('../src/lib/supabase-writer.js')
})

const SUPABASE_URL = 'https://test.supabase.co'
const SERVICE_ROLE_KEY = 'test-service-role-key'

function setupConfig(url = SUPABASE_URL, key = SERVICE_ROLE_KEY) {
  writer._resetConfig()
  core.getInput.mockImplementation((name) => {
    if (name === 'supabase-url') return url
    if (name === 'supabase-service-role-key') return key
    return ''
  })
}

let capturedFetches = []

beforeEach(() => {
  setupConfig()
  capturedFetches = []
  global.fetch = jest.fn(async (url, opts) => {
    capturedFetches.push({ url, opts })
    return { ok: true, text: async () => '' }
  })
})

afterEach(() => {
  delete global.fetch
})

// ===========================================================================
// Helpers
// ===========================================================================

function lastFetch() {
  return capturedFetches[capturedFetches.length - 1]
}

function parsedBody(fetchCall) {
  return JSON.parse(fetchCall.opts.body)
}

// ===========================================================================
// writeAudit
// ===========================================================================

describe('writeAudit', () => {
  it('POSTs to ai_evaluation_audits with ON CONFLICT (id) DO NOTHING', async () => {
    await writer.writeAudit({
      batchId: 'batch-1',
      subjectUserId: 'user-1',
      threadCount: 3,
      emailCount: 10,
      cost: 0,
      inputTokens: 0,
      outputTokens: 0,
      model: 'gpt-4',
      evaluation: '{"threads":[]}',
    })

    const { url, opts } = lastFetch()
    expect(url).toContain('/rest/v1/ai_evaluation_audits')
    expect(url).toContain('on_conflict=id')
    expect(opts.headers['Prefer']).toContain('ignore-duplicates')
    const body = parsedBody(lastFetch())
    expect(body).toHaveLength(1)
    expect(body[0].id).toBe('batch-1')
    expect(body[0].batch_id).toBe('batch-1')
    expect(body[0].subject_user_id).toBe('user-1')
    expect(body[0].thread_count).toBe(3)
    expect(body[0].model_used).toBe('gpt-4')
  })

  it('does not include user-owned columns', async () => {
    await writer.writeAudit({
      batchId: 'b1',
      subjectUserId: 'u1',
      threadCount: 1,
      emailCount: 1,
      cost: 0,
      inputTokens: 0,
      outputTokens: 0,
      model: 'm',
      evaluation: '{}',
    })
    const body = parsedBody(lastFetch())
    expect(body[0]).not.toHaveProperty('category')
    expect(body[0]).not.toHaveProperty('is_deleted')
    expect(body[0]).not.toHaveProperty('updated_at')
  })

  it('throws on non-ok response', async () => {
    global.fetch = jest.fn(async () => ({ ok: false, status: 422, text: async () => 'bad request' }))
    await expect(
      writer.writeAudit({
        batchId: 'b1',
        subjectUserId: 'u1',
        threadCount: 0,
        emailCount: 0,
        cost: 0,
        inputTokens: 0,
        outputTokens: 0,
        model: 'm',
        evaluation: '{}',
      }),
    ).rejects.toThrow('Supabase ai_evaluation_audits upsert failed: 422')
  })
})

// ===========================================================================
// writeEvals
// ===========================================================================

describe('writeEvals', () => {
  it('POSTs to email_thread_evaluations with ON CONFLICT (id) DO UPDATE', async () => {
    await writer.writeEvals([
      {
        threadId: 'th-1',
        subjectUserId: 'user-1',
        auditId: 'audit-1',
        aiInsight: 'brand_deal',
        aiSummary: 'summary',
        isDeal: true,
        likelyScam: false,
        aiScore: 8,
      },
    ])

    const { url, opts } = lastFetch()
    expect(url).toContain('/rest/v1/email_thread_evaluations')
    expect(url).toContain('on_conflict=id')
    expect(opts.headers['Prefer']).toContain('merge-duplicates')
    const body = parsedBody(lastFetch())
    expect(body).toHaveLength(1)
    expect(body[0].id).toBe('th-1')
    expect(body[0].thread_id).toBe('th-1')
    expect(body[0].subject_user_id).toBe('user-1')
    expect(body[0].ai_evaluation_audit_id).toBe('audit-1')
    expect(body[0].is_deal).toBe(true)
    expect(body[0].ai_score).toBe(8)
  })

  it('uses thread_id as the stable id for idempotent re-ingest', async () => {
    const evals = [
      {
        threadId: 'th-stable',
        subjectUserId: 'u1',
        auditId: null,
        aiInsight: null,
        aiSummary: null,
        isDeal: false,
        likelyScam: false,
        aiScore: 3,
      },
    ]
    await writer.writeEvals(evals)
    await writer.writeEvals(evals)

    expect(capturedFetches).toHaveLength(2)
    const body1 = parsedBody(capturedFetches[0])
    const body2 = parsedBody(capturedFetches[1])
    expect(body1[0].id).toBe('th-stable')
    expect(body2[0].id).toBe('th-stable')
  })

  it('does not include user-owned columns (updated_at)', async () => {
    await writer.writeEvals([
      {
        threadId: 'th-1',
        subjectUserId: 'u1',
        auditId: null,
        aiInsight: null,
        aiSummary: null,
        isDeal: false,
        likelyScam: false,
        aiScore: 0,
      },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0]).not.toHaveProperty('updated_at')
    expect(body[0]).not.toHaveProperty('category')
    expect(body[0]).not.toHaveProperty('is_deleted')
  })

  it('is a no-op for empty array', async () => {
    await writer.writeEvals([])
    expect(capturedFetches).toHaveLength(0)
  })
})

// ===========================================================================
// writeDeals
// ===========================================================================

describe('writeDeals', () => {
  it('POSTs to deals with ON CONFLICT (id) DO UPDATE', async () => {
    await writer.writeDeals([
      {
        threadId: 'th-1',
        subjectUserId: 'user-1',
        dealName: 'Brand Deal',
        dealType: 'sponsorship',
        value: 5000,
        currency: 'USD',
        isAiSorted: true,
        mainContact: {
          email: 'alice@co.com',
          name: 'Alice',
          company: 'Co Inc',
          title: 'CEO',
          phone_number: null,
        },
      },
    ])

    const { url, opts } = lastFetch()
    expect(url).toContain('/rest/v1/deals')
    expect(url).toContain('on_conflict=id')
    expect(opts.headers['Prefer']).toContain('merge-duplicates')
    const body = parsedBody(lastFetch())
    expect(body).toHaveLength(1)
    expect(body[0].id).toBe('th-1')
    expect(body[0].thread_id).toBe('th-1')
    expect(body[0].subject_user_id).toBe('user-1')
    expect(body[0].deal_name).toBe('Brand Deal')
    expect(body[0].value).toBe(5000)
    expect(body[0].main_contact_email).toBe('alice@co.com')
    expect(body[0].main_contact_name).toBe('Alice')
    expect(body[0].main_contact_company).toBe('Co Inc')
    expect(body[0].brand).toBe('Co Inc')
  })

  it('does not include user-owned columns (category, is_deleted, updated_at)', async () => {
    await writer.writeDeals([
      {
        threadId: 'th-1',
        subjectUserId: 'u1',
        dealName: 'test',
        dealType: null,
        value: 0,
        currency: 'USD',
        isAiSorted: true,
        mainContact: null,
      },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0]).not.toHaveProperty('category')
    expect(body[0]).not.toHaveProperty('is_deleted')
    expect(body[0]).not.toHaveProperty('updated_at')
  })

  it('uses threadId as stable id — same thread re-ingested yields same id', async () => {
    const deal = {
      threadId: 'th-stable',
      subjectUserId: 'u1',
      dealName: 'Deal A',
      dealType: null,
      value: 100,
      currency: 'USD',
      isAiSorted: true,
      mainContact: null,
    }
    await writer.writeDeals([deal])
    await writer.writeDeals([deal])
    expect(parsedBody(capturedFetches[0])[0].id).toBe('th-stable')
    expect(parsedBody(capturedFetches[1])[0].id).toBe('th-stable')
  })

  it('handles null mainContact gracefully', async () => {
    await writer.writeDeals([
      {
        threadId: 'th-1',
        subjectUserId: 'u1',
        dealName: 'No contact',
        dealType: null,
        value: 0,
        currency: 'USD',
        isAiSorted: true,
        mainContact: null,
      },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0].main_contact_email).toBeNull()
    expect(body[0].brand).toBeNull()
  })
})

// ===========================================================================
// deleteDeals
// ===========================================================================

describe('deleteDeals', () => {
  it('sends DELETE with id=in.(...) filter', async () => {
    await writer.deleteDeals(['th-1', 'th-2'])
    const { url, opts } = lastFetch()
    expect(url).toContain('/rest/v1/deals')
    expect(url).toContain('id=in.')
    expect(url).toContain('th-1')
    expect(url).toContain('th-2')
    expect(opts.method).toBe('DELETE')
  })

  it('is a no-op for empty array', async () => {
    await writer.deleteDeals([])
    expect(capturedFetches).toHaveLength(0)
  })
})

// ===========================================================================
// writeContacts
// ===========================================================================

describe('writeContacts', () => {
  it('POSTs to contacts with ON CONFLICT (user_id, email) DO UPDATE', async () => {
    await writer.writeContacts([
      {
        userId: 'user-1',
        email: 'alice@co.com',
        name: 'Alice',
        company: 'Co Inc',
        title: 'CEO',
        phone: null,
      },
    ])

    const { url, opts } = lastFetch()
    expect(url).toContain('/rest/v1/contacts')
    expect(url).toContain('on_conflict=user_id%2Cemail')
    expect(opts.headers['Prefer']).toContain('merge-duplicates')
    const body = parsedBody(lastFetch())
    expect(body).toHaveLength(1)
    expect(body[0].user_id).toBe('user-1')
    expect(body[0].subject_user_id).toBe('user-1')
    expect(body[0].email).toBe('alice@co.com')
    expect(body[0].name).toBe('Alice')
    expect(body[0].company_name).toBe('Co Inc')
  })

  it('does not include user-owned columns (updated_at)', async () => {
    await writer.writeContacts([
      { userId: 'u1', email: 'a@b.com', name: null, company: null, title: null, phone: null },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0]).not.toHaveProperty('updated_at')
    expect(body[0]).not.toHaveProperty('is_deleted')
  })

  it('is a no-op for empty array', async () => {
    await writer.writeContacts([])
    expect(capturedFetches).toHaveLength(0)
  })
})

// ===========================================================================
// Config validation
// ===========================================================================

describe('config validation', () => {
  it('throws when supabase-url is missing', async () => {
    writer._resetConfig()
    core.getInput.mockImplementation((name) => {
      if (name === 'supabase-url') return ''
      if (name === 'supabase-service-role-key') return 'key'
      return ''
    })
    await expect(
      writer.writeAudit({
        batchId: 'b1',
        subjectUserId: 'u1',
        threadCount: 0,
        emailCount: 0,
        cost: 0,
        inputTokens: 0,
        outputTokens: 0,
        model: 'm',
        evaluation: '{}',
      }),
    ).rejects.toThrow('supabase-url and supabase-service-role-key are required')
  })

  it('throws when service-role-key is missing', async () => {
    writer._resetConfig()
    core.getInput.mockImplementation((name) => {
      if (name === 'supabase-url') return 'https://x.supabase.co'
      if (name === 'supabase-service-role-key') return ''
      return ''
    })
    await expect(writer.writeContacts([{ userId: 'u', email: 'a@b.com' }])).rejects.toThrow(
      'supabase-url and supabase-service-role-key are required',
    )
  })
})
