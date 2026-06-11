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

// The four business tables have NO subject_user_id column (RLS keys on user_id
// for deals/contacts and transitive joins for evals/audits). This guard asserts
// the writer never sends a column the canonical schema does not have.
function assertNoSubjectUserId(row) {
  expect(row).not.toHaveProperty('subject_user_id')
}

// created_at / updated_at are DB-managed (defaults + touch_updated_at triggers).
function assertNoManagedTimestamps(row) {
  expect(row).not.toHaveProperty('created_at')
  expect(row).not.toHaveProperty('updated_at')
}

// ===========================================================================
// writeAudit
// ===========================================================================

describe('writeAudit', () => {
  it('POSTs to ai_evaluation_audits with ON CONFLICT (id) DO NOTHING, id=batchId', async () => {
    await writer.writeAudit({
      batchId: 'batch-1',
      threadCount: 3,
      emailCount: 10,
      cost: 0,
      inputTokens: 0,
      outputTokens: 0,
      model: 'gpt-4',
      aiEvaluation: { threads: [] },
    })

    const { url, opts } = lastFetch()
    expect(url).toContain('/rest/v1/ai_evaluation_audits')
    expect(url).toContain('on_conflict=id')
    expect(opts.headers['Prefer']).toContain('ignore-duplicates')
    const body = parsedBody(lastFetch())
    expect(body).toHaveLength(1)
    expect(body[0].id).toBe('batch-1')
    expect(body[0].batch_id).toBe('batch-1')
    expect(body[0].thread_count).toBe(3)
    expect(body[0].model_used).toBe('gpt-4')
    assertNoSubjectUserId(body[0])
  })

  it('writes ai_evaluation as a jsonb object, not a string', async () => {
    await writer.writeAudit({
      batchId: 'b1',
      threadCount: 1,
      emailCount: 1,
      cost: 0,
      inputTokens: 0,
      outputTokens: 0,
      model: 'm',
      aiEvaluation: { threads: [{ thread_id: 't1', is_deal: true }] },
    })
    const body = parsedBody(lastFetch())
    expect(typeof body[0].ai_evaluation).toBe('object')
    expect(body[0].ai_evaluation.threads[0].thread_id).toBe('t1')
  })

  it('does not include user-owned / managed columns', async () => {
    await writer.writeAudit({
      batchId: 'b1',
      threadCount: 1,
      emailCount: 1,
      cost: 0,
      inputTokens: 0,
      outputTokens: 0,
      model: 'm',
      aiEvaluation: {},
    })
    const body = parsedBody(lastFetch())
    assertNoSubjectUserId(body[0])
    expect(body[0]).not.toHaveProperty('updated_at')
  })

  it('throws on non-ok response', async () => {
    global.fetch = jest.fn(async () => ({
      ok: false,
      status: 422,
      text: async () => 'bad request',
    }))
    await expect(
      writer.writeAudit({
        batchId: 'b1',
        threadCount: 0,
        emailCount: 0,
        cost: 0,
        inputTokens: 0,
        outputTokens: 0,
        model: 'm',
        aiEvaluation: {},
      }),
    ).rejects.toThrow('Supabase ai_evaluation_audits upsert failed: 422')
  })

  it('sanitizes PostgREST error bodies so row PII never reaches the thrown message', async () => {
    global.fetch = jest.fn(async () => ({
      ok: false,
      status: 409,
      text: async () =>
        JSON.stringify({
          code: '23505',
          message: 'duplicate key value violates unique constraint "contacts_pkey"',
          // PostgREST embeds the failing row here — must NOT be surfaced.
          details: 'Key (user_id, email)=(u1, alice@secret.com) already exists.',
        }),
    }))
    let msg = ''
    try {
      await writer.writeContacts([
        {
          userId: 'u1',
          email: 'alice@secret.com',
          name: null,
          company: null,
          title: null,
          phone: null,
        },
      ])
    } catch (e) {
      msg = e.message
    }
    expect(msg).toContain('409')
    expect(msg).toContain('23505')
    expect(msg).toContain('duplicate key value')
    expect(msg).not.toContain('alice@secret.com')
  })
})

// ===========================================================================
// writeEvals
// ===========================================================================

describe('writeEvals', () => {
  it('POSTs to email_thread_evaluations with ON CONFLICT (id) DO UPDATE, id=threadId', async () => {
    await writer.writeEvals([
      {
        threadId: 'th-1',
        auditId: 'batch-1',
        aiInsight: 'brand_deal',
        aiSummary: 'summary',
        isDeal: true,
        likelyScam: false,
        aiScore: 8,
        mainContact: {
          email: 'Alice@CO.com',
          name: 'Alice',
          company: 'Co Inc',
          title: 'CEO',
          phone_number: '+123',
        },
      },
    ])

    const { url, opts } = lastFetch()
    expect(url).toContain('/rest/v1/email_thread_evaluations')
    expect(url).toContain('on_conflict=id')
    expect(opts.headers['Prefer']).toContain('merge-duplicates')
    // omitted columns (created_at/updated_at) must use DB defaults, not NULL
    expect(opts.headers['Prefer']).toContain('missing=default')
    const body = parsedBody(lastFetch())
    expect(body).toHaveLength(1)
    expect(body[0].id).toBe('th-1')
    expect(body[0].thread_id).toBe('th-1')
    expect(body[0].ai_evaluation_audit_id).toBe('batch-1')
    expect(body[0].is_deal).toBe(true)
    expect(body[0].ai_score).toBe(8)
    assertNoSubjectUserId(body[0])
  })

  it('carries the denormalized main_contact_* columns (they live on ETE, not deals)', async () => {
    await writer.writeEvals([
      {
        threadId: 'th-1',
        auditId: 'b1',
        aiInsight: null,
        aiSummary: null,
        isDeal: true,
        likelyScam: false,
        aiScore: 5,
        mainContact: {
          email: 'Bob@Example.COM',
          name: 'Bob',
          company: 'Acme',
          title: 'Mgr',
          phone_number: '555',
        },
      },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0].main_contact_name).toBe('Bob')
    // lowercased to match contacts.email (contact_card_v join key)
    expect(body[0].main_contact_email).toBe('bob@example.com')
    expect(body[0].main_contact_company).toBe('Acme')
    expect(body[0].main_contact_title).toBe('Mgr')
    expect(body[0].main_contact_phone_number).toBe('555')
  })

  it('null main_contact yields null main_contact_* columns (non-deal threads)', async () => {
    await writer.writeEvals([
      {
        threadId: 'th-nd',
        auditId: 'b1',
        aiInsight: null,
        aiSummary: null,
        isDeal: false,
        likelyScam: false,
        aiScore: 2,
        mainContact: null,
      },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0].is_deal).toBe(false)
    expect(body[0].main_contact_email).toBeNull()
    expect(body[0].main_contact_name).toBeNull()
  })

  it('uses thread_id as the stable id for idempotent re-ingest', async () => {
    const evals = [
      {
        threadId: 'th-stable',
        auditId: null,
        aiInsight: null,
        aiSummary: null,
        isDeal: false,
        likelyScam: false,
        aiScore: 3,
        mainContact: null,
      },
    ]
    await writer.writeEvals(evals)
    await writer.writeEvals(evals)
    expect(parsedBody(capturedFetches[0])[0].id).toBe('th-stable')
    expect(parsedBody(capturedFetches[1])[0].id).toBe('th-stable')
  })

  it('does not include subject_user_id or managed timestamps', async () => {
    await writer.writeEvals([
      {
        threadId: 'th-1',
        auditId: null,
        aiInsight: null,
        aiSummary: null,
        isDeal: false,
        likelyScam: false,
        aiScore: 0,
        mainContact: null,
      },
    ])
    const body = parsedBody(lastFetch())
    assertNoSubjectUserId(body[0])
    assertNoManagedTimestamps(body[0])
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
  it('POSTs to deals with ON CONFLICT (id) DO UPDATE and user_id (not subject_user_id)', async () => {
    await writer.writeDeals([
      {
        threadId: 'th-1',
        userId: 'user-1',
        category: 'brand_deal',
        dealName: 'Brand Deal',
        dealType: 'sponsorship',
        value: 5000,
        currency: 'USD',
        brand: 'Co Inc',
        isAiSorted: true,
      },
    ])

    const { url, opts } = lastFetch()
    expect(url).toContain('/rest/v1/deals')
    expect(url).toContain('on_conflict=id')
    expect(opts.headers['Prefer']).toContain('merge-duplicates')
    const body = parsedBody(lastFetch())
    expect(body).toHaveLength(1)
    expect(body[0].id).toBe('th-1')
    expect(body[0].user_id).toBe('user-1')
    expect(body[0].thread_id).toBe('th-1')
    // join key to email_thread_evaluations.id (deal_card_v + transitive RLS)
    expect(body[0].email_thread_evaluation_id).toBe('th-1')
    expect(body[0].deal_name).toBe('Brand Deal')
    expect(body[0].value).toBe(5000)
    expect(body[0].brand).toBe('Co Inc')
    assertNoSubjectUserId(body[0])
  })

  it('does NOT carry main_contact_* columns (those live on email_thread_evaluations)', async () => {
    await writer.writeDeals([
      {
        threadId: 'th-1',
        userId: 'u1',
        category: null,
        dealName: 'x',
        dealType: null,
        value: 0,
        currency: 'USD',
        brand: 'Acme',
        isAiSorted: true,
      },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0]).not.toHaveProperty('main_contact_email')
    expect(body[0]).not.toHaveProperty('main_contact_name')
    expect(body[0]).not.toHaveProperty('main_contact_company')
  })

  it('writes the AI category (ingestion-owned; users are SELECT-only on deals)', async () => {
    await writer.writeDeals([
      {
        threadId: 'th-1',
        userId: 'u1',
        category: 'job_offer',
        dealName: 'x',
        dealType: null,
        value: 0,
        currency: 'USD',
        brand: null,
        isAiSorted: true,
      },
    ])
    expect(parsedBody(lastFetch())[0].category).toBe('job_offer')
  })

  it('does not include is_deleted (no such column) or managed timestamps', async () => {
    await writer.writeDeals([
      {
        threadId: 'th-1',
        userId: 'u1',
        category: null,
        dealName: 'x',
        dealType: null,
        value: 0,
        currency: 'USD',
        brand: null,
        isAiSorted: true,
      },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0]).not.toHaveProperty('is_deleted')
    assertNoManagedTimestamps(body[0])
  })

  it('uses threadId as stable id — same thread re-ingested yields same id', async () => {
    const deal = {
      threadId: 'th-stable',
      userId: 'u1',
      category: null,
      dealName: 'Deal A',
      dealType: null,
      value: 100,
      currency: 'USD',
      brand: null,
      isAiSorted: true,
    }
    await writer.writeDeals([deal])
    await writer.writeDeals([deal])
    expect(parsedBody(capturedFetches[0])[0].id).toBe('th-stable')
    expect(parsedBody(capturedFetches[1])[0].id).toBe('th-stable')
  })

  it('coerces a non-finite value (NaN/Infinity) to 0, not a JSON null', async () => {
    await writer.writeDeals([
      {
        threadId: 'th-1',
        userId: 'u1',
        category: null,
        dealName: 'x',
        dealType: null,
        value: NaN,
        currency: 'USD',
        brand: null,
        isAiSorted: true,
      },
    ])
    // typeof NaN === 'number' would have slipped through and JSON.stringify(NaN)
    // serializes to null; Number.isFinite guards it to a real 0.
    expect(parsedBody(lastFetch())[0].value).toBe(0)
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
        email: 'Alice@CO.com',
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
    // lowercased so it matches email_thread_evaluations.main_contact_email
    expect(body[0].email).toBe('alice@co.com')
    expect(body[0].name).toBe('Alice')
    expect(body[0].company_name).toBe('Co Inc')
    assertNoSubjectUserId(body[0])
  })

  it('has no surrogate id (composite PK user_id,email) and no managed timestamps', async () => {
    await writer.writeContacts([
      { userId: 'u1', email: 'a@b.com', name: null, company: null, title: null, phone: null },
    ])
    const body = parsedBody(lastFetch())
    expect(body[0]).not.toHaveProperty('id')
    assertNoManagedTimestamps(body[0])
  })

  it('is a no-op for empty array', async () => {
    await writer.writeContacts([])
    expect(capturedFetches).toHaveLength(0)
  })

  it('omits null optional fields so a sparse re-upsert cannot blank existing values', async () => {
    await writer.writeContacts([
      { userId: 'u1', email: 'a@b.com', name: null, company: null, title: null, phone: null },
    ])
    // Only PK columns are sent; PostgREST leaves name/company/title/phone intact
    // on conflict because they are absent from the DO UPDATE SET column list.
    expect(parsedBody(lastFetch())[0]).toEqual({ user_id: 'u1', email: 'a@b.com' })
  })

  it('includes only the populated optional fields', async () => {
    await writer.writeContacts([
      { userId: 'u1', email: 'A@B.com', name: 'Alice', company: null, title: 'CEO', phone: null },
    ])
    expect(parsedBody(lastFetch())[0]).toEqual({
      user_id: 'u1',
      email: 'a@b.com',
      name: 'Alice',
      title: 'CEO',
    })
  })
})

// ===========================================================================
// writeClassifyHeartbeat (Story 8.1, Task 5.3)
// ===========================================================================

describe('writeClassifyHeartbeat', () => {
  const ROW = {
    run_id: 'gha-run-1',
    node: 'testnet',
    status: 'success',
    rows_written_total: 5,
    rows_by_table: { deals: 1, contacts: 1, email_thread_evaluations: 2, ai_evaluation_audits: 1 },
    ingest_write_target: 'supabase',
  }

  it('POSTs one row to classify_heartbeat with service-role headers, no on_conflict', async () => {
    await writer.writeClassifyHeartbeat(ROW)

    const call = lastFetch()
    expect(call.url).toBe(`${SUPABASE_URL}/rest/v1/classify_heartbeat`)
    expect(call.opts.method).toBe('POST')
    expect(call.url).not.toContain('on_conflict')
    expect(call.opts.headers.Authorization).toBe(`Bearer ${SERVICE_ROLE_KEY}`)
    expect(call.opts.headers.apikey).toBe(SERVICE_ROLE_KEY)
    // Append-only insert — missing=default (id/created_at fall to DB defaults),
    // and NO resolution= (it is not a merge).
    expect(call.opts.headers.Prefer).toContain('missing=default')
    expect(call.opts.headers.Prefer).not.toContain('resolution=')
    // PostgREST wants an array body even for a single row.
    expect(parsedBody(call)).toEqual([ROW])
  })

  it('is a no-op when row is null/undefined', async () => {
    await writer.writeClassifyHeartbeat(null)
    expect(capturedFetches).toHaveLength(0)
  })

  it('throws a sanitized error on a non-ok response', async () => {
    global.fetch = jest.fn(async () => ({
      ok: false,
      status: 422,
      text: async () => '{"code":"23514","message":"violates check constraint"}',
    }))
    await expect(writer.writeClassifyHeartbeat(ROW)).rejects.toThrow(
      'Supabase classify_heartbeat insert failed: 422 [23514] violates check constraint',
    )
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
        threadCount: 0,
        emailCount: 0,
        cost: 0,
        inputTokens: 0,
        outputTokens: 0,
        model: 'm',
        aiEvaluation: {},
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
