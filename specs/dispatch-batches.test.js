import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest'

import { encryptValue } from '../shared/crypto.js'

// --- Mock @actions/core ---
const mockOutputs = {}
const core = {
  getInput: vi.fn(),
  setOutput: vi.fn((name, value) => {
    mockOutputs[name] = value
  }),
  setFailed: vi.fn(),
  info: vi.fn(),
  error: vi.fn(),
}
vi.mock('@actions/core', () => core)

const { run } = await import('../dispatch-batches/src/main.js')

// --- Helpers ---
const ENC_KEY = 'test-encryption-key-for-dispatch'

function mockInputs(overrides = {}) {
  const defaults = {
    'sxt-api-url': 'https://sxt.example.com',
    'sxt-schema': 'dealsync_stg_v1',
    'sxt-access-token': encryptValue('test-jwt-token', ENC_KEY),
    'sxt-biscuit-select': encryptValue('select-biscuit', ENC_KEY),
    'sxt-biscuit-dml': encryptValue('dml-biscuit', ENC_KEY),
    'w3-rpc-url': 'https://w3.example.com/rpc',
    'active-filter': '400',
    'active-detect': '200',
    'pending-filter': '50',
    'pending-detect': '30',
    'max-filter': '600',
    'max-detect': '400',
    'filter-batch-size': '25',
    'detect-batch-size': '10',
    'encryption-key': ENC_KEY,
    ...overrides,
  }
  core.getInput.mockImplementation((name) => defaults[name] ?? '')
}

function sxtResponse(data) {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function sxtError(status, body) {
  return new Response(body, { status })
}

function w3Success(id, triggerHash) {
  return new Response(
    JSON.stringify({
      jsonrpc: '2.0',
      id,
      result: { triggerHash },
    }),
    { status: 200, headers: { 'content-type': 'application/json' } },
  )
}

function w3Error(id, code, message) {
  return new Response(
    JSON.stringify({
      jsonrpc: '2.0',
      id,
      error: { code, message },
    }),
    { status: 200, headers: { 'content-type': 'application/json' } },
  )
}

describe('dispatch-batches', () => {
  let fetchSpy

  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers({ shouldAdvanceTime: true })
    for (const key of Object.keys(mockOutputs)) delete mockOutputs[key]
    fetchSpy = vi.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
    vi.useRealTimers()
  })

  // 1. Slot calculation (600 max - 400 active = 200 available)
  it('calculates available slots correctly', async () => {
    mockInputs({
      'max-filter': '600',
      'active-filter': '400',
      'max-detect': '400',
      'active-detect': '200',
      'pending-filter': '10',
      'pending-detect': '10',
    })

    // Call order per phase:
    // CLAIM PHASE:
    //   Filter batch 0: claim DML, verify SELECT
    //   Filter batch 1: claim DML, verify SELECT (CNT=0 -> stop)
    //   Detect batch 0: claim DML, verify SELECT
    //   Detect batch 1: claim DML, verify SELECT (CNT=0 -> stop)
    // DISPATCH PHASE (per batch: lookup, DELETE, INSERT, trigger, updateHash):
    //   Filter batch 0: lookup, DELETE, INSERT, trigger, updateHash
    //   Detect batch 0: lookup, DELETE, INSERT, trigger, updateHash
    fetchSpy
      // Filter batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      // Filter batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Detect batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      // Detect batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Filter batch 0 dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'hash-f-0'))
      .mockResolvedValueOnce(sxtResponse([]))
      // Detect batch 0 dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(2, 'hash-d-0'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.dispatched_filter_count).toBe('1')
    expect(mockOutputs.dispatched_detect_count).toBe('1')
  })

  // 2. Early exit (pending_filter=0, pending_detect=0) -> dispatched counts = 0
  it('early exits when no pending emails', async () => {
    mockInputs({
      'pending-filter': '0',
      'pending-detect': '0',
    })

    await run()

    expect(fetchSpy).not.toHaveBeenCalled()
    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.dispatched_filter_count).toBe('0')
    expect(mockOutputs.dispatched_detect_count).toBe('0')
    expect(core.info).toHaveBeenCalledWith('No pending emails to dispatch')
  })

  // 3. Atomic claim SQL generation (correct schema, stage, limit)
  it('generates correct filter claim SQL with schema, stage, and batch size', async () => {
    mockInputs({
      'sxt-schema': 'dealsync_stg_v1',
      'filter-batch-size': '25',
      'pending-filter': '10',
      'pending-detect': '0',
    })

    fetchSpy
      // Filter batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      // Filter batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'hash-1'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    // First fetch call is the claim DML
    const claimCall = fetchSpy.mock.calls[0]
    const claimBody = JSON.parse(claimCall[1].body)
    expect(claimBody.sqlText).toBe(
      'UPDATE dealsync_stg_v1.EMAIL_METADATA SET STAGE = 1000 WHERE ID IN (SELECT ID FROM dealsync_stg_v1.EMAIL_METADATA WHERE STAGE = 2 LIMIT 25)',
    )
    expect(claimBody.biscuits).toEqual(['dml-biscuit'])

    // Second fetch call is the verify SELECT
    const verifyCall = fetchSpy.mock.calls[1]
    const verifyBody = JSON.parse(verifyCall[1].body)
    expect(verifyBody.sqlText).toBe(
      'SELECT COUNT(*) AS CNT FROM dealsync_stg_v1.EMAIL_METADATA WHERE STAGE = 1000',
    )
    expect(verifyBody.biscuits).toEqual(['select-biscuit'])
  })

  // 4. Detection claim SQL includes thread-completeness NOT EXISTS check
  it('generates detection claim SQL with thread-completeness check', async () => {
    mockInputs({
      'sxt-schema': 'dealsync_stg_v1',
      'detect-batch-size': '10',
      'pending-filter': '0',
      'pending-detect': '20',
      'active-filter': '600', // max out filter so we skip filter batches
    })

    fetchSpy
      // Detect batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }]))
      // Detect batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'hash-d-1'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    // First fetch call is the detection claim DML
    const claimCall = fetchSpy.mock.calls[0]
    const claimBody = JSON.parse(claimCall[1].body)
    expect(claimBody.sqlText).toBe(
      'UPDATE dealsync_stg_v1.EMAIL_METADATA SET STAGE = 11000 WHERE ID IN (SELECT em.ID FROM dealsync_stg_v1.EMAIL_METADATA em WHERE em.STAGE = 3 AND NOT EXISTS (SELECT 1 FROM dealsync_stg_v1.EMAIL_METADATA m2 WHERE m2.THREAD_ID = em.THREAD_ID AND m2.USER_ID = em.USER_ID AND m2.STAGE IN (1, 2)) LIMIT 10)',
    )
  })

  // 5. W3 RPC trigger payload construction (correct params, incrementing id)
  it('sends correct W3 RPC trigger payload with incrementing ids', async () => {
    mockInputs({
      'pending-filter': '50',
      'pending-detect': '20',
    })

    fetchSpy
      // Filter batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 25 }]))
      // Filter batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Detect batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      // Detect batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Filter dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'filter-hash'))
      .mockResolvedValueOnce(sxtResponse([]))
      // Detect dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(2, 'detect-hash'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    // Find W3 RPC calls (they go to w3-rpc-url, not sxt-api-url)
    const w3Calls = fetchSpy.mock.calls.filter(
      (c) => c[0] === 'https://w3.example.com/rpc',
    )
    expect(w3Calls).toHaveLength(2)

    // First trigger (filter, id=1)
    const filterPayload = JSON.parse(w3Calls[0][1].body)
    expect(filterPayload).toEqual({
      jsonrpc: '2.0',
      method: 'w3_triggerWorkflow',
      params: {
        workflowName: 'dealsync-processor',
        body: {
          batch_type: 'filter',
          transition_stage: '1000',
          reset_stage: '2',
          previous_trigger_hash: '',
        },
      },
      id: 1,
    })

    // Second trigger (detect, id=2)
    const detectPayload = JSON.parse(w3Calls[1][1].body)
    expect(detectPayload).toEqual({
      jsonrpc: '2.0',
      method: 'w3_triggerWorkflow',
      params: {
        workflowName: 'dealsync-processor',
        body: {
          batch_type: 'detect',
          transition_stage: '11000',
          reset_stage: '3',
          previous_trigger_hash: '',
        },
      },
      id: 2,
    })
  })

  // 6. 100ms gap between triggers (verify setTimeout called)
  it('enforces 100ms gap between triggers', async () => {
    const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout')

    mockInputs({
      'pending-filter': '50',
      'pending-detect': '0',
    })

    fetchSpy
      // Filter batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 25 }]))
      // Filter batch 1: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 20 }]))
      // Filter batch 2: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Batch 0 dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'h1'))
      .mockResolvedValueOnce(sxtResponse([]))
      // Batch 1 dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(2, 'h2'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    // Check that setTimeout was called with 100ms for rate limiting
    const timeoutCalls = setTimeoutSpy.mock.calls.filter(
      (c) => c[1] === 100,
    )
    expect(timeoutCalls.length).toBe(2)

    setTimeoutSpy.mockRestore()
  })

  // 7. Trigger failure -> emails reset to original stage
  it('resets claimed emails to original stage on trigger failure', async () => {
    mockInputs({
      'pending-filter': '10',
      'pending-detect': '0',
    })

    fetchSpy
      // Filter batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      // Filter batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Dispatch: lookup, DELETE, INSERT
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      // Trigger FAILS
      .mockResolvedValueOnce(w3Error(1, -32000, 'workflow not found'))
      // Reset claimed emails back to stage 2
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    // Find the reset call - last SxT call
    const sxtCalls = fetchSpy.mock.calls.filter((c) =>
      c[0].includes('sxt.example.com'),
    )
    const lastSxtCall = sxtCalls[sxtCalls.length - 1]
    const resetBody = JSON.parse(lastSxtCall[1].body)
    expect(resetBody.sqlText).toBe(
      'UPDATE dealsync_stg_v1.EMAIL_METADATA SET STAGE = 2 WHERE STAGE = 1000',
    )

    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.dispatched_filter_count).toBe('0')
    expect(core.error).toHaveBeenCalledWith(
      expect.stringContaining('trigger failed'),
    )
  })

  // 7b. Detection trigger failure resets to stage 3
  it('resets detection emails to stage 3 on trigger failure', async () => {
    mockInputs({
      'pending-filter': '0',
      'pending-detect': '10',
      'active-filter': '600', // max out filter
    })

    fetchSpy
      // Detect batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }]))
      // Detect batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Dispatch: lookup, DELETE, INSERT
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      // Trigger FAILS
      .mockResolvedValueOnce(w3Error(1, -32000, 'timeout'))
      // Reset to stage 3
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    const sxtCalls = fetchSpy.mock.calls.filter((c) =>
      c[0].includes('sxt.example.com'),
    )
    const lastSxtCall = sxtCalls[sxtCalls.length - 1]
    const resetBody = JSON.parse(lastSxtCall[1].body)
    expect(resetBody.sqlText).toBe(
      'UPDATE dealsync_stg_v1.EMAIL_METADATA SET STAGE = 3 WHERE STAGE = 11000',
    )

    expect(mockOutputs.dispatched_detect_count).toBe('0')
  })

  // 8. Dispatch log write (DELETE + INSERT)
  it('writes dispatch log with DELETE then INSERT for each batch', async () => {
    mockInputs({
      'pending-filter': '10',
      'pending-detect': '0',
    })

    // Call order with pending-detect=0:
    // 0: filter claim DML
    // 1: filter verify SELECT
    // 2: filter batch 1 claim DML
    // 3: filter batch 1 verify SELECT (CNT=0)
    // -- no detection claims (pending=0) --
    // 4: lookup hash (SELECT)
    // 5: dispatch log DELETE
    // 6: dispatch log INSERT
    // 7: trigger (W3 RPC)
    // 8: update hash
    fetchSpy
      // Filter batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      // Filter batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'abc123'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    const sxtCalls = fetchSpy.mock.calls.filter((c) =>
      c[0].includes('sxt.example.com'),
    )

    // Index 5 and 6 (0-indexed) are the dispatch log DELETE and INSERT
    const deleteBody = JSON.parse(sxtCalls[5][1].body)
    expect(deleteBody.sqlText).toBe(
      'DELETE FROM dealsync_stg_v1.DISPATCH_LOG WHERE TRANSITION_STAGE = 1000',
    )

    const insertBody = JSON.parse(sxtCalls[6][1].body)
    expect(insertBody.sqlText).toBe(
      "INSERT INTO dealsync_stg_v1.DISPATCH_LOG (TRANSITION_STAGE, TRIGGER_HASH, BATCH_TYPE, CREATED_AT) VALUES (1000, '', 'filter', CURRENT_TIMESTAMP)",
    )
  })

  // 9. Input validation (negative numbers rejected)
  it('rejects negative numbers in numeric inputs', async () => {
    mockInputs({
      'active-filter': '-5',
    })

    await run()

    expect(mockOutputs.success).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining('active-filter must be non-negative integer'),
    )
  })

  it('rejects non-numeric input values', async () => {
    mockInputs({
      'max-filter': 'abc',
    })

    await run()

    expect(mockOutputs.success).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining('max-filter must be non-negative integer'),
    )
  })

  // 10. Empty claim (SELECT count returns 0) -> stops dispatching
  it('stops dispatching when claim returns 0 emails', async () => {
    mockInputs({
      'pending-filter': '50',
      'pending-detect': '0',
    })

    fetchSpy
      // Filter batch 0: claim DML
      .mockResolvedValueOnce(sxtResponse([]))
      // Filter batch 0: verify count = 0 (no emails to claim)
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))

    await run()

    // No triggers should be made
    const w3Calls = fetchSpy.mock.calls.filter(
      (c) => c[0] === 'https://w3.example.com/rpc',
    )
    expect(w3Calls).toHaveLength(0)

    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.dispatched_filter_count).toBe('0')
    expect(mockOutputs.dispatched_detect_count).toBe('0')
  })

  // Additional: dispatch log hash update on success
  it('updates dispatch log with trigger hash on success', async () => {
    mockInputs({
      'pending-filter': '10',
      'pending-detect': '0',
    })

    fetchSpy
      // Filter batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      // Filter batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'abc-trigger-hash'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    const sxtCalls = fetchSpy.mock.calls.filter((c) =>
      c[0].includes('sxt.example.com'),
    )
    // Last SxT call should be the hash update
    const updateBody = JSON.parse(sxtCalls[sxtCalls.length - 1][1].body)
    expect(updateBody.sqlText).toBe(
      "UPDATE dealsync_stg_v1.DISPATCH_LOG SET TRIGGER_HASH = 'abc-trigger-hash' WHERE TRANSITION_STAGE = 1000",
    )
  })

  // Additional: transition stages increment by 10 per batch
  it('increments transition stages by 10 per batch', async () => {
    mockInputs({
      'pending-filter': '100',
      'pending-detect': '0',
      'filter-batch-size': '25',
    })

    fetchSpy
      // Filter batch 0 (stage 1000): claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 25 }]))
      // Filter batch 1 (stage 1010): claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 25 }]))
      // Filter batch 2 (stage 1020): claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Batch 0 dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'h1'))
      .mockResolvedValueOnce(sxtResponse([]))
      // Batch 1 dispatch: lookup, DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(sxtResponse([{ TRIGGER_HASH: '' }]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(2, 'h2'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    // Verify batch 0 used stage 1000
    const claim0 = JSON.parse(fetchSpy.mock.calls[0][1].body)
    expect(claim0.sqlText).toContain('SET STAGE = 1000')

    // Verify batch 1 used stage 1010
    const claim1 = JSON.parse(fetchSpy.mock.calls[2][1].body)
    expect(claim1.sqlText).toContain('SET STAGE = 1010')

    expect(mockOutputs.dispatched_filter_count).toBe('2')
  })

  // Additional: SxT API error throws and sets failure
  it('handles SxT API errors gracefully', async () => {
    mockInputs({
      'pending-filter': '10',
      'pending-detect': '0',
    })

    fetchSpy.mockResolvedValueOnce(sxtError(500, 'Internal Server Error'))

    await run()

    expect(mockOutputs.success).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining('SxT 500'),
    )
  })

  // Additional: previous trigger hash is passed for re-dispatched batches
  it('passes previous trigger hash from dispatch log lookup', async () => {
    mockInputs({
      'pending-filter': '10',
      'pending-detect': '0',
    })

    fetchSpy
      // Filter batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      // Filter batch 1: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Dispatch: lookup (returns existing hash), DELETE, INSERT, trigger, updateHash
      .mockResolvedValueOnce(
        sxtResponse([{ TRIGGER_HASH: 'previous-hash-123' }]),
      )
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(w3Success(1, 'new-hash'))
      .mockResolvedValueOnce(sxtResponse([]))

    await run()

    const w3Calls = fetchSpy.mock.calls.filter(
      (c) => c[0] === 'https://w3.example.com/rpc',
    )
    const triggerPayload = JSON.parse(w3Calls[0][1].body)
    expect(triggerPayload.params.body.previous_trigger_hash).toBe(
      'previous-hash-123',
    )
  })

  // Additional: schema sanitization
  it('rejects invalid schema names', async () => {
    mockInputs({
      'sxt-schema': 'schema; DROP TABLE',
    })

    await run()

    expect(mockOutputs.success).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining('Invalid schema'),
    )
  })
})
