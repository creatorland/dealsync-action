import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { encryptValue, decryptValue } from '../shared/crypto.js'

// --- Mock @actions/core ---
const outputs = {}
const core = {
  getInput: vi.fn(),
  setOutput: vi.fn((name, value) => {
    outputs[name] = value
  }),
  setFailed: vi.fn(),
  info: vi.fn(),
  error: vi.fn(),
}
vi.mock('@actions/core', () => core)

// We import run() AFTER the mock is set up
const { run } = await import('../http/src/main.js')

// --- Helpers ---
const ENC_KEY = 'a'.repeat(64)

function mockInputs(map) {
  core.getInput.mockImplementation((name, _opts) => map[name] ?? '')
}

function jsonResponse(body, status = 200, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  })
}

function textResponse(body, status = 200, headers = {}) {
  return new Response(body, {
    status,
    headers: { 'content-type': 'text/plain', ...headers },
  })
}

describe('http action', () => {
  let fetchSpy

  beforeEach(() => {
    vi.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
    fetchSpy = vi.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  // --- Retry on 429 with Retry-After header ---
  it('retries on 429 with Retry-After header', async () => {
    fetchSpy
      .mockResolvedValueOnce(
        new Response('rate limited', {
          status: 429,
          headers: { 'retry-after': '0' },
        })
      )
      .mockResolvedValueOnce(jsonResponse({ ok: true }))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'max-retries': '3',
      'retry-delay-ms': '10',
      'retry-on-status': '429,500,502,503,504',
      'retry-backoff': 'exponential',
    })

    await run()

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(outputs['success']).toBe('true')
    expect(outputs['status-code']).toBe('200')
    expect(core.setFailed).not.toHaveBeenCalled()
  })

  // --- Retry on 500 with exponential backoff ---
  it('retries on 500 with exponential backoff', async () => {
    fetchSpy
      .mockResolvedValueOnce(textResponse('server error', 500))
      .mockResolvedValueOnce(textResponse('server error', 500))
      .mockResolvedValueOnce(jsonResponse({ recovered: true }))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'max-retries': '3',
      'retry-delay-ms': '10',
      'retry-on-status': '429,500,502,503,504',
      'retry-backoff': 'exponential',
    })

    await run()

    expect(fetchSpy).toHaveBeenCalledTimes(3)
    expect(outputs['success']).toBe('true')
    expect(outputs['status-code']).toBe('200')
    expect(core.setFailed).not.toHaveBeenCalled()
  })

  // --- Max retries exceeded -> failure ---
  it('fails after max retries exceeded, success output is false', async () => {
    fetchSpy
      .mockResolvedValueOnce(textResponse('error', 500))
      .mockResolvedValueOnce(textResponse('error', 500))
      .mockResolvedValueOnce(textResponse('error', 500))
      .mockResolvedValueOnce(textResponse('still error', 500))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'max-retries': '3',
      'retry-delay-ms': '10',
      'retry-on-status': '429,500,502,503,504',
      'retry-backoff': 'exponential',
    })

    await run()

    // 1 initial + 3 retries = 4 total
    expect(fetchSpy).toHaveBeenCalledTimes(4)
    expect(outputs['success']).toBe('false')
    expect(outputs['status-code']).toBe('500')
    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining('HTTP 500')
    )
  })

  // --- SxT array unwrapping (single element) ---
  it('unwraps single-element SxT array for extract-outputs', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse([{ FOO: 'bar', BAZ: 42 }]))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'extract-outputs': 'FOO,BAZ',
      'max-retries': '0',
    })

    await run()

    expect(outputs['FOO']).toBe('bar')
    expect(outputs['BAZ']).toBe('42')
    expect(outputs['success']).toBe('true')
  })

  // --- SxT array NOT unwrapped when multiple elements ---
  it('does not unwrap multi-element array for extract-outputs', async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse([
        { FOO: 'first', BAZ: 1 },
        { FOO: 'second', BAZ: 2 },
      ])
    )

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'extract-outputs': 'FOO',
      'max-retries': '0',
    })

    await run()

    // Array stays as-is; top-level FOO doesn't exist on an array
    expect(outputs['FOO']).toBeUndefined()
    expect(outputs['success']).toBe('true')
  })

  // --- decrypt-inputs with comma-separated paths ---
  it('decrypts comma-separated decrypt-inputs paths', async () => {
    const encryptedAuth = encryptValue('my-secret-token', ENC_KEY)
    const encryptedBody = encryptValue('secret-body-value', ENC_KEY)

    fetchSpy.mockResolvedValueOnce(jsonResponse({ ok: true }))

    mockInputs({
      url: 'https://example.com/api',
      method: 'POST',
      headers: JSON.stringify({ Authorization: `Bearer ${encryptedAuth}` }),
      body: JSON.stringify({ biscuits: { 0: encryptedBody } }),
      'decrypt-inputs': 'headers.Authorization,body.biscuits.0',
      'encryption-key': ENC_KEY,
      'max-retries': '0',
    })

    await run()

    // Verify the fetch was called with decrypted values
    const callArgs = fetchSpy.mock.calls[0]
    const sentHeaders = callArgs[1].headers
    expect(sentHeaders.Authorization).toBe('Bearer my-secret-token')

    const sentBody = JSON.parse(callArgs[1].body)
    expect(sentBody.biscuits['0']).toBe('secret-body-value')

    expect(outputs['success']).toBe('true')
  })

  // --- encrypt-outputs ---
  it('encrypts specified outputs', async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse({ token: 'sensitive-value', name: 'public' })
    )

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'extract-outputs': 'token,name',
      'encrypt-outputs': 'token',
      'encryption-key': ENC_KEY,
      'max-retries': '0',
    })

    await run()

    // token should be encrypted, name should be plain
    expect(outputs['token']).not.toBe('sensitive-value')
    expect(decryptValue(outputs['token'], ENC_KEY)).toBe('sensitive-value')
    expect(outputs['name']).toBe('public')
  })

  // --- extract-outputs with UPPERCASE keys (SxT convention) ---
  it('extracts UPPERCASE keys from response (SxT convention)', async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse([
        {
          USER_ID: 'u-123',
          EMAIL_ADDRESS: 'test@example.com',
          DEAL_STAGE: 3,
        },
      ])
    )

    mockInputs({
      url: 'https://example.com/sxt',
      method: 'POST',
      'extract-outputs': 'USER_ID,EMAIL_ADDRESS,DEAL_STAGE',
      'max-retries': '0',
    })

    await run()

    // Single-element array should be unwrapped
    expect(outputs['USER_ID']).toBe('u-123')
    expect(outputs['EMAIL_ADDRESS']).toBe('test@example.com')
    expect(outputs['DEAL_STAGE']).toBe('3')
  })

  // --- success and status-code always set before throw on non-200 ---
  it('sets success and status-code before throwing on non-retryable error', async () => {
    fetchSpy.mockResolvedValueOnce(textResponse('forbidden', 403))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'max-retries': '0',
      'retry-on-status': '429,500,502,503,504',
    })

    await run()

    expect(outputs['success']).toBe('false')
    expect(outputs['status-code']).toBe('403')
    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining('HTTP 403')
    )
  })

  // --- Basic successful GET ---
  it('makes a successful GET request and returns response', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ data: 'hello' }))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'max-retries': '0',
    })

    await run()

    expect(outputs['success']).toBe('true')
    expect(outputs['status-code']).toBe('200')
    expect(outputs['response']).toBe(JSON.stringify({ data: 'hello' }))
    expect(core.setFailed).not.toHaveBeenCalled()
  })

  // --- POST with body ---
  it('sends body for POST requests', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ created: true }))

    mockInputs({
      url: 'https://example.com/api',
      method: 'POST',
      body: JSON.stringify({ name: 'test' }),
      headers: JSON.stringify({ 'Content-Type': 'application/json' }),
      'max-retries': '0',
    })

    await run()

    const callArgs = fetchSpy.mock.calls[0]
    expect(callArgs[1].method).toBe('POST')
    expect(callArgs[1].body).toBe(JSON.stringify({ name: 'test' }))
    expect(outputs['success']).toBe('true')
  })

  // --- Network error retries ---
  it('retries on network errors', async () => {
    fetchSpy
      .mockRejectedValueOnce(new Error('fetch failed'))
      .mockResolvedValueOnce(jsonResponse({ ok: true }))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'max-retries': '2',
      'retry-delay-ms': '10',
      'retry-backoff': 'linear',
    })

    await run()

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(outputs['success']).toBe('true')
    expect(core.setFailed).not.toHaveBeenCalled()
  })

  // --- Linear backoff ---
  it('uses linear backoff when configured', async () => {
    fetchSpy
      .mockResolvedValueOnce(textResponse('error', 502))
      .mockResolvedValueOnce(jsonResponse({ ok: true }))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'max-retries': '2',
      'retry-delay-ms': '10',
      'retry-on-status': '502',
      'retry-backoff': 'linear',
    })

    await run()

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(outputs['success']).toBe('true')
  })
})
