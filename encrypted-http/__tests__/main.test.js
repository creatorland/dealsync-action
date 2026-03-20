import { jest } from '@jest/globals'
import { encryptValue, decryptValue } from '../../shared/crypto.js'

const outputs = {}
jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn((name, value) => {
    outputs[name] = value
  }),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

const core = await import('@actions/core')
const { run } = await import('../src/main.js')

const ENC_KEY = 'a'.repeat(64)

function mockInputs(map) {
  core.getInput.mockImplementation((name) => map[name] ?? '')
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

describe('encrypted-http main', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  it('makes a successful GET and returns response', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ data: 'hello' }))
    mockInputs({ url: 'https://example.com/api', method: 'GET', 'max-retries': '0' })

    await run()

    expect(outputs['success']).toBe('true')
    expect(outputs['status-code']).toBe('200')
    expect(outputs['response']).toBe(JSON.stringify({ data: 'hello' }))
    expect(core.setFailed).not.toHaveBeenCalled()
  })

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

  it('encrypt=false skips all encryption operations', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ secret: 'visible' }))
    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      encrypt: 'false',
      'encryption-key': ENC_KEY,
      'encrypt-outputs': 'response',
      'max-retries': '0',
    })

    await run()

    // Response should NOT be encrypted even though key + encrypt-outputs are set
    expect(outputs['response']).toBe(JSON.stringify({ secret: 'visible' }))
  })

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

    const callArgs = fetchSpy.mock.calls[0]
    expect(callArgs[1].headers.Authorization).toBe('Bearer my-secret-token')
    const sentBody = JSON.parse(callArgs[1].body)
    expect(sentBody.biscuits['0']).toBe('secret-body-value')
    expect(outputs['success']).toBe('true')
  })

  it('encrypts specified outputs', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ token: 'sensitive-value', name: 'public' }))
    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'extract-outputs': 'token,name',
      'encrypt-outputs': 'token',
      'encryption-key': ENC_KEY,
      'max-retries': '0',
    })

    await run()

    expect(outputs['token']).not.toBe('sensitive-value')
    expect(decryptValue(outputs['token'], ENC_KEY)).toBe('sensitive-value')
    expect(outputs['name']).toBe('public')
  })

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

  it('does not unwrap multi-element array for extract-outputs', async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse([
        { FOO: 'first', BAZ: 1 },
        { FOO: 'second', BAZ: 2 },
      ]),
    )
    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'extract-outputs': 'FOO',
      'max-retries': '0',
    })

    await run()

    expect(outputs['FOO']).toBeUndefined()
    expect(outputs['success']).toBe('true')
  })

  it('sets success=false and status-code on non-retryable error', async () => {
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
    expect(core.setFailed).toHaveBeenCalledWith(expect.stringContaining('HTTP 403'))
  })

  it('retries on 429 then succeeds', async () => {
    fetchSpy
      .mockResolvedValueOnce(
        new Response('rate limited', { status: 429, headers: { 'retry-after': '0' } }),
      )
      .mockResolvedValueOnce(jsonResponse({ ok: true }))

    mockInputs({
      url: 'https://example.com/api',
      method: 'GET',
      'max-retries': '3',
      'retry-delay-ms': '10',
      'retry-on-status': '429,500,502,503,504',
    })

    await run()

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(outputs['success']).toBe('true')
  })
})
