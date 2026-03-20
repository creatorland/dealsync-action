import { jest } from '@jest/globals'
import { HttpClient, HttpClientError } from '../src/client.js'

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

describe('HttpClient', () => {
  let fetchSpy

  beforeEach(() => {
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  it('makes a successful GET request', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ data: 'hello' }))

    const client = new HttpClient({ maxRetries: 0 })
    const response = await client.request('https://example.com/api')

    expect(fetchSpy).toHaveBeenCalledTimes(1)
    expect(response.ok).toBe(true)
  })

  it('retries on 429 with Retry-After header', async () => {
    fetchSpy
      .mockResolvedValueOnce(
        new Response('rate limited', {
          status: 429,
          headers: { 'retry-after': '0' },
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ ok: true }))

    const client = new HttpClient({ maxRetries: 3, baseDelay: 10 })
    const response = await client.request('https://example.com/api')

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(response.ok).toBe(true)
  })

  it('retries on 500 with exponential backoff', async () => {
    fetchSpy
      .mockResolvedValueOnce(textResponse('server error', 500))
      .mockResolvedValueOnce(textResponse('server error', 500))
      .mockResolvedValueOnce(jsonResponse({ recovered: true }))

    const client = new HttpClient({ maxRetries: 3, baseDelay: 10 })
    const response = await client.request('https://example.com/api')

    expect(fetchSpy).toHaveBeenCalledTimes(3)
    expect(response.ok).toBe(true)
  })

  it('throws HttpClientError after max retries exceeded', async () => {
    fetchSpy
      .mockResolvedValueOnce(textResponse('error', 500))
      .mockResolvedValueOnce(textResponse('error', 500))
      .mockResolvedValueOnce(textResponse('error', 500))
      .mockResolvedValueOnce(textResponse('still error', 500))

    const client = new HttpClient({ maxRetries: 3, baseDelay: 10 })

    await expect(client.request('https://example.com/api')).rejects.toThrow(HttpClientError)
    expect(fetchSpy).toHaveBeenCalledTimes(4)
  })

  it('auto-sets Content-Type for JSON bodies', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ ok: true }))

    const client = new HttpClient({ maxRetries: 0 })
    await client.request('https://example.com/api', {
      method: 'POST',
      body: JSON.stringify({ name: 'test' }),
    })

    const callArgs = fetchSpy.mock.calls[0]
    expect(callArgs[1].headers['Content-Type']).toBe('application/json')
  })

  it('does not override existing Content-Type', async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ ok: true }))

    const client = new HttpClient({ maxRetries: 0 })
    await client.request('https://example.com/api', {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain' },
      body: JSON.stringify({ name: 'test' }),
    })

    const callArgs = fetchSpy.mock.calls[0]
    expect(callArgs[1].headers['Content-Type']).toBe('text/plain')
  })

  it('retries on network errors', async () => {
    fetchSpy
      .mockRejectedValueOnce(new Error('fetch failed'))
      .mockResolvedValueOnce(jsonResponse({ ok: true }))

    const client = new HttpClient({ maxRetries: 2, baseDelay: 10 })
    const response = await client.request('https://example.com/api')

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(response.ok).toBe(true)
  })

  it('throws on non-retryable status codes', async () => {
    fetchSpy.mockResolvedValueOnce(textResponse('forbidden', 403))

    const client = new HttpClient({ maxRetries: 3, baseDelay: 10 })

    await expect(client.request('https://example.com/api')).rejects.toThrow('HTTP 403')
    expect(fetchSpy).toHaveBeenCalledTimes(1)
  })

  it('includes status on HttpClientError', async () => {
    fetchSpy.mockResolvedValueOnce(textResponse('forbidden', 403))

    const client = new HttpClient({ maxRetries: 0 })

    try {
      await client.request('https://example.com/api')
    } catch (err) {
      expect(err).toBeInstanceOf(HttpClientError)
      expect(err.status).toBe(403)
    }
  })
})
