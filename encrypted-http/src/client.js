/**
 * Pure HTTP client with retry logic. No @actions/core dependency.
 */

export class HttpClientError extends Error {
  constructor(message, status) {
    super(message)
    this.name = 'HttpClientError'
    this.status = status
  }
}

function calculateDelay(attempt, baseDelay) {
  const jitter = Math.random() * baseDelay
  return Math.min(baseDelay * Math.pow(2, attempt) + jitter, 30000)
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

export class HttpClient {
  constructor(config = {}) {
    this.maxRetries = config.maxRetries ?? 3
    this.baseDelay = config.baseDelay ?? 1000
    this.retryStatuses = config.retryStatuses ?? [429, 500, 502, 503, 504]
    this.log = config.log ?? (() => {})
  }

  async request(url, options = {}) {
    const { method = 'GET', headers = {}, body } = options
    const fetchOptions = { method, headers: { ...headers } }

    // Auto-set Content-Type for JSON bodies
    if (body && method !== 'GET' && method !== 'HEAD') {
      const hasContentType = Object.keys(fetchOptions.headers).some(
        (k) => k.toLowerCase() === 'content-type',
      )
      if (!hasContentType) {
        try {
          JSON.parse(typeof body === 'string' ? body : JSON.stringify(body))
          fetchOptions.headers['Content-Type'] = 'application/json'
        } catch {
          // Not JSON, let fetch use default
        }
      }
      fetchOptions.body = body
    }

    let lastError
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        const response = await fetch(url, fetchOptions)

        if (!response.ok) {
          if (attempt < this.maxRetries && this.retryStatuses.includes(response.status)) {
            const retryAfter = response.headers.get('retry-after')
            const delay = retryAfter
              ? parseInt(retryAfter) * 1000
              : calculateDelay(attempt, this.baseDelay)
            this.log(`Retry ${attempt + 1}/${this.maxRetries} after ${delay}ms (status ${response.status})`)
            await sleep(delay)
            continue
          }
          const responseBody = await response.text()
          throw new HttpClientError(`HTTP ${response.status}: ${responseBody}`, response.status)
        }

        return response
      } catch (err) {
        lastError = err
        if (err instanceof HttpClientError) throw err
        if (attempt < this.maxRetries) {
          this.log(`Retry ${attempt + 1}/${this.maxRetries} after network error: ${err.message}`)
          await sleep(calculateDelay(attempt, this.baseDelay))
          continue
        }
        throw err
      }
    }
    throw lastError
  }
}
