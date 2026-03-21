import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

const core = await import('@actions/core')
const { runFetchContent } = await import('../src/fetch-content.js')

function mockInputs(map) {
  core.getInput.mockImplementation((name) => map[name] ?? '')
}

function makeMetadataRow(overrides = {}) {
  return {
    EMAIL_METADATA_ID: 'em-1',
    MESSAGE_ID: 'msg-1',
    USER_ID: 'user-1',
    SYNC_STATE_ID: 'ss-1',
    THREAD_ID: 'thread-1',
    ...overrides,
  }
}

function fetchResponse(emails) {
  return new Response(JSON.stringify({ data: emails }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

describe('fetch-content command', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  it('returns empty for empty metadata', async () => {
    mockInputs({ metadata: '[]', 'content-fetcher-url': 'https://fetcher.example.com' })

    const result = await runFetchContent()

    expect(result).toBe('[]')
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('fetches content for a single batch (<= 50 messages)', async () => {
    const rows = [
      makeMetadataRow({ EMAIL_METADATA_ID: 'em-1', MESSAGE_ID: 'msg-1' }),
      makeMetadataRow({ EMAIL_METADATA_ID: 'em-2', MESSAGE_ID: 'msg-2' }),
    ]
    mockInputs({
      metadata: JSON.stringify(rows),
      'content-fetcher-url': 'https://fetcher.example.com',
    })

    fetchSpy.mockResolvedValueOnce(
      fetchResponse([
        { messageId: 'msg-1', body: 'Hello' },
        { messageId: 'msg-2', body: 'World' },
      ]),
    )

    const result = await runFetchContent()
    const emails = JSON.parse(result)

    expect(emails).toHaveLength(2)
    expect(emails[0].id).toBe('em-1')
    expect(emails[0].threadId).toBe('thread-1')
    expect(emails[0].body).toBe('Hello')
    expect(emails[1].id).toBe('em-2')

    expect(fetchSpy).toHaveBeenCalledTimes(1)
    const callBody = JSON.parse(fetchSpy.mock.calls[0][1].body)
    expect(callBody.userId).toBe('user-1')
    expect(callBody.syncStateId).toBe('ss-1')
    expect(callBody.messageIds).toEqual(['msg-1', 'msg-2'])
  })

  it('chunks into multiple batches when > 50 messages', async () => {
    const rows = Array.from({ length: 75 }, (_, i) =>
      makeMetadataRow({ EMAIL_METADATA_ID: `em-${i}`, MESSAGE_ID: `msg-${i}` }),
    )
    mockInputs({
      metadata: JSON.stringify(rows),
      'content-fetcher-url': 'https://fetcher.example.com',
    })

    fetchSpy.mockResolvedValueOnce(
      fetchResponse(
        Array.from({ length: 50 }, (_, i) => ({ messageId: `msg-${i}`, body: `body-${i}` })),
      ),
    )
    fetchSpy.mockResolvedValueOnce(
      fetchResponse(
        Array.from({ length: 25 }, (_, i) => ({
          messageId: `msg-${i + 50}`,
          body: `body-${i + 50}`,
        })),
      ),
    )

    const result = await runFetchContent()
    const emails = JSON.parse(result)

    expect(fetchSpy).toHaveBeenCalledTimes(2)
    expect(emails).toHaveLength(75)

    const call1Body = JSON.parse(fetchSpy.mock.calls[0][1].body)
    expect(call1Body.messageIds).toHaveLength(50)

    const call2Body = JSON.parse(fetchSpy.mock.calls[1][1].body)
    expect(call2Body.messageIds).toHaveLength(25)
  })

  it('merges previousAiSummary and existingDealId from metadata', async () => {
    const rows = [
      makeMetadataRow({
        MESSAGE_ID: 'msg-1',
        PREVIOUS_AI_SUMMARY: 'Previous deal discussion',
        EXISTING_DEAL_ID: 'deal-123',
      }),
    ]
    mockInputs({
      metadata: JSON.stringify(rows),
      'content-fetcher-url': 'https://fetcher.example.com',
    })

    fetchSpy.mockResolvedValueOnce(fetchResponse([{ messageId: 'msg-1', body: 'Hello' }]))

    const result = await runFetchContent()
    const emails = JSON.parse(result)

    expect(emails[0].previousAiSummary).toBe('Previous deal discussion')
    expect(emails[0].existingDealId).toBe('deal-123')
  })

  it('encrypts output when encryption key provided', async () => {
    const { decryptValue } = await import('../../shared/crypto.js')
    const key = 'a'.repeat(64)
    const rows = [makeMetadataRow()]
    mockInputs({
      metadata: JSON.stringify(rows),
      'content-fetcher-url': 'https://fetcher.example.com',
      'encryption-key': key,
    })

    fetchSpy.mockResolvedValueOnce(fetchResponse([{ messageId: 'msg-1', body: 'secret' }]))

    const result = await runFetchContent()

    // result should be encrypted string
    expect(result).not.toContain('secret')
    const decrypted = JSON.parse(decryptValue(result, key))
    expect(decrypted[0].body).toBe('secret')
  })

  it('continues on batch failure and reports failed_ids via setOutput', async () => {
    const rows = Array.from({ length: 75 }, (_, i) =>
      makeMetadataRow({ EMAIL_METADATA_ID: `em-${i}`, MESSAGE_ID: `msg-${i}` }),
    )
    mockInputs({
      metadata: JSON.stringify(rows),
      'content-fetcher-url': 'https://fetcher.example.com',
    })

    fetchSpy.mockResolvedValueOnce(
      fetchResponse(
        Array.from({ length: 50 }, (_, i) => ({ messageId: `msg-${i}`, body: `body-${i}` })),
      ),
    )
    fetchSpy.mockResolvedValueOnce(new Response('Internal Server Error', { status: 500 }))

    const result = await runFetchContent()
    const emails = JSON.parse(result)

    expect(emails).toHaveLength(50)
    expect(core.setOutput).toHaveBeenCalledWith('failed_ids', expect.stringContaining("'em-50'"))
    expect(core.error).toHaveBeenCalledWith(expect.stringContaining('Batch 2/2 failed'))
  })

  it('calls correct URL', async () => {
    mockInputs({
      metadata: JSON.stringify([makeMetadataRow()]),
      'content-fetcher-url': 'https://my-fetcher.example.com',
    })

    fetchSpy.mockResolvedValueOnce(fetchResponse([{ messageId: 'msg-1', body: 'test' }]))

    await runFetchContent()

    expect(fetchSpy.mock.calls[0][0]).toBe('https://my-fetcher.example.com/email-content/fetch')
  })
})
