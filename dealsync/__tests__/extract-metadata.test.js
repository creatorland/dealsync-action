import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

const core = await import('@actions/core')
const { runExtractMetadata } = await import('../src/extract-metadata.js')

function mockInputs(map) {
  core.getInput.mockImplementation((name) => map[name] ?? '')
}

describe('extract-metadata command', () => {
  beforeEach(() => jest.clearAllMocks())

  it('extracts userId, messageIds, and syncStateId from metadata rows', async () => {
    const metadata = [
      { USER_ID: 'u1', MESSAGE_ID: 'msg-1', SYNC_STATE_ID: 'ss-1', EMAIL_METADATA_ID: 'e1' },
      { USER_ID: 'u1', MESSAGE_ID: 'msg-2', SYNC_STATE_ID: 'ss-1', EMAIL_METADATA_ID: 'e2' },
      { USER_ID: 'u1', MESSAGE_ID: 'msg-3', SYNC_STATE_ID: 'ss-1', EMAIL_METADATA_ID: 'e3' },
    ]
    mockInputs({ metadata: JSON.stringify(metadata) })

    const result = await runExtractMetadata()

    expect(result.userId).toBe('u1')
    expect(result.messageIds).toEqual(['msg-1', 'msg-2', 'msg-3'])
    expect(result.syncStateId).toBe('ss-1')
  })

  it('returns empty for empty array', async () => {
    mockInputs({ metadata: '[]' })

    const result = await runExtractMetadata()

    expect(result.userId).toBe('')
    expect(result.messageIds).toEqual([])
  })

  it('returns empty for missing input', async () => {
    mockInputs({ metadata: '' })

    const result = await runExtractMetadata()

    expect(result.userId).toBe('')
    expect(result.messageIds).toEqual([])
  })

  it('preserves original metadata in output', async () => {
    const metadata = [
      { USER_ID: 'u1', MESSAGE_ID: 'msg-1', SYNC_STATE_ID: 'ss-1', THREAD_ID: 't1' },
    ]
    const raw = JSON.stringify(metadata)
    mockInputs({ metadata: raw })

    const result = await runExtractMetadata()

    expect(result.metadata).toBe(raw)
  })
})
