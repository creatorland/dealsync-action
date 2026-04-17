import { jest } from '@jest/globals'

const mockGetInput = jest.fn()
const mockSetSecret = jest.fn()

jest.unstable_mockModule('@actions/core', () => ({
  getInput: mockGetInput,
  setSecret: mockSetSecret,
  setFailed: jest.fn(),
  error: jest.fn(),
}))

const { resolveFirestoreServiceAccountJson } = await import(
  '../src/commands/emit-scan-complete-webhooks.js'
)

describe('resolveFirestoreServiceAccountJson', () => {
  const saved = process.env.FIRESTORE_SERVICE_ACCOUNT_JSON

  afterEach(() => {
    jest.clearAllMocks()
    if (saved === undefined) {
      delete process.env.FIRESTORE_SERVICE_ACCOUNT_JSON
    } else {
      process.env.FIRESTORE_SERVICE_ACCOUNT_JSON = saved
    }
  })

  it('returns action input when set (non-empty)', () => {
    mockGetInput.mockImplementation((name) =>
      name === 'firestore-service-account-json' ? '{"project_id":"p"}' : '',
    )
    delete process.env.FIRESTORE_SERVICE_ACCOUNT_JSON
    expect(resolveFirestoreServiceAccountJson()).toBe('{"project_id":"p"}')
  })

  it('prefers action input over env when both set', () => {
    mockGetInput.mockImplementation((name) =>
      name === 'firestore-service-account-json' ? '{"from":"input"}' : '',
    )
    process.env.FIRESTORE_SERVICE_ACCOUNT_JSON = '{"from":"env"}'
    expect(resolveFirestoreServiceAccountJson()).toBe('{"from":"input"}')
  })

  it('uses FIRESTORE_SERVICE_ACCOUNT_JSON when input is empty', () => {
    mockGetInput.mockReturnValue('')
    process.env.FIRESTORE_SERVICE_ACCOUNT_JSON = '{"project_id":"env"}'
    expect(resolveFirestoreServiceAccountJson()).toBe('{"project_id":"env"}')
  })

  it('returns empty string when neither source is set', () => {
    mockGetInput.mockReturnValue('')
    delete process.env.FIRESTORE_SERVICE_ACCOUNT_JSON
    expect(resolveFirestoreServiceAccountJson()).toBe('')
  })

  it('ignores whitespace-only env', () => {
    mockGetInput.mockReturnValue('')
    process.env.FIRESTORE_SERVICE_ACCOUNT_JSON = '   \n  '
    expect(resolveFirestoreServiceAccountJson()).toBe('')
  })
})
