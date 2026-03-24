import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

jest.unstable_mockModule('../prompts/system-template.md', () => ({
  default:
    'You are a brand deal email classifier. Return JSON only.\n\n{{CLASSIFICATION_INSTRUCTIONS}}\n\n{{THREAD_DATA}}',
}))

jest.unstable_mockModule('../prompts/classification-instructions.md', () => ({
  default: '# Classification Instructions\nTest classification content.',
}))

const core = await import('@actions/core')
const { buildPrompt } = await import('../src/lib/build-prompt.js')

function makeEmail(overrides = {}) {
  return {
    id: 'email-1',
    messageId: 'msg-1',
    userId: 'user-1',
    threadId: 'thread-1',
    previousAiSummary: null,
    existingDealId: null,
    topLevelHeaders: [
      { name: 'From', value: 'alice@example.com' },
      { name: 'Subject', value: 'Partnership Opportunity' },
      { name: 'Date', value: '2024-01-15' },
    ],
    labelIds: ['INBOX'],
    body: 'We would like to discuss a brand partnership.',
    replyBody: null,
    ...overrides,
  }
}

describe('buildPrompt', () => {
  it('new thread (isIncremental false): no previousAiSummary', () => {
    const emails = [makeEmail()]
    const { systemPrompt, userPrompt } = buildPrompt(emails)

    expect(systemPrompt).toContain('Classification Instructions')
    expect(systemPrompt).toContain('brand deal email classifier')
    expect(userPrompt).toContain('--- Thread: thread-1 ---')
    expect(userPrompt).not.toContain('Previous AI Summary')
    expect(userPrompt).toContain('Email 1:')
    expect(userPrompt).toContain('From: alice@example.com')
    expect(userPrompt).toContain('Subject: Partnership Opportunity')
    expect(userPrompt).toContain('Date: 2024-01-15')
    expect(userPrompt).toContain('Body: We would like to discuss a brand partnership.')
  })

  it('incremental thread: previousAiSummary present', () => {
    const emails = [makeEmail({ previousAiSummary: 'Previous deal discussion about sponsorship.' })]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('---')
    expect(userPrompt).toContain('Previous AI Summary: Previous deal discussion about sponsorship.')
  })

  it('multi-thread batch', () => {
    const emails = [
      makeEmail({ threadId: 'thread-a', previousAiSummary: null }),
      makeEmail({
        id: 'email-2',
        threadId: 'thread-b',
        previousAiSummary: 'Existing summary.',
        body: 'Follow-up on the deal.',
      }),
    ]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('--- Thread: thread-a ---')
    expect(userPrompt).toContain('--- Thread: thread-b ---')
    expect(userPrompt).toContain('Previous AI Summary: Existing summary.')
  })

  it('empty body shows [no body]', () => {
    const emails = [makeEmail({ body: null, replyBody: null })]
    const { userPrompt } = buildPrompt(emails)
    expect(userPrompt).toContain('Body: [no body]')
  })

  it('uses replyBody when body is missing', () => {
    const emails = [makeEmail({ body: null, replyBody: 'Reply content here' })]
    const { userPrompt } = buildPrompt(emails)
    expect(userPrompt).toContain('Body: Reply content here')
  })

  it('classification instructions injected into system prompt', () => {
    const emails = [makeEmail()]
    const { systemPrompt } = buildPrompt(emails)

    expect(systemPrompt).toContain('# Classification Instructions')
    expect(systemPrompt).toContain('Test classification content')
    expect(systemPrompt).not.toContain('{{CLASSIFICATION_INSTRUCTIONS}}')
  })

  it('groups multiple emails with same threadId', () => {
    const emails = [
      makeEmail({ id: 'email-1', threadId: 'thread-1', body: 'First email' }),
      makeEmail({
        id: 'email-2',
        threadId: 'thread-1',
        body: 'Second email',
        topLevelHeaders: [
          { name: 'From', value: 'bob@example.com' },
          { name: 'Subject', value: 'Re: Partnership' },
          { name: 'Date', value: '2024-01-16' },
        ],
      }),
    ]
    const { userPrompt } = buildPrompt(emails)

    const threadHeaders = userPrompt.match(/--- Thread: thread-1/g)
    expect(threadHeaders).toHaveLength(1)
    expect(userPrompt).toContain('Email 1:')
    expect(userPrompt).toContain('Email 2:')
    expect(userPrompt).toContain('Body: First email')
    expect(userPrompt).toContain('Body: Second email')
  })

  it('uses email.id as threadId when threadId is missing', () => {
    const emails = [makeEmail({ id: 'standalone-email', threadId: undefined })]
    const { userPrompt } = buildPrompt(emails)
    expect(userPrompt).toContain('--- Thread: standalone-email')
  })

  it('handles missing topLevelHeaders', () => {
    const emails = [makeEmail({ topLevelHeaders: undefined })]
    const { userPrompt } = buildPrompt(emails)
    expect(userPrompt).toContain('From:  |')
  })
})
