import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

jest.unstable_mockModule('../prompts/system-template.md', () => ({
  default: 'You are an email classifier for influencer inboxes. Return JSON only.',
}))

jest.unstable_mockModule('../prompts/classification-instructions.md', () => ({
  default:
    'Classify each thread.\n\n{{CLASSIFICATION_INSTRUCTIONS}}\n\n# Threads to Classify\n\n{{THREAD_DATA}}',
}))

const { buildPrompt } = await import('../src/lib/build-prompt.js')

function makeEmail(overrides = {}) {
  return {
    id: 'email-1',
    messageId: 'msg-1',
    userId: 'user-1',
    threadId: 'thread-1',
    previousAiSummary: null,
    topLevelHeaders: [
      { name: 'from', value: 'alice@example.com' },
      { name: 'subject', value: 'Partnership Opportunity' },
      { name: 'date', value: '2024-01-15' },
    ],
    body: 'We would like to discuss a brand partnership.',
    ...overrides,
  }
}

describe('buildPrompt', () => {
  it('new thread: no previousAiSummary', () => {
    const emails = [makeEmail()]
    const { systemPrompt, userPrompt } = buildPrompt(emails)

    expect(systemPrompt).toContain('email classifier')
    expect(userPrompt).toContain('--- THREAD 1 ---')
    expect(userPrompt).toContain('Thread ID: thread-1')
    expect(userPrompt).toContain('Previous AI Summary: None')
    expect(userPrompt).toContain('[Message 1]')
    expect(userPrompt).toContain('From: alice@example.com')
    expect(userPrompt).toContain('Subject: Partnership Opportunity')
    expect(userPrompt).toContain('We would like to discuss a brand partnership.')
  })

  it('incremental thread: previousAiSummary present', () => {
    const emails = [makeEmail({ previousAiSummary: 'Previous deal discussion about sponsorship.' })]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('Previous AI Summary: Previous deal discussion about sponsorship.')
    expect(userPrompt).not.toContain('Previous AI Summary: None')
  })

  it('multi-thread batch', () => {
    const emails = [
      makeEmail({ threadId: 'thread-a', previousAiSummary: null }),
      makeEmail({
        threadId: 'thread-b',
        previousAiSummary: 'Prior eval: brand deal in progress.',
        id: 'email-2',
        messageId: 'msg-2',
      }),
    ]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('--- THREAD 1 ---')
    expect(userPrompt).toContain('Thread ID: thread-a')
    expect(userPrompt).toContain('--- THREAD 2 ---')
    expect(userPrompt).toContain('Thread ID: thread-b')
    expect(userPrompt).toContain('Previous AI Summary: Prior eval: brand deal in progress.')
  })

  it('classification instructions placeholder is replaced', () => {
    const emails = [makeEmail()]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).not.toContain('{{CLASSIFICATION_INSTRUCTIONS}}')
    expect(userPrompt).not.toContain('{{THREAD_DATA}}')
  })

  it('system prompt is the persona template', () => {
    const emails = [makeEmail()]
    const { systemPrompt } = buildPrompt(emails)

    expect(systemPrompt).toContain('email classifier')
    expect(systemPrompt).not.toContain('{{')
  })

  it('thread data uses structured format with message numbers', () => {
    const emails = [
      makeEmail(),
      makeEmail({
        id: 'email-2',
        messageId: 'msg-2',
        body: 'Sounds great, lets discuss.',
      }),
    ]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('[Message 1]')
    expect(userPrompt).toContain('[Message 2]')
    expect(userPrompt).toContain('Message Count: 2')
  })
})
