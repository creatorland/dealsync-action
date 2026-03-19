import { describe, it, expect, vi, beforeEach } from 'vitest'

// Mock the .md import so it works without Rollup
vi.mock(
  '../build-ai-prompt/prompts/classification-instructions.md',
  async () => {
    const fs = await import('fs')
    const path = await import('path')
    const mdPath = path.resolve(
      import.meta.dirname,
      '../build-ai-prompt/prompts/classification-instructions.md',
    )
    const content = fs.readFileSync(mdPath, 'utf-8')
    return { default: content }
  },
)

// Mock @actions/core
const mockInputs = {}
const mockOutputs = {}
let mockFailedMessage = null

vi.mock('@actions/core', () => ({
  getInput: vi.fn(name => mockInputs[name] || ''),
  setOutput: vi.fn((name, value) => {
    mockOutputs[name] = value
  }),
  setFailed: vi.fn(msg => {
    mockFailedMessage = msg
  }),
}))

import { buildPrompt } from '../build-ai-prompt/src/prompt-template.js'
import { run } from '../build-ai-prompt/src/main.js'

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

    // System prompt includes classification instructions
    expect(systemPrompt).toContain('What is a Deal?')
    expect(systemPrompt).toContain('deal classification engine')

    // User prompt shows isIncremental: false
    expect(userPrompt).toContain('(isIncremental: false)')
    expect(userPrompt).not.toContain('(isIncremental: true)')

    // All email data present
    expect(userPrompt).toContain('Email 1:')
    expect(userPrompt).toContain('From: alice@example.com')
    expect(userPrompt).toContain('Subject: Partnership Opportunity')
    expect(userPrompt).toContain('Date: 2024-01-15')
    expect(userPrompt).toContain(
      'Body: We would like to discuss a brand partnership.',
    )
  })

  it('incremental thread: previousAiSummary present', () => {
    const emails = [
      makeEmail({
        previousAiSummary: 'Previous deal discussion about sponsorship.',
      }),
    ]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('(isIncremental: true)')
    expect(userPrompt).toContain(
      'Previous AI Summary: Previous deal discussion about sponsorship.',
    )
  })

  it('multi-thread batch: two threads, one new and one incremental', () => {
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

    expect(userPrompt).toContain('--- Thread: thread-a (isIncremental: false)')
    expect(userPrompt).toContain('--- Thread: thread-b (isIncremental: true)')
    expect(userPrompt).toContain('Previous AI Summary: Existing summary.')
  })

  it('empty body handling: no body or replyBody shows [no body]', () => {
    const emails = [makeEmail({ body: null, replyBody: null })]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('Body: [no body]')
  })

  it('uses replyBody when body is missing', () => {
    const emails = [makeEmail({ body: null, replyBody: 'Reply content here' })]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('Body: Reply content here')
  })

  it('classification instructions injection: system prompt contains md content', () => {
    const emails = [makeEmail()]
    const { systemPrompt } = buildPrompt(emails)

    expect(systemPrompt).toContain('# Classification Instructions')
    expect(systemPrompt).toContain('## What is a Deal?')
    expect(systemPrompt).toContain('## What is NOT a Deal?')
    expect(systemPrompt).toContain('## Scoring Guide (ai_score 1-10)')
    expect(systemPrompt).toContain('## Category Definitions')
    expect(systemPrompt).toContain('## Language Detection')
    // The placeholder should be replaced
    expect(systemPrompt).not.toContain('{{CLASSIFICATION_INSTRUCTIONS}}')
  })

  it('thread grouping: multiple emails with same threadId grouped together', () => {
    const emails = [
      makeEmail({
        id: 'email-1',
        threadId: 'thread-1',
        body: 'First email in thread',
      }),
      makeEmail({
        id: 'email-2',
        threadId: 'thread-1',
        body: 'Second email in thread',
        topLevelHeaders: [
          { name: 'From', value: 'bob@example.com' },
          { name: 'Subject', value: 'Re: Partnership Opportunity' },
          { name: 'Date', value: '2024-01-16' },
        ],
      }),
    ]
    const { userPrompt } = buildPrompt(emails)

    // Only one thread header
    const threadHeaders = userPrompt.match(/--- Thread: thread-1/g)
    expect(threadHeaders).toHaveLength(1)

    // Both emails listed
    expect(userPrompt).toContain('Email 1:')
    expect(userPrompt).toContain('Email 2:')
    expect(userPrompt).toContain('Body: First email in thread')
    expect(userPrompt).toContain('Body: Second email in thread')
    expect(userPrompt).toContain('From: alice@example.com')
    expect(userPrompt).toContain('From: bob@example.com')
  })

  it('uses email.id as threadId when threadId is missing', () => {
    const emails = [
      makeEmail({ id: 'standalone-email', threadId: undefined }),
    ]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('--- Thread: standalone-email')
  })

  it('handles missing topLevelHeaders gracefully', () => {
    const emails = [makeEmail({ topLevelHeaders: undefined })]
    const { userPrompt } = buildPrompt(emails)

    // Should show empty values for headers
    expect(userPrompt).toContain('From:  |')
    expect(userPrompt).toContain('Subject:  |')
    expect(userPrompt).toContain('Date: ')
  })
})

describe('run (main.js)', () => {
  beforeEach(() => {
    for (const key in mockInputs) delete mockInputs[key]
    for (const key in mockOutputs) delete mockOutputs[key]
    mockFailedMessage = null
  })

  it('empty input: returns empty prompts and success true', async () => {
    mockInputs['emails'] = '[]'
    await run()

    expect(mockOutputs['system-prompt']).toBe('')
    expect(mockOutputs['user-prompt']).toBe('')
    expect(mockOutputs['success']).toBe('true')
    expect(mockFailedMessage).toBeNull()
  })

  it('missing input: returns empty prompts and success true', async () => {
    // getInput returns '' for missing keys
    await run()

    expect(mockOutputs['system-prompt']).toBe('')
    expect(mockOutputs['user-prompt']).toBe('')
    expect(mockOutputs['success']).toBe('true')
  })

  it('valid emails: builds prompts and sets outputs', async () => {
    const emails = [makeEmail()]
    mockInputs['emails'] = JSON.stringify(emails)
    await run()

    expect(mockOutputs['success']).toBe('true')
    expect(mockOutputs['system-prompt']).toContain('deal classification engine')
    expect(mockOutputs['user-prompt']).toContain('thread-1')
    expect(mockFailedMessage).toBeNull()
  })

  it('invalid JSON: sets success false and calls setFailed', async () => {
    mockInputs['emails'] = '{not valid json'
    await run()

    expect(mockOutputs['success']).toBe('false')
    expect(mockFailedMessage).toBeTruthy()
  })
})
