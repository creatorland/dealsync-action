import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

jest.unstable_mockModule('../prompts/system.md', () => ({
  default: 'You are an email classifier for influencer inboxes. Return JSON only.',
}))

jest.unstable_mockModule('../prompts/system-llama.md', () => ({
  default: 'You are an email classifier (llama). Return JSON only.',
}))

jest.unstable_mockModule('../prompts/user.md', () => ({
  default:
    'Classify the email threads below. Return one JSON object per thread in a JSON array.\n\n# Threads to Classify\n\n{{THREAD_DATA}}',
}))

const { buildPrompt, parseAndValidate } = await import('../src/lib/ai.js')

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
    expect(userPrompt).toContain('THREAD_ID_INDEX: 1')
    expect(userPrompt).toContain('PREVIOUS_AI_SUMMARY: None')
    expect(userPrompt).toContain('[Message 1]')
    expect(userPrompt).toContain('From: alice@example.com')
    expect(userPrompt).toContain('Subject: Partnership Opportunity')
    expect(userPrompt).toContain('We would like to discuss a brand partnership.')
  })

  it('incremental thread: previousAiSummary present', () => {
    const emails = [makeEmail({ previousAiSummary: 'Previous deal discussion about sponsorship.' })]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).toContain('PREVIOUS_AI_SUMMARY: Previous deal discussion about sponsorship.')
    expect(userPrompt).not.toContain('PREVIOUS_AI_SUMMARY: None')
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

    expect(userPrompt).toContain('THREAD_ID_INDEX: 1')
    expect(userPrompt).toContain('THREAD_ID_INDEX: 2')
    expect(userPrompt).toContain('PREVIOUS_AI_SUMMARY: Prior eval: brand deal in progress.')
  })

  it('thread data placeholder is replaced', () => {
    const emails = [makeEmail()]
    const { userPrompt } = buildPrompt(emails)

    expect(userPrompt).not.toContain('{{THREAD_DATA}}')
    expect(userPrompt).toContain('Classify the email threads below')
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

describe('parseAndValidate — deal_value contract', () => {
  // parseAndValidate is the single source of truth for the parsed thread shape.
  // deal_value must come out as number|null — never NaN/Infinity — so the SQL
  // write paths can trust it. The AI can return garbage (a non-numeric string,
  // an object), and a raw Number() of that would be NaN, which renders as
  // invalid SQL; toFiniteNumberOrNull clamps it to null at the source.
  function parseOne(dealValue) {
    const raw = JSON.stringify([{ thread_id: 't1', is_deal: true, deal_value: dealValue }])
    return parseAndValidate(raw)[0]
  }

  it('keeps a finite number (including decimals) as-is', () => {
    expect(parseOne(1000).deal_value).toBe(1000)
    expect(parseOne(1234.56).deal_value).toBe(1234.56)
  })

  it('preserves a real zero rather than coercing it to null', () => {
    expect(parseOne(0).deal_value).toBe(0)
  })

  it('coerces a numeric string to a number', () => {
    expect(parseOne('2500').deal_value).toBe(2500)
  })

  it('maps a non-numeric value to null instead of NaN', () => {
    expect(parseOne('not-a-number').deal_value).toBeNull()
  })

  it('maps a non-finite value (Infinity) to null', () => {
    expect(parseOne('1e999').deal_value).toBeNull()
  })

  it('maps absent / null deal_value to null', () => {
    const raw = JSON.stringify([
      { thread_id: 't1', is_deal: true },
      { thread_id: 't2', is_deal: true, deal_value: null },
    ])
    const result = parseAndValidate(raw)
    expect(result[0].deal_value).toBeNull()
    expect(result[1].deal_value).toBeNull()
  })

  it('never emits NaN for any deal_value input', () => {
    const raw = JSON.stringify(
      [1000, '2500', 'garbage', '1e999', 0, null, {}, []].map((v, i) => ({
        thread_id: `t${i}`,
        is_deal: true,
        deal_value: v,
      })),
    )
    for (const r of parseAndValidate(raw)) {
      expect(r.deal_value === null || Number.isFinite(r.deal_value)).toBe(true)
    }
  })
})
