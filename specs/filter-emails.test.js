import { vi, describe, it, expect, beforeEach } from 'vitest'

const mockOutputs = {}

vi.mock('@actions/core', () => {
  return {
    getInput: vi.fn(),
    setOutput: vi.fn((name, value) => {
      mockOutputs[name] = value
    }),
    setFailed: vi.fn(),
  }
})

import * as core from '@actions/core'
import { run } from '../filter-emails/src/main.js'
import {
  checkAuthenticationResults,
  checkSender,
  checkBulkHeaders,
  checkSubject,
  checkSenderName,
  checkFreeEmail,
} from '../filter-emails/src/rules.js'

function makeEmail(overrides = {}) {
  return {
    id: 'test-id-1',
    messageId: 'msg-1',
    userId: 'user-1',
    topLevelHeaders: [
      { name: 'From', value: 'Jane Smith <jane@example.com>' },
      { name: 'Subject', value: 'Partnership Opportunity' },
      { name: 'Authentication-Results', value: 'dkim=pass spf=pass' },
    ],
    labelIds: [],
    ...overrides,
  }
}

describe('filter-emails', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    Object.keys(mockOutputs).forEach((key) => delete mockOutputs[key])
  })

  describe('run()', () => {
    it('passes email through all rules into filtered_ids', async () => {
      core.getInput.mockReturnValue(JSON.stringify([makeEmail()]))
      await run()
      expect(mockOutputs.filtered_ids).toBe("'test-id-1'")
      expect(mockOutputs.rejected_ids).toBe('')
      expect(mockOutputs.success).toBe('true')
    })

    it('returns empty outputs for empty input', async () => {
      core.getInput.mockReturnValue('[]')
      await run()
      expect(mockOutputs.filtered_ids).toBe('')
      expect(mockOutputs.rejected_ids).toBe('')
      expect(mockOutputs.success).toBe('true')
    })

    it('returns empty outputs for missing input', async () => {
      core.getInput.mockReturnValue('')
      await run()
      expect(mockOutputs.filtered_ids).toBe('')
      expect(mockOutputs.rejected_ids).toBe('')
      expect(mockOutputs.success).toBe('true')
    })

    it('splits multiple emails into filtered and rejected', async () => {
      const emails = [
        makeEmail({ id: 'good-1' }),
        makeEmail({
          id: 'bad-1',
          topLevelHeaders: [
            { name: 'From', value: 'noreply@spam.com' },
            { name: 'Subject', value: 'Hello' },
            { name: 'Authentication-Results', value: 'dkim=pass' },
          ],
        }),
        makeEmail({ id: 'good-2' }),
      ]
      core.getInput.mockReturnValue(JSON.stringify(emails))
      await run()
      expect(mockOutputs.filtered_ids).toBe("'good-1','good-2'")
      expect(mockOutputs.rejected_ids).toBe("'bad-1'")
      expect(mockOutputs.success).toBe('true')
    })

    it('rejects IDs with invalid characters', async () => {
      const emails = [
        makeEmail({ id: "'; DROP TABLE" }),
      ]
      core.getInput.mockReturnValue(JSON.stringify(emails))
      await run()
      expect(mockOutputs.success).toBe('false')
      expect(core.setFailed).toHaveBeenCalledWith(
        expect.stringContaining('Invalid ID format'),
      )
    })

    it('handles parse error gracefully', async () => {
      core.getInput.mockReturnValue('{not valid json')
      await run()
      expect(mockOutputs.success).toBe('false')
      expect(core.setFailed).toHaveBeenCalled()
    })
  })

  describe('Rule 1: checkAuthenticationResults', () => {
    it('passes when auth header is missing', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'Subject', value: 'Hello' },
        ],
      })
      expect(checkAuthenticationResults(email)).toBe(false)
    })

    it('passes when dkim=pass is present', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'Authentication-Results', value: 'dkim=pass spf=fail' },
        ],
      })
      expect(checkAuthenticationResults(email)).toBe(false)
    })

    it('passes when spf=pass is present', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'Authentication-Results', value: 'dkim=fail spf=pass' },
        ],
      })
      expect(checkAuthenticationResults(email)).toBe(false)
    })

    it('passes when dmarc=pass is present', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'Authentication-Results', value: 'dmarc=pass' },
        ],
      })
      expect(checkAuthenticationResults(email)).toBe(false)
    })

    it('rejects when auth header exists but no pass values', () => {
      const email = makeEmail({
        topLevelHeaders: [
          {
            name: 'Authentication-Results',
            value: 'dkim=fail spf=fail dmarc=fail',
          },
        ],
      })
      expect(checkAuthenticationResults(email)).toBe(true)
    })

    it('handles case-insensitive header name', () => {
      const email = makeEmail({
        topLevelHeaders: [
          {
            name: 'authentication-results',
            value: 'dkim=fail spf=fail',
          },
        ],
      })
      expect(checkAuthenticationResults(email)).toBe(true)
    })
  })

  describe('Rule 2: checkSender', () => {
    it('passes for normal sender', () => {
      const email = makeEmail()
      expect(checkSender(email)).toBe(false)
    })

    it('rejects blocked prefix (noreply@)', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'No Reply <noreply@company.com>' },
        ],
      })
      expect(checkSender(email)).toBe(true)
    })

    it('rejects blocked prefix (do-not-reply@)', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'do-not-reply@company.com' },
        ],
      })
      expect(checkSender(email)).toBe(true)
    })

    it('rejects blocked domain suffix (.gov)', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'John <john@agency.gov>' },
        ],
      })
      expect(checkSender(email)).toBe(true)
    })

    it('rejects blocked domain containing marketing.', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'promo <hi@marketing.company.com>' },
        ],
      })
      expect(checkSender(email)).toBe(true)
    })

    it('rejects blocked domain containing newsletter.', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'News <news@newsletter.example.com>' },
        ],
      })
      expect(checkSender(email)).toBe(true)
    })

    it('passes when from header is missing', () => {
      const email = makeEmail({ topLevelHeaders: [] })
      expect(checkSender(email)).toBe(false)
    })

    it('handles bare email without angle brackets', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'support@company.com' }],
      })
      expect(checkSender(email)).toBe(true)
    })
  })

  describe('Rule 3: checkBulkHeaders', () => {
    it('passes for normal email', () => {
      const email = makeEmail()
      expect(checkBulkHeaders(email)).toBe(false)
    })

    it('rejects when list-id header present', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'List-Id', value: '<list.example.com>' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })

    it('rejects when x-mailing-list header present', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'X-Mailing-List', value: 'announcements' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })

    it('rejects when precedence is bulk', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'Precedence', value: 'bulk' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })

    it('rejects when marketing tool name found in header value', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'X-Mailer', value: 'Mailchimp Campaign v3.2' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })

    it('rejects sendgrid in header value', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'X-SG-EID', value: 'sendgrid-12345' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })

    it('rejects precedence auto_reply', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'Precedence', value: 'auto_reply' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })
  })

  describe('Rule 4: checkSubject', () => {
    it('passes for normal subject', () => {
      const email = makeEmail()
      expect(checkSubject(email)).toBe(false)
    })

    it('rejects subject containing "newsletter"', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'Subject', value: 'Weekly Newsletter - March 2026' },
        ],
      })
      expect(checkSubject(email)).toBe(true)
    })

    it('rejects subject containing "order confirmation"', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'Subject', value: 'Your Order Confirmation #1234' },
        ],
      })
      expect(checkSubject(email)).toBe(true)
    })

    it('rejects subject containing "special offer"', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'Subject', value: 'Special Offer Just for You!' },
        ],
      })
      expect(checkSubject(email)).toBe(true)
    })

    it('rejects subject containing "weekly digest"', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'Subject', value: 'Your Weekly Digest' },
        ],
      })
      expect(checkSubject(email)).toBe(true)
    })

    it('passes when subject header is missing', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
        ],
      })
      expect(checkSubject(email)).toBe(false)
    })
  })

  describe('Rule 5: checkSenderName', () => {
    it('passes for personalized name', () => {
      const email = makeEmail()
      expect(checkSenderName(email)).toBe(false)
    })

    it('rejects "The Team" sender name', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'The Team <team@example.com>' },
        ],
      })
      expect(checkSenderName(email)).toBe(true)
    })

    it('rejects "Newsletter" sender name', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Newsletter <news@example.com>' },
        ],
      })
      expect(checkSenderName(email)).toBe(true)
    })

    it('rejects "Customer Support" sender name', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Customer Support <cs@example.com>' },
        ],
      })
      expect(checkSenderName(email)).toBe(true)
    })

    it('rejects quoted non-personalized name', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: '"The Team" <team@example.com>' },
        ],
      })
      expect(checkSenderName(email)).toBe(true)
    })

    it('passes when no display name present', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'jane@example.com' }],
      })
      expect(checkSenderName(email)).toBe(false)
    })

    it('passes when from header missing', () => {
      const email = makeEmail({ topLevelHeaders: [] })
      expect(checkSenderName(email)).toBe(false)
    })
  })

  describe('Rule 6: checkFreeEmail', () => {
    it('passes for normal business email', () => {
      const email = makeEmail()
      expect(checkFreeEmail(email)).toBe(false)
    })

    it('rejects info@ on gmail', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Info <info@gmail.com>' },
        ],
      })
      expect(checkFreeEmail(email)).toBe(true)
    })

    it('rejects noreply@ on yahoo', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'No Reply <noreply@yahoo.com>' },
        ],
      })
      expect(checkFreeEmail(email)).toBe(true)
    })

    it('rejects support@ on outlook', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Support <support@outlook.com>' },
        ],
      })
      expect(checkFreeEmail(email)).toBe(true)
    })

    it('rejects newsletter@ on hotmail', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'NL <newsletter@hotmail.com>' },
        ],
      })
      expect(checkFreeEmail(email)).toBe(true)
    })

    it('passes for personal name on gmail', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@gmail.com>' },
        ],
      })
      expect(checkFreeEmail(email)).toBe(false)
    })

    it('rejects no-reply (with hyphen) on gmail', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'NR <no-reply@gmail.com>' },
        ],
      })
      expect(checkFreeEmail(email)).toBe(true)
    })

    it('passes when from header missing', () => {
      const email = makeEmail({ topLevelHeaders: [] })
      expect(checkFreeEmail(email)).toBe(false)
    })
  })

  describe('ID sanitization', () => {
    it('allows alphanumeric IDs with hyphens and underscores', async () => {
      const emails = [makeEmail({ id: 'abc-123_DEF' })]
      core.getInput.mockReturnValue(JSON.stringify(emails))
      await run()
      expect(mockOutputs.filtered_ids).toBe("'abc-123_DEF'")
      expect(mockOutputs.success).toBe('true')
    })

    it('rejects IDs with SQL injection characters', async () => {
      const emails = [makeEmail({ id: "'; DROP TABLE emails--" })]
      core.getInput.mockReturnValue(JSON.stringify(emails))
      await run()
      expect(mockOutputs.success).toBe('false')
      expect(core.setFailed).toHaveBeenCalledWith(
        expect.stringContaining('Invalid ID format'),
      )
    })

    it('rejects IDs with spaces', async () => {
      const emails = [makeEmail({ id: 'id with spaces' })]
      core.getInput.mockReturnValue(JSON.stringify(emails))
      await run()
      expect(mockOutputs.success).toBe('false')
    })
  })
})
