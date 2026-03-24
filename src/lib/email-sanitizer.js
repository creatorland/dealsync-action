/**
 * Email body sanitization for AI classification.
 *
 * Cleans raw email content before sending to the classifier:
 * 1. HTML → plaintext (preserves structure, strips tags)
 * 2. Strip quoted replies and forwarded content
 * 3. Strip email signatures
 * 4. Collapse excessive whitespace
 * 5. Truncate to max character limit
 */

import { convert } from 'html-to-text'
import EmailReplyParser from 'email-reply-parser'

const MAX_BODY_CHARS = 3000 // Per email — keeps token usage reasonable for batches of 5

/**
 * Sanitize an email body for AI classification.
 * @param {string} body - Raw email body (may be HTML or plaintext)
 * @returns {string} Cleaned plaintext suitable for AI prompt
 */
export function sanitizeEmailBody(body) {
  if (!body || typeof body !== 'string') return ''

  // Step 1: HTML to plaintext
  let text = body
  if (text.includes('<') && (text.includes('</') || text.includes('/>'))) {
    text = convert(text, {
      wordwrap: false,
      selectors: [
        { selector: 'a', options: { hideLinkHrefIfSameAsText: true } },
        { selector: 'img', format: 'skip' },
        { selector: 'style', format: 'skip' },
        { selector: 'script', format: 'skip' },
      ],
    })
  }

  // Step 2: Strip quoted replies and signatures using email-reply-parser
  try {
    const parsed = new EmailReplyParser().read(text)
    // Get only visible (non-quoted, non-signature) fragments
    const visible = parsed.getVisibleText({ aggressive: true })
    if (visible && visible.trim().length > 0) {
      text = visible
    }
  } catch {
    // Parser failed — use original text
  }

  // Step 3: Collapse whitespace
  text = text
    .replace(/\r\n/g, '\n')           // Normalize line endings
    .replace(/\n{3,}/g, '\n\n')       // Max 2 consecutive newlines
    .replace(/[ \t]{2,}/g, ' ')       // Collapse horizontal whitespace
    .replace(/^\s+$/gm, '')           // Remove whitespace-only lines
    .trim()

  // Step 4: Truncate
  if (text.length > MAX_BODY_CHARS) {
    text = text.substring(0, MAX_BODY_CHARS) + '\n[... truncated]'
  }

  return text
}
