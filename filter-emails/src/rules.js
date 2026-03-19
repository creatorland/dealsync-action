import blockedDomains from '../config/blocked-domains.json'
import blockedPrefixes from '../config/blocked-prefixes.json'
import automatedSubjects from '../config/automated-subjects.json'
import freeEmailPatterns from '../config/free-email-patterns.json'
import nonPersonalizedNames from '../config/non-personalized-names.json'
import marketingHeaders from '../config/marketing-headers.json'

/**
 * Get a header value from an email by name (case-insensitive).
 */
function getHeader(email, name) {
  const header = email.topLevelHeaders?.find(
    (h) => h.name.toLowerCase() === name.toLowerCase(),
  )
  return header?.value || ''
}

/**
 * Extract email address from a From header value.
 * Handles both "Name <email>" and bare "email" formats.
 */
function extractEmailAddress(from) {
  const match = from.match(/<([^>]+)>/)
  return (match ? match[1] : from).trim().toLowerCase()
}

/**
 * Extract display name from a From header value.
 * Returns empty string if no display name found.
 */
function extractDisplayName(from) {
  const match = from.match(/^(.+?)\s*</)
  return match
    ? match[1]
        .trim()
        .replace(/^["']|["']$/g, '')
        .toLowerCase()
    : ''
}

/**
 * Rule 1: Check authentication results (DKIM/SPF/DMARC).
 * Returns true (reject) if auth header exists but none of dkim/spf/dmarc pass.
 * Returns false (pass) if auth header is not present.
 */
export function checkAuthenticationResults(email) {
  const authResults = getHeader(email, 'authentication-results')
  if (!authResults) return false

  const hasDkim = authResults.includes('dkim=pass')
  const hasSpf = authResults.includes('spf=pass')
  const hasDmarc = authResults.includes('dmarc=pass')

  return !hasDkim && !hasSpf && !hasDmarc
}

/**
 * Rule 2: Check sender against blocked prefixes and domain suffixes.
 * Returns true (reject) if sender matches a blocked prefix or domain.
 */
export function checkSender(email) {
  const fromValue = getHeader(email, 'from')
  if (!fromValue) return false

  const emailAddr = extractEmailAddress(fromValue)

  // Check blocked prefixes
  for (const prefix of blockedPrefixes) {
    if (emailAddr.startsWith(prefix)) return true
  }

  // Extract domain and check blocked domain suffixes
  const atIndex = emailAddr.indexOf('@')
  if (atIndex === -1) return false
  const domain = emailAddr.slice(atIndex + 1)

  for (const blockedDomain of blockedDomains) {
    if (domain.includes(blockedDomain)) return true
  }

  return false
}

/**
 * Rule 3: Check for bulk/marketing headers.
 * Returns true (reject) if bulk headers or marketing tool signatures found.
 */
export function checkBulkHeaders(email) {
  const headers = email.topLevelHeaders || []

  // Check for presence of bulk mail headers
  for (const bulkHeader of marketingHeaders.headers) {
    const found = headers.find(
      (h) => h.name.toLowerCase() === bulkHeader.toLowerCase(),
    )
    if (found) return true
  }

  // Check all header values for marketing tool names
  for (const header of headers) {
    const value = (header.value || '').toLowerCase()
    for (const tool of marketingHeaders.tools) {
      if (value.includes(tool)) return true
    }
  }

  // Check precedence header for auto_reply or bulk values
  const precedence = getHeader(email, 'precedence').toLowerCase()
  for (const val of marketingHeaders.values) {
    if (precedence.includes(val)) return true
  }

  return false
}

/**
 * Rule 4: Check subject line for automated content patterns.
 * Returns true (reject) if subject contains an automated subject term.
 */
export function checkSubject(email) {
  const subject = getHeader(email, 'subject').toLowerCase()
  if (!subject) return false

  for (const term of automatedSubjects) {
    if (subject.includes(term.toLowerCase())) return true
  }

  return false
}

/**
 * Rule 5: Check sender display name against non-personalized names.
 * Returns true (reject) if display name matches exactly.
 */
export function checkSenderName(email) {
  const fromValue = getHeader(email, 'from')
  if (!fromValue) return false

  const displayName = extractDisplayName(fromValue)
  if (!displayName) return false

  return nonPersonalizedNames.some((name) => displayName === name.toLowerCase())
}

/**
 * Rule 6: Check if sender matches free email provider patterns.
 * Returns true (reject) if email address matches a free email pattern.
 */
export function checkFreeEmail(email) {
  const fromValue = getHeader(email, 'from')
  if (!fromValue) return false

  const emailAddr = extractEmailAddress(fromValue)

  for (const pattern of freeEmailPatterns) {
    const regex = new RegExp(pattern, 'i')
    if (regex.test(emailAddr)) return true
  }

  return false
}
