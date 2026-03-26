import { getHeader } from './email-utils.js'
import blockedDomains from '../../config/blocked-domains.json'
import blockedPrefixes from '../../config/blocked-prefixes.json'
import automatedSubjects from '../../config/automated-subjects.json'
import freeEmailPatterns from '../../config/free-email-patterns.json'
import nonPersonalizedNames from '../../config/non-personalized-names.json'
import marketingHeaders from '../../config/marketing-headers.json'

function extractEmailAddress(from) {
  const match = from.match(/<([^>]+)>/)
  return (match ? match[1] : from).trim().toLowerCase()
}

function extractDisplayName(from) {
  const match = from.match(/^(.+?)\s*</)
  return match
    ? match[1]
        .trim()
        .replace(/^["']|["']$/g, '')
        .toLowerCase()
    : ''
}

export function isRejected(email) {
  // Rule 1: Authentication results
  const authResults = getHeader(email, 'authentication-results')
  if (authResults) {
    const hasDkim = authResults.includes('dkim=pass')
    const hasSpf = authResults.includes('spf=pass')
    const hasDmarc = authResults.includes('dmarc=pass')
    if (!hasDkim && !hasSpf && !hasDmarc) return true
  }

  // Rule 2: Blocked sender
  const fromValue = getHeader(email, 'from')
  if (fromValue) {
    const emailAddr = extractEmailAddress(fromValue)
    for (const prefix of blockedPrefixes) {
      if (emailAddr.startsWith(prefix)) return true
    }
    const atIndex = emailAddr.indexOf('@')
    if (atIndex !== -1) {
      const domain = emailAddr.slice(atIndex + 1)
      for (const blockedDomain of blockedDomains) {
        if (domain.includes(blockedDomain)) return true
      }
    }
  }

  // Rule 3: Bulk headers
  const headers = email.topLevelHeaders || []
  for (const bulkHeader of marketingHeaders.headers) {
    if (headers.find((h) => h.name.toLowerCase() === bulkHeader.toLowerCase())) return true
  }
  for (const header of headers) {
    const value = (header.value || '').toLowerCase()
    for (const tool of marketingHeaders.tools) {
      if (value.includes(tool)) return true
    }
  }
  const precedence = getHeader(email, 'precedence').toLowerCase()
  for (const val of marketingHeaders.values) {
    if (precedence.includes(val)) return true
  }

  // Rule 4: Automated subject
  const subject = getHeader(email, 'subject').toLowerCase()
  if (subject) {
    for (const term of automatedSubjects) {
      if (subject.includes(term.toLowerCase())) return true
    }
  }

  // Rule 5: Non-personalized sender name
  if (fromValue) {
    const displayName = extractDisplayName(fromValue)
    if (displayName && nonPersonalizedNames.some((name) => displayName === name.toLowerCase())) {
      return true
    }
  }

  // Rule 6: Free email with non-personal prefix
  if (fromValue) {
    const emailAddr = extractEmailAddress(fromValue)
    for (const pattern of freeEmailPatterns) {
      if (new RegExp(pattern, 'i').test(emailAddr)) return true
    }
  }

  return false
}
