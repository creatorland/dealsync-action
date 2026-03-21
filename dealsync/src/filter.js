import * as core from '@actions/core'
import { tryDecrypt } from '../../shared/crypto.js'
import blockedDomains from '../config/blocked-domains.json'
import blockedPrefixes from '../config/blocked-prefixes.json'
import automatedSubjects from '../config/automated-subjects.json'
import freeEmailPatterns from '../config/free-email-patterns.json'
import nonPersonalizedNames from '../config/non-personalized-names.json'
import marketingHeaders from '../config/marketing-headers.json'

function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`Invalid ID format: ${id}`)
  }
  return id
}

function getHeader(email, name) {
  const header = email.topLevelHeaders?.find((h) => h.name.toLowerCase() === name.toLowerCase())
  return header?.value || ''
}

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

export function checkAuthenticationResults(email) {
  const authResults = getHeader(email, 'authentication-results')
  if (!authResults) return false
  const hasDkim = authResults.includes('dkim=pass')
  const hasSpf = authResults.includes('spf=pass')
  const hasDmarc = authResults.includes('dmarc=pass')
  return !hasDkim && !hasSpf && !hasDmarc
}

export function checkSender(email) {
  const fromValue = getHeader(email, 'from')
  if (!fromValue) return false
  const emailAddr = extractEmailAddress(fromValue)
  for (const prefix of blockedPrefixes) {
    if (emailAddr.startsWith(prefix)) return true
  }
  const atIndex = emailAddr.indexOf('@')
  if (atIndex === -1) return false
  const domain = emailAddr.slice(atIndex + 1)
  for (const blockedDomain of blockedDomains) {
    if (domain.includes(blockedDomain)) return true
  }
  return false
}

export function checkBulkHeaders(email) {
  const headers = email.topLevelHeaders || []
  for (const bulkHeader of marketingHeaders.headers) {
    const found = headers.find((h) => h.name.toLowerCase() === bulkHeader.toLowerCase())
    if (found) return true
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
  return false
}

export function checkSubject(email) {
  const subject = getHeader(email, 'subject').toLowerCase()
  if (!subject) return false
  for (const term of automatedSubjects) {
    if (subject.includes(term.toLowerCase())) return true
  }
  return false
}

export function checkSenderName(email) {
  const fromValue = getHeader(email, 'from')
  if (!fromValue) return false
  const displayName = extractDisplayName(fromValue)
  if (!displayName) return false
  return nonPersonalizedNames.some((name) => displayName === name.toLowerCase())
}

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

export async function runFilter() {
  const encrypt = core.getInput('encrypt') !== 'false'
  const encryptionKey = encrypt ? core.getInput('encryption-key') : null

  let emailsJson = core.getInput('emails')
  if (!emailsJson || emailsJson === '[]') {
    return { filtered_ids: '', rejected_ids: '' }
  }

  // If input is a JSON object with .emails key (from fetch-content result), extract it
  try {
    const parsed = JSON.parse(emailsJson)
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed) && parsed.emails) {
      emailsJson = typeof parsed.emails === 'string' ? parsed.emails : JSON.stringify(parsed.emails)
    }
  } catch {
    // Not JSON wrapper, treat as raw emails input
  }

  // Decrypt emails input if encrypted
  if (encryptionKey) {
    const decrypted = tryDecrypt(emailsJson, encryptionKey)
    if (decrypted !== null) emailsJson = decrypted
  }

  const emails = JSON.parse(emailsJson)
  const filteredIds = []
  const rejectedIds = []

  for (const email of emails) {
    const rejected =
      checkAuthenticationResults(email) ||
      checkSender(email) ||
      checkBulkHeaders(email) ||
      checkSubject(email) ||
      checkSenderName(email) ||
      checkFreeEmail(email)

    if (rejected) {
      rejectedIds.push(email.id)
    } else {
      filteredIds.push(email.id)
    }
  }

  return {
    filtered_ids: filteredIds.map((id) => `'${sanitizeId(id)}'`).join(','),
    rejected_ids: rejectedIds.map((id) => `'${sanitizeId(id)}'`).join(','),
  }
}
