import { getHeader } from './email-utils.js'
import { sanitizeEmailBody } from './email-sanitizer.js'
import systemTemplate from '../../prompts/system.md'
import classificationInstructions from '../../prompts/user.md'

function groupByThread(emails) {
  const threads = {}
  for (const email of emails) {
    const threadId = email.threadId || email.id
    if (!threads[threadId]) threads[threadId] = []
    threads[threadId].push(email)
  }
  return threads
}

function buildThreadData(emails) {
  const threads = groupByThread(emails)
  const parts = []
  const threadOrder = []
  let threadIndex = 0

  for (const [threadId, threadEmails] of Object.entries(threads)) {
    threadIndex++
    threadOrder.push(threadId)
    let section = `THREAD_ID_INDEX: ${threadIndex}\n`
    section += `MODE: FULL_THREAD\n`
    section += `Message Count: ${threadEmails.length}\n`

    const previousSummary = threadEmails[0].previousAiSummary
    section += `PREVIOUS_AI_SUMMARY: ${previousSummary || 'None'}\n\n`

    threadEmails.forEach((email, i) => {
      const from = getHeader(email, 'from')
      const date = getHeader(email, 'date')
      const subject = getHeader(email, 'subject')
      section += `[Message ${i + 1}]\n`
      section += `From: ${from}\n`
      section += `Date: ${date}\n`
      section += `Subject: ${subject}\n\n`
      const rawBody = email.body || email.replyBody || ''
      const body = sanitizeEmailBody(rawBody) || '[no body]'
      section += `${body}\n\n`
    })

    section += '===\n'
    parts.push(section)
  }

  return { text: parts.join('\n'), threadOrder }
}

export function buildPrompt(emails, { systemOverride, userOverride } = {}) {
  const { text: threadData, threadOrder } = buildThreadData(emails)

  const systemPrompt = (systemOverride || systemTemplate).trim()

  const userPrompt = (userOverride || classificationInstructions)
    .replace('{{THREAD_DATA}}', threadData)
    .trim()

  return { systemPrompt, userPrompt, threadOrder }
}
