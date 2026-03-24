import { getHeader } from './email-utils.js'
import systemTemplate from '../../prompts/system-template.md'
import classificationInstructions from '../../prompts/classification-instructions.md'

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
  let threadIndex = 0

  for (const [threadId, threadEmails] of Object.entries(threads)) {
    threadIndex++
    let section = `--- THREAD ${threadIndex} ---\n`
    section += `Thread ID: ${threadId}\n`
    section += `Message Count: ${threadEmails.length}\n`

    const previousSummary = threadEmails[0].previousAiSummary
    section += `Previous AI Summary: ${previousSummary || 'None'}\n\n`

    threadEmails.forEach((email, i) => {
      const from = getHeader(email, 'from')
      const date = getHeader(email, 'date')
      const subject = getHeader(email, 'subject')
      section += `[Message ${i + 1}]\n`
      section += `From: ${from}\n`
      section += `Date: ${date}\n`
      section += `Subject: ${subject}\n\n`
      const body = email.body || email.replyBody || '[no body]'
      section += `${body}\n\n`
    })

    parts.push(section)
  }

  return parts.join('')
}

export function buildPrompt(emails) {
  const threadData = buildThreadData(emails)

  // System prompt is the short persona
  const systemPrompt = systemTemplate.trim()

  // User prompt is the full classifier instructions with thread data injected
  const userPrompt = classificationInstructions
    .replace('{{CLASSIFICATION_INSTRUCTIONS}}', '')
    .replace('{{THREAD_DATA}}', threadData)
    .trim()

  return { systemPrompt, userPrompt }
}
