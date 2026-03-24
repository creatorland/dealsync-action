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
  let threadData = ''

  for (const [threadId, threadEmails] of Object.entries(threads)) {
    threadData += `--- Thread: ${threadId} ---\n`
    const previousSummary = threadEmails[0].previousAiSummary
    if (previousSummary != null && previousSummary !== '') {
      threadData += `Previous AI Summary: ${previousSummary}\n\n`
    }

    threadEmails.forEach((email, i) => {
      const from = getHeader(email, 'from')
      const subject = getHeader(email, 'subject')
      const date = getHeader(email, 'date')
      threadData += `Email ${i + 1}: From: ${from} | Subject: ${subject} | Date: ${date}\n`
      const body = email.body || email.replyBody || '[no body]'
      threadData += `Body: ${body}\n\n`
    })
  }

  return threadData.trim()
}

export function buildPrompt(emails) {
  const threadData = buildThreadData(emails)

  const systemPrompt = systemTemplate
    .replace('{{CLASSIFICATION_INSTRUCTIONS}}', classificationInstructions)
    .replace('{{THREAD_DATA}}', '')
    .trim()

  return { systemPrompt, userPrompt: threadData }
}
