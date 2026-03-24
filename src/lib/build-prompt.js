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
    const isIncremental =
      threadEmails[0].previousAiSummary != null && threadEmails[0].previousAiSummary !== ''

    if (isIncremental) {
      threadData += `--- Thread: ${threadId} (INCREMENTAL) ---\n`
      threadData += `Previous AI Summary: ${threadEmails[0].previousAiSummary}\n\n`
    } else {
      threadData += `--- Thread: ${threadId} (FULL_THREAD) ---\n`
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
