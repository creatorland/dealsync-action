// Import the .md file as a string at build time (via Rollup string plugin)
import classificationInstructions from '../prompts/classification-instructions.md'

const STRUCTURAL_TEMPLATE = `You are a deal classification engine. You MUST respond with valid JSON matching this exact schema:

{
  "threads": [
    {
      "thread_id": "<thread_id>",
      "is_deal": boolean,
      "ai_score": number (1-10),
      "category": "new" | "in_progress" | "completed" | "likely_scam" | "low_confidence",
      "deal_name": string | null,
      "deal_type": string | null,
      "deal_value": string | null,
      "currency": string | null,
      "main_contact": { "name": string, "email": string, "company": string | null, "title": string | null } | null,
      "ai_summary": string,
      "language": string (ISO 639-1)
    }
  ]
}

Rules:
- Respond ONLY with the JSON object. No markdown, no explanation.
- One entry per thread_id in the input.
- If is_deal is false, set deal_name/deal_type/deal_value/main_contact to null.
- ai_summary must be a concise summary of the thread's deal relevance (max 500 chars).

{{CLASSIFICATION_INSTRUCTIONS}}`

function getHeader(email, name) {
  const header = email.topLevelHeaders?.find(
    h => h.name.toLowerCase() === name.toLowerCase(),
  )
  return header?.value || ''
}

function groupByThread(emails) {
  const threads = {}
  for (const email of emails) {
    const threadId = email.threadId || email.id
    if (!threads[threadId]) threads[threadId] = []
    threads[threadId].push(email)
  }
  return threads
}

export function buildPrompt(emails) {
  const threads = groupByThread(emails)
  let threadData = ''

  for (const [threadId, threadEmails] of Object.entries(threads)) {
    const isIncremental =
      threadEmails[0].previousAiSummary != null &&
      threadEmails[0].previousAiSummary !== ''

    if (isIncremental) {
      threadData += `--- Thread: ${threadId} (isIncremental: true) ---\n`
      threadData += `Previous AI Summary: ${threadEmails[0].previousAiSummary}\n\n`
    } else {
      threadData += `--- Thread: ${threadId} (isIncremental: false) ---\n`
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

  const systemPrompt = STRUCTURAL_TEMPLATE.replace(
    '{{CLASSIFICATION_INSTRUCTIONS}}',
    classificationInstructions,
  )

  return { systemPrompt, userPrompt: threadData.trim() }
}
