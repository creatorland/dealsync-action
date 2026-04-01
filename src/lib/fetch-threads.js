/**
 * Thread-aware retry layer for email fetching.
 *
 * Sits between pipelines and fetchEmails(), adding retry logic that
 * ensures entire threads are fetched before returning them as complete.
 */

import { fetchEmails } from './emails.js'
import { backoffMs, sleep } from './retry.js'

const DEFAULT_MAX_FETCH_ATTEMPTS = 10
const DEFAULT_DEADLINE_MS = 200000

/**
 * Fetch emails with thread-aware retry logic.
 *
 * Calls fetchEmails() in rounds, retrying only failed messageIds.
 * A thread is "complete" only when ALL its messageIds have been fetched.
 * A thread is "unfetchable" when any of its messageIds exceeds maxFetchAttempts
 * or the wall-clock deadline is reached.
 *
 * @param {string[]} messageIds — all message IDs to fetch
 * @param {Map} metaByMessageId — Map with THREAD_ID per messageId
 * @param {object} opts — fetchEmails opts plus deadlineMs and maxFetchAttempts
 * @returns {{ completedThreads: object[], unfetchableThreadIds: string[] }}
 */
export async function fetchThreadEmails(messageIds, metaByMessageId, opts) {
  const {
    deadlineMs = DEFAULT_DEADLINE_MS,
    maxFetchAttempts = DEFAULT_MAX_FETCH_ATTEMPTS,
    ...fetchOpts
  } = opts

  if (!messageIds || messageIds.length === 0) {
    return { completedThreads: [], unfetchableThreadIds: [] }
  }

  // Step 1: Build thread map — { threadId: Set<messageId> }
  const threadMap = new Map()
  for (const msgId of messageIds) {
    const meta = metaByMessageId.get(msgId)
    if (!meta) continue
    const threadId = meta.THREAD_ID
    if (!threadMap.has(threadId)) {
      threadMap.set(threadId, new Set())
    }
    threadMap.get(threadId).add(msgId)
  }

  // Step 2: Initialize state
  const fetchedMap = new Map() // messageId → EmailContent
  const attemptCounts = new Map() // messageId → number
  const completedThreads = []
  const unfetchableThreadIds = []
  const deadline = Date.now() + deadlineMs

  let pendingMessageIds = [...messageIds]
  let round = 0

  // Step 3: Fetch loop
  while (pendingMessageIds.length > 0) {
    round++

    // 3a. If round > 1: check deadline, apply backoff
    if (round > 1) {
      if (Date.now() >= deadline) {
        console.log(
          `[fetchThreadEmails] deadline reached after round ${round - 1}` +
            ` — ${pendingMessageIds.length} messageIds still pending`,
        )
        break
      }
      const wait = backoffMs(round - 2, { base: 1000, max: 60000 })
      console.log(
        `[fetchThreadEmails] round ${round}: retrying ${pendingMessageIds.length} messageIds, backoff ${wait}ms`,
      )
      await sleep(wait)
    } else {
      const threadCount = threadMap.size
      console.log(
        `[fetchThreadEmails] round 1: fetching ${pendingMessageIds.length} messageIds across ${threadCount} threads`,
      )
    }

    // 3b. Call fetchEmails
    const { fetched, failed } = await fetchEmails(pendingMessageIds, metaByMessageId, fetchOpts)

    // 3c. Store fetched emails
    for (const email of fetched) {
      fetchedMap.set(email.messageId, email)
    }

    // 3d. Increment attemptCounts for ALL messageIds processed this round
    for (const msgId of pendingMessageIds) {
      attemptCounts.set(msgId, (attemptCounts.get(msgId) || 0) + 1)
    }

    // 3e. Check thread completeness
    for (const [threadId, msgIdSet] of [...threadMap.entries()]) {
      const allFetched = [...msgIdSet].every((id) => fetchedMap.has(id))
      if (allFetched) {
        // Move emails to completedThreads
        for (const id of msgIdSet) {
          completedThreads.push(fetchedMap.get(id))
          fetchedMap.delete(id)
        }
        threadMap.delete(threadId)
      }
    }

    // 3f. Check for permanently stuck threads
    for (const [threadId, msgIdSet] of [...threadMap.entries()]) {
      const hasStuckMsg = [...msgIdSet].some(
        (id) => !fetchedMap.has(id) && (attemptCounts.get(id) || 0) >= maxFetchAttempts,
      )
      if (hasStuckMsg) {
        unfetchableThreadIds.push(threadId)
        // Clean up
        for (const id of msgIdSet) {
          fetchedMap.delete(id)
        }
        threadMap.delete(threadId)
      }
    }

    // 3g. Build new pendingMessageIds
    pendingMessageIds = []
    for (const [, msgIdSet] of threadMap) {
      for (const id of msgIdSet) {
        if (!fetchedMap.has(id) && (attemptCounts.get(id) || 0) < maxFetchAttempts) {
          pendingMessageIds.push(id)
        }
      }
    }

    // 3h. If no pending → break
    if (pendingMessageIds.length === 0) break
  }

  // Step 4: Remaining threads in threadMap after loop = unfetchable (deadline hit)
  for (const threadId of threadMap.keys()) {
    unfetchableThreadIds.push(threadId)
  }

  console.log(
    `[fetchThreadEmails] done: ${completedThreads.length} emails from completed threads, ${unfetchableThreadIds.length} unfetchable threads`,
  )

  return { completedThreads, unfetchableThreadIds }
}
