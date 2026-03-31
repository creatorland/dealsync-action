import * as core from '@actions/core'
import { buildPrompt, callModel, parseAndValidate } from '../lib/ai.js'
import { isRejected } from '../lib/emails.js'
import {
  computeDetectionMetrics,
  computeCategoryAccuracy,
  computeScoreInRange,
  computeScamDetection,
  computePerThread,
  computeJsonHealth,
} from '../lib/metrics.js'
import groundTruth from '../../eval/ground-truth.json'

const PROMPT_BASE_URL = 'https://raw.githubusercontent.com/creatorland/dealsync-action'

async function fetchPromptsByHash(hash) {
  const [systemResp, userResp] = await Promise.all([
    fetch(`${PROMPT_BASE_URL}/${hash}/prompts/system.md`),
    fetch(`${PROMPT_BASE_URL}/${hash}/prompts/user.md`),
  ])
  if (!systemResp.ok)
    throw new Error(`Failed to fetch system.md at ${hash}: HTTP ${systemResp.status}`)
  if (!userResp.ok) throw new Error(`Failed to fetch user.md at ${hash}: HTTP ${userResp.status}`)
  return {
    systemOverride: await systemResp.text(),
    userOverride: await userResp.text(),
  }
}

export async function runEval() {
  const hyperbolicKey = core.getInput('hyperbolic-key')
  const model = core.getInput('primary-model') || 'Qwen/Qwen3-235B-A22B-Instruct-2507'
  const aiApiUrl = core.getInput('ai-api-url') || 'https://api.hyperbolic.xyz/v1/chat/completions'
  const numRuns = parseInt(core.getInput('runs') || '10', 10)
  const temperature = parseFloat(core.getInput('temperature') || '0')
  const batchSize = parseInt(core.getInput('batch-size') || '1', 10)
  const concurrency = parseInt(core.getInput('concurrency') || '10', 10)
  const promptHash = core.getInput('prompt-hash') || ''

  if (!hyperbolicKey) throw new Error('hyperbolic-key is required for eval')

  // Fetch prompts from a specific commit hash, or use bundled defaults
  let promptOverrides = {}
  if (promptHash) {
    console.log(`[eval] fetching prompts from commit ${promptHash}`)
    promptOverrides = await fetchPromptsByHash(promptHash)
  }

  // Filter out entries with empty bodies — AI can't classify without content
  const withBody = groundTruth.filter((gt) => gt.emails.some((e) => e.body && e.body.trim() !== ''))

  // Apply static filter rules (same as production pipeline)
  const filtered = []
  const passedFilter = []
  for (const gt of withBody) {
    if (isRejected(gt.emails[0])) {
      filtered.push(gt)
    } else {
      passedFilter.push(gt)
    }
  }

  const usableEntries = passedFilter
  console.log(
    `[eval] model=${model} runs=${numRuns} threads=${usableEntries.length} batch_size=${batchSize} concurrency=${concurrency} prompt=${promptHash || 'bundled'}`,
  )
  console.log(
    `[eval] ground truth: ${groundTruth.length} total, ${groundTruth.length - withBody.length} empty body, ${filtered.length} static-filtered, ${usableEntries.length} to AI`,
  )

  const aiOpts = { apiUrl: aiApiUrl, apiKey: hyperbolicKey }
  const allRuns = []
  const jsonHealthPerRun = []
  let totalInputTokens = 0
  let totalOutputTokens = 0

  for (let run = 1; run <= numRuns; run++) {
    console.log(`[eval] --- run ${run}/${numRuns} ---`)

    // Split ground truth into batches (0 = all at once)
    const batches = []
    if (batchSize > 0) {
      for (let i = 0; i < usableEntries.length; i += batchSize) {
        batches.push(usableEntries.slice(i, i + batchSize))
      }
    } else {
      batches.push(usableEntries)
    }

    const runHealth = {
      clean: 0,
      repaired: 0,
      corrective_retry: 0,
      failed: 0,
      total_batches: batches.length,
    }

    // Process batches with concurrency pool
    async function processBatch(batch, batchIdx) {
      const allEmails = batch.flatMap((gt) => gt.emails)
      const { systemPrompt, userPrompt } = buildPrompt(allEmails, promptOverrides)
      const messages = [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ]

      let rawContent = null
      let usage = {}
      try {
        const result = await callModel(model, messages, { temperature, ...aiOpts })
        rawContent = result.content
        usage = result.usage || {}
      } catch (apiErr) {
        console.log(`[eval] run ${run} batch ${batchIdx + 1} API failed: ${apiErr.message}`)
        return { threads: [], health: 'failed', usage }
      }

      // Layer 1: Local JSON repair
      try {
        const parsed = parseAndValidate(rawContent)
        return { threads: parsed, health: 'clean', usage }
      } catch (parseErr) {
        // Layer 2: Corrective retry
        try {
          const correctiveMessages = [
            ...messages,
            { role: 'assistant', content: rawContent },
            {
              role: 'user',
              content: `Your previous response could not be parsed as valid JSON.\n\nParse error:\n${parseErr.message}\n\nPlease return the corrected classification as a valid JSON array. Fix only the JSON formatting. Return ONLY the JSON array.`,
            },
          ]
          const corrected = await callModel(model, correctiveMessages, {
            temperature: 0,
            ...aiOpts,
          })
          const parsed = parseAndValidate(corrected.content)
          return { threads: parsed, health: 'corrective_retry', usage }
        } catch (correctiveErr) {
          console.log(
            `[eval] run ${run} batch ${batchIdx + 1}: corrective retry failed: ${correctiveErr.message}`,
          )
          return { threads: [], health: 'failed', usage }
        }
      }
    }

    // Run batches in parallel with concurrency limit
    const runThreads = []
    const pending = []
    let completed = 0

    for (let b = 0; b < batches.length; b++) {
      const p = processBatch(batches[b], b).then((result) => {
        completed++
        runThreads.push(...result.threads)
        runHealth[result.health]++
        totalInputTokens += result.usage.prompt_tokens || 0
        totalOutputTokens += result.usage.completion_tokens || 0
        if (completed % 10 === 0 || completed === batches.length) {
          console.log(`[eval] run ${run}: ${completed}/${batches.length} batches done`)
        }
      })
      pending.push(p)

      // Enforce concurrency limit
      if (pending.length >= concurrency) {
        await Promise.race(pending)
        // Remove resolved promises
        for (let i = pending.length - 1; i >= 0; i--) {
          const settled = await Promise.race([pending[i].then(() => true), Promise.resolve(false)])
          if (settled) pending.splice(i, 1)
        }
      }
    }
    await Promise.all(pending)

    console.log(
      `[eval] run ${run}: ${runThreads.length} threads (clean=${runHealth.clean} retry=${runHealth.corrective_retry} failed=${runHealth.failed})`,
    )

    if (runThreads.length > 0) {
      allRuns.push(runThreads)
    }
    jsonHealthPerRun.push({
      clean: runHealth.failed === 0 && runHealth.corrective_retry === 0,
      repaired: false,
      corrective_retry: runHealth.corrective_retry > 0,
      failed: runHealth.failed > 0,
      batch_count: runHealth.total_batches,
      batch_clean: runHealth.clean,
      batch_retry: runHealth.corrective_retry,
      batch_failed: runHealth.failed,
    })
  }

  if (allRuns.length === 0) {
    throw new Error(`All ${numRuns} runs failed — no valid results to compute metrics`)
  }

  // Compute metrics
  const detection = computeDetectionMetrics(allRuns, usableEntries)
  const categorization = computeCategoryAccuracy(allRuns, usableEntries)
  const urgencyScoring = computeScoreInRange(allRuns, usableEntries)
  const scamDetection = computeScamDetection(allRuns, usableEntries)
  const perThread = computePerThread(allRuns, usableEntries)
  const jsonHealth = computeJsonHealth(jsonHealthPerRun)

  const result = {
    model,
    temperature,
    batch_size: batchSize,
    prompt_hash: promptHash || 'bundled',
    runs: numRuns,
    successful_runs: allRuns.length,
    ground_truth_count: usableEntries.length,
    detection,
    categorization,
    urgency_scoring: urgencyScoring,
    scam_detection: scamDetection,
    json_health: jsonHealth,
    cost: {
      total_input_tokens: totalInputTokens,
      total_output_tokens: totalOutputTokens,
      avg_cost_per_thread:
        usableEntries.length > 0
          ? +(
              (totalInputTokens * 0.000001 + totalOutputTokens * 0.000002) /
              usableEntries.length
            ).toFixed(6)
          : 0,
    },
    per_thread: perThread,
  }

  console.log(
    `[eval] complete: recall=${detection.recall.mean} precision=${detection.precision.mean} f2=${detection.f2.mean} json_failures=${jsonHealth.total_failures}`,
  )
  return result
}
