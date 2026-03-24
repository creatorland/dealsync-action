import * as core from '@actions/core'
import { buildPrompt } from '../lib/build-prompt.js'
import { callModel, parseAndValidate } from '../lib/ai-client.js'
import {
  computeDetectionMetrics,
  computeCategoryAccuracy,
  computeScoreInRange,
  computeScamDetection,
  computePerThread,
  computeJsonHealth,
} from '../lib/metrics.js'
import groundTruth from '../../eval/ground-truth.json'

export async function runEval() {
  const hyperbolicKey = core.getInput('hyperbolic-key')
  const model = core.getInput('primary-model') || 'Qwen/Qwen3-235B-A22B-Instruct-2507'
  const aiApiUrl = core.getInput('ai-api-url') || 'https://api.hyperbolic.xyz/v1/chat/completions'
  const numRuns = parseInt(core.getInput('runs') || '10', 10)
  const temperature = parseFloat(core.getInput('temperature') || '0')

  if (!hyperbolicKey) throw new Error('hyperbolic-key is required for eval')

  // Filter out entries with empty bodies — AI can't classify without content
  const usableEntries = groundTruth.filter((gt) =>
    gt.emails.some((e) => e.body && e.body.trim() !== ''),
  )
  console.log(`[eval] model=${model} runs=${numRuns} threads=${usableEntries.length} (${groundTruth.length - usableEntries.length} skipped: empty body)`)

  const aiOpts = { apiUrl: aiApiUrl, apiKey: hyperbolicKey }
  const allRuns = []
  const jsonHealthPerRun = []
  let totalInputTokens = 0
  let totalOutputTokens = 0

  for (let run = 1; run <= numRuns; run++) {
    console.log(`[eval] --- run ${run}/${numRuns} ---`)

    // Build prompt from ground truth emails (same path as production)
    const allEmails = usableEntries.flatMap((gt) => gt.emails)
    const { systemPrompt, userPrompt } = buildPrompt(allEmails)

    const messages = [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userPrompt },
    ]

    const health = { clean: false, repaired: false, corrective_retry: false, failed: false }
    let threads = null

    // Layer 0: Primary model call
    let rawContent = null
    let usage = {}
    try {
      const result = await callModel(model, messages, { temperature, ...aiOpts })
      rawContent = result.content
      usage = result.usage || {}
    } catch (apiErr) {
      console.log(`[eval] run ${run} API failed: ${apiErr.message}`)
    }

    if (rawContent) {
      // Layer 1: Local JSON repair
      try {
        threads = parseAndValidate(rawContent)
        health.clean = true
        console.log(`[eval] run ${run}: clean parse, ${threads.length} threads`)
      } catch (parseErr) {
        console.log(`[eval] run ${run}: parse failed (${parseErr.message}), attempting repair`)
        health.repaired = true

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
          const corrected = await callModel(model, correctiveMessages, { temperature: 0, ...aiOpts })
          threads = parseAndValidate(corrected.content)
          health.corrective_retry = true
          health.repaired = false
          console.log(`[eval] run ${run}: corrective retry succeeded`)
        } catch (correctiveErr) {
          console.log(`[eval] run ${run}: corrective retry failed: ${correctiveErr.message}`)
          health.failed = true
          health.repaired = false
        }
      }
    } else {
      health.failed = true
    }

    totalInputTokens += usage.prompt_tokens || 0
    totalOutputTokens += usage.completion_tokens || 0

    if (threads) {
      allRuns.push(threads)
    }
    jsonHealthPerRun.push(health)
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
      avg_cost_per_thread: usableEntries.length > 0
        ? +((totalInputTokens * 0.000001 + totalOutputTokens * 0.000002) / usableEntries.length).toFixed(6)
        : 0,
    },
    per_thread: perThread,
  }

  console.log(`[eval] complete: recall=${detection.recall.mean} precision=${detection.precision.mean} f2=${detection.f2.mean} json_failures=${jsonHealth.total_failures}`)
  return result
}
