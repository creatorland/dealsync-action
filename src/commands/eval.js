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
  const batchSize = parseInt(core.getInput('batch-size') || '0', 10) // 0 = all at once

  if (!hyperbolicKey) throw new Error('hyperbolic-key is required for eval')

  // Filter out entries with empty bodies — AI can't classify without content
  const usableEntries = groundTruth.filter((gt) =>
    gt.emails.some((e) => e.body && e.body.trim() !== ''),
  )
  console.log(`[eval] model=${model} runs=${numRuns} threads=${usableEntries.length} batch_size=${batchSize || 'all'} (${groundTruth.length - usableEntries.length} skipped: empty body)`)

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

    const runThreads = []
    const runHealth = { clean: 0, repaired: 0, corrective_retry: 0, failed: 0, total_batches: batches.length }

    for (let b = 0; b < batches.length; b++) {
      const batch = batches[b]
      const allEmails = batch.flatMap((gt) => gt.emails)
      const { systemPrompt, userPrompt } = buildPrompt(allEmails)

      const messages = [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ]

      if (batches.length > 1) {
        console.log(`[eval] run ${run} batch ${b + 1}/${batches.length} (${batch.length} threads)`)
      }

      // Layer 0: Primary model call
      let rawContent = null
      let usage = {}
      try {
        const result = await callModel(model, messages, { temperature, ...aiOpts })
        rawContent = result.content
        usage = result.usage || {}
      } catch (apiErr) {
        console.log(`[eval] run ${run} batch ${b + 1} API failed: ${apiErr.message}`)
      }

      if (rawContent) {
        // Layer 1: Local JSON repair
        try {
          const parsed = parseAndValidate(rawContent)
          runThreads.push(...parsed)
          runHealth.clean++
        } catch (parseErr) {
          console.log(`[eval] run ${run} batch ${b + 1}: parse failed (${parseErr.message}), attempting repair`)

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
            const parsed = parseAndValidate(corrected.content)
            runThreads.push(...parsed)
            runHealth.corrective_retry++
          } catch (correctiveErr) {
            console.log(`[eval] run ${run} batch ${b + 1}: corrective retry failed: ${correctiveErr.message}`)
            runHealth.failed++
          }
        }
      } else {
        runHealth.failed++
      }

      totalInputTokens += usage.prompt_tokens || 0
      totalOutputTokens += usage.completion_tokens || 0
    }

    console.log(`[eval] run ${run}: ${runThreads.length} threads (clean=${runHealth.clean} retry=${runHealth.corrective_retry} failed=${runHealth.failed})`)

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
    batch_size: batchSize || usableEntries.length,
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
