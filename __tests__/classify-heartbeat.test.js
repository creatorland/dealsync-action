import { describe, it, expect } from '@jest/globals'

import { buildHeartbeatRow } from '../src/lib/classify-heartbeat.js'

// ===========================================================================
// Unit tests for the pure classify-heartbeat row builder (Story 8.1, Task 5.3).
//
// The heartbeat exists because a W3 run reports `workflowRunCompleted` even when
// the non-fatal Step 6.5 Supabase write wrote nothing (the run-state-overstates-
// health trap). buildHeartbeatRow turns the run's aggregated per-table write
// counts + failure tally into the row the dealsync-v2 supabase-scrape-exporter
// reads: status + rows_written_total + rows_by_table.
//
// Status contract (matches public.classify_heartbeat CHECK in 0014):
//   success  — no write failures (rows may be 0: a run with nothing to classify
//              is a clean run; the scraper's true-success index filters rows>0)
//   partial  — some writes failed but some rows landed
//   failure  — writes were attempted and none landed
// ===========================================================================

describe('buildHeartbeatRow', () => {
  it('sums per-table counts and reports success when nothing failed', () => {
    const row = buildHeartbeatRow({
      runId: 'gha-run-1',
      node: 'testnet',
      rowsByTable: { deals: 1, contacts: 1, email_thread_evaluations: 2, ai_evaluation_audits: 1 },
      ingestWriteTarget: 'supabase',
      failureCount: 0,
    })
    expect(row).toEqual({
      run_id: 'gha-run-1',
      node: 'testnet',
      status: 'success',
      rows_written_total: 5,
      rows_by_table: {
        deals: 1,
        contacts: 1,
        email_thread_evaluations: 2,
        ai_evaluation_audits: 1,
      },
      ingest_write_target: 'supabase',
    })
  })

  it('is success with zero rows when nothing failed (a clean empty run)', () => {
    const row = buildHeartbeatRow({
      runId: 'gha-run-2',
      node: 'betanet',
      rowsByTable: {},
      ingestWriteTarget: 'both',
      failureCount: 0,
    })
    expect(row.status).toBe('success')
    expect(row.rows_written_total).toBe(0)
    expect(row.rows_by_table).toEqual({})
  })

  it('is partial when a write failed but rows still landed', () => {
    const row = buildHeartbeatRow({
      runId: 'r',
      node: 'betanet',
      rowsByTable: { deals: 1, contacts: 1, ai_evaluation_audits: 1 },
      ingestWriteTarget: 'both',
      failureCount: 1,
    })
    expect(row.status).toBe('partial')
    expect(row.rows_written_total).toBe(3)
  })

  it('is failure when writes were attempted and none landed', () => {
    const row = buildHeartbeatRow({
      runId: 'r',
      node: 'testnet',
      rowsByTable: { deals: 0 },
      ingestWriteTarget: 'supabase',
      failureCount: 2,
    })
    expect(row.status).toBe('failure')
    expect(row.rows_written_total).toBe(0)
  })

  it('drops zero/negative/non-finite counts from rows_by_table and floors the total', () => {
    const row = buildHeartbeatRow({
      runId: 'r',
      node: 'testnet',
      rowsByTable: {
        deals: 2,
        contacts: 0,
        email_thread_evaluations: -3,
        ai_evaluation_audits: NaN,
      },
      ingestWriteTarget: 'supabase',
      failureCount: 0,
    })
    expect(row.rows_by_table).toEqual({ deals: 2 })
    expect(row.rows_written_total).toBe(2)
  })

  it('defaults missing optional inputs (rowsByTable, ingestWriteTarget, failureCount)', () => {
    const row = buildHeartbeatRow({ runId: 'r', node: 'testnet' })
    expect(row).toEqual({
      run_id: 'r',
      node: 'testnet',
      status: 'success',
      rows_written_total: 0,
      rows_by_table: {},
      ingest_write_target: null,
    })
  })
})
