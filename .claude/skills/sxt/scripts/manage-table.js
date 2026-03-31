#!/usr/bin/env node
/**
 * Create, drop, or recreate an SxT table.
 *
 * Usage:
 *   node --experimental-wasm-modules manage-table.js <action> <schema.table> [--public-key=<hex>]
 *
 * Actions: create, drop, recreate, verify
 *
 * Examples:
 *   node --experimental-wasm-modules manage-table.js recreate DEALSYNC_STG_V1.deal_states
 *   node --experimental-wasm-modules manage-table.js create DEALSYNC_STG_V1.deal_states --public-key=3da58ca7...
 *   node --experimental-wasm-modules manage-table.js verify DEALSYNC_STG_V1.deal_states
 *
 * Env vars: see sxt-client.js
 *
 * NOTE: The CREATE TABLE DDL must be provided via stdin or edited in the DDL_REGISTRY below.
 *       Add new table definitions to the registry as needed.
 */

import { authenticate, generateBiscuit, executeSql, derivePublicKey } from './sxt-client.js'

// ── DDL Registry ─────────────────────────────────────────────────────
// Add table definitions here. Key = lowercase "schema.table".
const DDL_REGISTRY = {
  'dealsync_stg_v1.deal_states': {
    columns: `
      id VARCHAR(255) NOT NULL,
      email_metadata_id VARCHAR(255) PRIMARY KEY,
      user_id VARCHAR(255) NOT NULL,
      contact_id VARCHAR(255),
      message_id VARCHAR(255),
      thread_id VARCHAR(255),
      sync_state_id VARCHAR(255),
      status VARCHAR(50) NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0,
      batch_id VARCHAR(128),
      workflow_triggers VARCHAR(65000),
      processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
    indexes: ['status', 'user_id', 'message_id', 'thread_id', 'sync_state_id', 'batch_id'],
  },

  'dealsync_stg_v1.batch_events': {
    columns: `
      trigger_hash VARCHAR(255) PRIMARY KEY,
      batch_id VARCHAR(128) NOT NULL,
      batch_type VARCHAR(20) NOT NULL,
      event_type VARCHAR(20) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
    indexes: ['batch_id'],
  },

  'dealsync_stg_v1.ai_evaluation_audits': {
    columns: `
      id VARCHAR(255) PRIMARY KEY,
      batch_id VARCHAR(128),
      thread_count INTEGER,
      email_count INTEGER,
      ai_evaluation VARCHAR,
      inference_cost DECIMAL(5,4),
      input_tokens INTEGER,
      model_used VARCHAR(255),
      output_tokens INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
    uniqueIndexes: ['batch_id'],
  },

  'dealsync_stg_v1.email_thread_evaluations': {
    columns: `
      id VARCHAR(255) PRIMARY KEY,
      thread_id VARCHAR(255) NOT NULL,
      ai_evaluation_audit_id VARCHAR(255),
      ai_insight VARCHAR(1500),
      ai_summary VARCHAR(3000),
      is_deal BOOLEAN,
      likely_scam BOOLEAN,
      ai_score INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
    indexes: ['ai_evaluation_audit_id'],
    uniqueIndexes: ['thread_id'],
  },

  'dealsync_stg_v1.deals': {
    columns: `
      id VARCHAR(255) PRIMARY KEY,
      user_id VARCHAR(255),
      thread_id VARCHAR(255) NOT NULL,
      email_thread_evaluation_id VARCHAR(255),
      deal_name VARCHAR(6400),
      deal_type VARCHAR(255),
      category VARCHAR(255),
      value DECIMAL(15,2),
      currency VARCHAR(10),
      brand VARCHAR(255),
      is_ai_sorted BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
    indexes: ['user_id'],
    uniqueIndexes: ['thread_id'],
  },

  'dealsync_stg_v1.user_sync_settings': {
    columns: `
      id VARCHAR(255) PRIMARY KEY,
      user_id VARCHAR(255) NOT NULL,
      email VARCHAR(255) DEFAULT '',
      timezone VARCHAR(10),
      time_of_day VARCHAR(10) DEFAULT '09:00',
      frequency VARCHAR(20) DEFAULT 'daily',
      next_sync_at TIMESTAMP,
      emails_processed_since_last_sync INTEGER DEFAULT 0,
      last_synced_at TIMESTAMP,
      last_sync_requested_at TIMESTAMP,
      sync_status VARCHAR(20) DEFAULT 'pending',
      skip_inbox BOOLEAN DEFAULT FALSE,
      daily_digest BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
    indexes: ['user_id'],
  },

  'dealsync_stg_v1.deal_contacts': {
    columns: `
      deal_id VARCHAR(255),
      user_id VARCHAR(255),
      email VARCHAR(255),
      contact_type VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (deal_id, user_id, email)`,
    indexes: ['deal_id', 'user_id'],
  },
}

// ── Parse args ────────────────────────────────────────────────────────
const args = process.argv.slice(2)
const action = args[0] || 'verify'
const fullTable = args[1] // e.g., DEALSYNC_STG_V1.deal_states
const publicKeyArg = args.find((a) => a.startsWith('--public-key='))?.split('=')[1]

if (!fullTable) {
  console.error('Usage: manage-table.js <action> <SCHEMA.table> [--public-key=<hex>]')
  process.exit(1)
}

const [SCHEMA, TABLE] = fullTable.split('.')
const RESOURCE = `${SCHEMA.toLowerCase()}.${TABLE.toLowerCase()}`

async function main() {
  console.log(`\n═══ ${action}: ${SCHEMA}.${TABLE} ═══`)

  const { jwt } = await authenticate()
  console.log('Authenticated')

  const privateKey = process.env.SXT_PRIVATE_KEY
  const publicKey = publicKeyArg || (await derivePublicKey(privateKey))

  switch (action) {
    case 'drop':
      await drop(jwt, privateKey)
      break
    case 'create':
      await create(jwt, privateKey, publicKey)
      await verify(jwt, privateKey)
      break
    case 'recreate':
      await drop(jwt, privateKey)
      await create(jwt, privateKey, publicKey)
      await verify(jwt, privateKey)
      break
    case 'verify':
      await verify(jwt, privateKey)
      break
    default:
      console.error(`Unknown action: ${action}. Use: create, drop, recreate, verify`)
      process.exit(1)
  }

  console.log('\nDone.\n')
}

async function drop(jwt, privateKey) {
  console.log('\nDropping...')
  const biscuit = generateBiscuit('ddl_drop', RESOURCE, privateKey)
  try {
    await executeSql(jwt, `DROP TABLE ${SCHEMA}.${TABLE}`, biscuit)
    console.log('  Dropped')
  } catch (err) {
    if (err.message.includes('not found') || err.message.includes('does not exist')) {
      console.log('  Table does not exist (skipping)')
    } else {
      throw err
    }
  }
}

async function create(jwt, privateKey, publicKey) {
  const def = DDL_REGISTRY[RESOURCE]
  if (!def) {
    console.error(`No DDL definition found for ${RESOURCE}. Add it to DDL_REGISTRY in this script.`)
    process.exit(1)
  }

  console.log('\nCreating table...')
  const createSql = `CREATE TABLE ${SCHEMA}.${TABLE} (${def.columns}
  ) WITH "public_key=${publicKey},access_type=permissioned"`

  const biscuit = generateBiscuit('ddl_create', RESOURCE, privateKey)
  await executeSql(jwt, createSql, biscuit)
  console.log('  Created')

  if (def.indexes?.length) {
    console.log('Creating indexes...')
    for (const col of def.indexes) {
      const idxName = `idx_${TABLE}_${col}`
      const idxSql = `CREATE INDEX ${idxName} ON ${SCHEMA}.${TABLE} (${col})`
      await executeSql(jwt, idxSql, biscuit)
      console.log(`  ${idxName}`)
    }
  }

  if (def.uniqueIndexes?.length) {
    console.log('Creating unique indexes...')
    for (const col of def.uniqueIndexes) {
      const idxName = `uniq_${TABLE}_${col}`
      const idxSql = `CREATE UNIQUE INDEX ${idxName} ON ${SCHEMA}.${TABLE} (${col})`
      await executeSql(jwt, idxSql, biscuit)
      console.log(`  ${idxName}`)
    }
  }
}

async function verify(jwt, privateKey) {
  console.log('\nVerifying...')
  const biscuit = generateBiscuit('dql_select', RESOURCE, privateKey)
  const result = await executeSql(jwt, `SELECT COUNT(*) AS CNT FROM ${SCHEMA}.${TABLE}`, biscuit)
  console.log(`  Table exists, rows: ${result?.[0]?.CNT ?? 0}`)
}

main().catch((err) => {
  console.error('\nFailed:', err.message)
  process.exit(1)
})
