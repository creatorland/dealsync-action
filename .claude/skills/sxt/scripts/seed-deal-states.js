import { authenticate, generateMasterBiscuit, executeSql } from './sxt-client.js'
import { randomUUID } from 'crypto'

const userId = process.argv[2]
const syncStateId = process.argv[3]
const limit = parseInt(process.argv[4] || '50')

if (!userId || !syncStateId) {
  console.error('Usage: seed-deal-states.js <userId> <syncStateId> [limit]')
  process.exit(1)
}

const { jwt } = await authenticate()
const pk = process.env.SXT_PRIVATE_KEY

const emBiscuit = generateMasterBiscuit('email_core_staging.email_metadata', pk)
const emails = await executeSql(
  jwt,
  `SELECT ID, MESSAGE_ID, THREAD_ID FROM EMAIL_CORE_STAGING.EMAIL_METADATA WHERE USER_ID = '${userId}' LIMIT ${limit}`,
  emBiscuit,
)
console.log(`Fetched ${emails.length} emails for user ${userId}`)

const dsBiscuit = generateMasterBiscuit('dealsync_stg_v1.deal_states', pk)
let inserted = 0
for (const em of emails) {
  const id = randomUUID()
  try {
    await executeSql(
      jwt,
      `INSERT INTO DEALSYNC_STG_V1.DEAL_STATES (ID, EMAIL_METADATA_ID, USER_ID, MESSAGE_ID, THREAD_ID, SYNC_STATE_ID, STATUS, ATTEMPTS, PROCESSED_AT, CREATED_AT, UPDATED_AT) VALUES ('${id}', '${em.ID}', '${userId}', '${em.MESSAGE_ID}', '${em.THREAD_ID}', '${syncStateId}', 'pending', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      dsBiscuit,
    )
    inserted++
  } catch (err) {
    console.error('Failed:', em.ID, err.message.substring(0, 80))
  }
}
console.log(`Inserted ${inserted} deal_states with status=pending`)
