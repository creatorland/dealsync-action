import { authenticate, generateMasterBiscuit, executeSql } from './sxt-client.js'

const { jwt } = await authenticate()
const pk = process.env.SXT_PRIVATE_KEY
const b = generateMasterBiscuit('dealsync_stg_v1.deal_states', pk)

await executeSql(
  jwt,
  "UPDATE DEALSYNC_STG_V1.DEAL_STATES SET STATUS = 'pending', BATCH_ID = NULL, ATTEMPTS = 0",
  b,
)

const r = await executeSql(
  jwt,
  'SELECT STATUS, COUNT(*) AS CNT FROM DEALSYNC_STG_V1.DEAL_STATES GROUP BY STATUS ORDER BY STATUS',
  b,
)
console.log('Reset done:', JSON.stringify(r))
