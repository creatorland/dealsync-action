import { SpaceAndTime } from 'sxt-nodejs-sdk'

const tables = process.argv.slice(2)
if (tables.length === 0) {
  console.error('Usage: generate-biscuit.js <table1> [table2] ...')
  process.exit(1)
}

const pk = process.env.SXT_PRIVATE_KEY
if (!pk) {
  console.error('SXT_PRIVATE_KEY not set')
  process.exit(1)
}

const sxt = new SpaceAndTime()
const auth = sxt.Authorization()
const ops = ['dql_select', 'dml_insert', 'dml_update', 'dml_delete']
const permissions = []
for (const table of tables) {
  for (const op of ops) {
    permissions.push({ operation: op, resource: table.toLowerCase() })
  }
}

const result = auth.CreateBiscuitToken(permissions, pk)
// Write ONLY the biscuit to stdout, everything else to stderr
process.stdout.write(result.data[0])
