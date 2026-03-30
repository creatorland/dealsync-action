// SQL sanitization utilities.
// Extracted to break circular dependency between queries.js and sql builders.

export function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`Invalid ID format: ${id}`)
  }
  return id
}

export function sanitizeString(s) {
  return (s || '')
    .replace(/[\u2018\u2019\u201A\u201B]/g, "'") // curly single quotes → straight
    .replace(/[\u201C\u201D\u201E\u201F]/g, '"') // curly double quotes → straight
    .replace(/'/g, "''") // escape single quotes for SQL
    .replace(/\\/g, '\\\\') // escape backslashes
}

export function toSqlIdList(ids) {
  return ids.map((id) => `'${sanitizeId(id)}'`).join(',')
}

export function sanitizeSchema(schema) {
  if (!/^[a-zA-Z0-9_]+$/.test(schema)) {
    throw new Error(`Invalid schema: ${schema}`)
  }
  return schema
}
