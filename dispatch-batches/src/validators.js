export function validatePositiveInt(value, name) {
  const n = parseInt(value, 10)
  if (isNaN(n) || n < 0)
    throw new Error(`${name} must be non-negative integer, got: ${value}`)
  return n
}
