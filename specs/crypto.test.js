import { describe, it, expect } from 'vitest'
import { encryptValue, decryptValue } from '../shared/crypto.js'

describe('crypto', () => {
  const key = 'a'.repeat(64) // 256-bit hex key

  it('round-trips a string through encrypt/decrypt', () => {
    const plaintext = 'hello world'
    const ciphertext = encryptValue(plaintext, key)
    expect(ciphertext).not.toBe(plaintext)
    expect(decryptValue(ciphertext, key)).toBe(plaintext)
  })

  it('produces different ciphertext for same input (random IV)', () => {
    const plaintext = 'test'
    const a = encryptValue(plaintext, key)
    const b = encryptValue(plaintext, key)
    expect(a).not.toBe(b)
  })
})
