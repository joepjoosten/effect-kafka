import type { Bytes, Message } from "@effect-kafka/core"
import { Writer } from "./binary.js"

export const bytes = (value: Bytes, limit = 16 * 1024 * 1024): Buffer => {
  const length = typeof value === "string" ? Buffer.byteLength(value) : value.byteLength
  if (length > limit) throw new Error(`Record field exceeds ${limit} bytes`)
  return Buffer.from(value)
}

const crcTable = Uint32Array.from({ length: 256 }, (_, byte) => {
  let crc = byte
  for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0x82f63b78 : 0)
  return crc >>> 0
})
export const crc32c = (buffer: Uint8Array): number => {
  let crc = 0xffffffff
  for (const byte of buffer) crc = (crc >>> 8) ^ crcTable[(crc ^ byte) & 255]!
  return (crc ^ 0xffffffff) >>> 0
}

/** Kafka's Java-compatible Murmur2 hash. */
export const murmur2 = (data: Uint8Array): number => {
  let hash = (0x9747b28c ^ data.length) >>> 0
  let offset = 0
  while (offset + 4 <= data.length) {
    let k = data[offset]! | data[offset + 1]! << 8 | data[offset + 2]! << 16 | data[offset + 3]! << 24
    k = Math.imul(k, 0x5bd1e995)
    k ^= k >>> 24
    k = Math.imul(k, 0x5bd1e995)
    hash = Math.imul(hash, 0x5bd1e995) ^ k
    offset += 4
  }
  const remaining = data.length - offset
  if (remaining >= 3) hash ^= data[offset + 2]! << 16
  if (remaining >= 2) hash ^= data[offset + 1]! << 8
  if (remaining >= 1) { hash ^= data[offset]!; hash = Math.imul(hash, 0x5bd1e995) }
  hash ^= hash >>> 13
  hash = Math.imul(hash, 0x5bd1e995)
  hash ^= hash >>> 15
  return hash >>> 0
}

const timestamp = (message: Message, now: bigint): bigint => {
  if (message.timestamp === undefined) return now
  if (!/^\d+$/.test(message.timestamp)) throw new Error("Timestamp must be a non-negative integer string")
  const value = BigInt(message.timestamp)
  if (value > 0x7fffffffffffffffn) throw new Error("Timestamp exceeds int64")
  return value
}

/** Message format v2: uncompressed records with CRC32C and zigzag varints. */
export const recordBatch = (messages: ReadonlyArray<Message>, limit: number, now = BigInt(Date.now())): Buffer => {
  if (messages.length === 0) throw new Error("Cannot encode an empty record batch")
  const firstTimestamp = timestamp(messages[0]!, now)
  let maxTimestamp = firstTimestamp
  const records = new Writer(limit)
  for (let index = 0; index < messages.length; index++) {
    const message = messages[index]!
    const time = timestamp(message, now)
    if (time > maxTimestamp) maxTimestamp = time
    const record = new Writer(limit).i8(0).varint(time - firstTimestamp).varint(index)
    for (const value of [message.key ?? null, message.value]) {
      if (value === null) record.varint(-1)
      else { const encoded = bytes(value, limit); record.varint(encoded.length).raw(encoded) }
    }
    const headers = new Writer(limit)
    let count = 0
    for (const [name, values] of Object.entries(message.headers ?? {})) {
      if (values === undefined) continue
      const key = bytes(name, limit)
      for (const value of typeof values === "string" || values instanceof Uint8Array ? [values] : values) {
        const encoded = bytes(value, limit)
        headers.varint(key.length).raw(key).varint(encoded.length).raw(encoded)
        count++
      }
    }
    record.varint(count).raw(headers.finish())
    const encoded = record.finish()
    records.varint(encoded.length).raw(encoded)
  }
  const content = new Writer(limit).i16(0).i32(messages.length - 1).i64(firstTimestamp).i64(maxTimestamp)
    .i64(-1n).i16(-1).i32(-1).i32(messages.length).raw(records.finish()).finish()
  const crc = Buffer.alloc(4)
  crc.writeUInt32BE(crc32c(content))
  const batch = new Writer(limit).i32(-1).i8(2).raw(crc).raw(content).finish()
  return new Writer(limit).i64(0n).i32(batch.length).raw(batch).finish()
}
