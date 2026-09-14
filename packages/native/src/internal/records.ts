import type { Bytes, Message } from "@effect-kafka/core"
import { gzipSync, gunzipSync, zstdCompressSync, zstdDecompressSync } from "node:zlib"
import { Reader, Writer } from "./binary.js"

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

export type Compression = "none" | "gzip" | "zstd"
export interface BatchOptions {
  readonly compression?: Compression
  readonly producerId?: bigint
  readonly producerEpoch?: number
  readonly sequence?: number
  readonly transactional?: boolean
}
/** Message format v2: uncompressed records with CRC32C and zigzag varints. */
export const recordBatch = (messages: ReadonlyArray<Message>, limit: number, now = BigInt(Date.now()), options: BatchOptions = {}): Buffer => {
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
  const raw = records.finish()
  const compression = options.compression ?? "none"
  const encodedRecords = compression === "gzip" ? gzipSync(raw, { maxOutputLength: limit })
    : compression === "zstd" ? zstdCompressSync(raw, { maxOutputLength: limit }) : raw
  const attributes = (compression === "gzip" ? 1 : compression === "zstd" ? 4 : 0) | (options.transactional ? 16 : 0)
  const content = new Writer(limit).i16(attributes).i32(messages.length - 1).i64(firstTimestamp).i64(maxTimestamp)
    .i64(options.producerId ?? -1n).i16(options.producerEpoch ?? -1).i32(options.sequence ?? -1).i32(messages.length).raw(encodedRecords).finish()
  const crc = Buffer.alloc(4)
  crc.writeUInt32BE(crc32c(content))
  const batch = new Writer(limit).i32(-1).i8(2).raw(crc).raw(content).finish()
  return new Writer(limit).i64(0n).i32(batch.length).raw(batch).finish()
}

export interface DecodedRecord {
  readonly offset: bigint
  readonly timestamp: string
  readonly key: Buffer | null
  readonly value: Buffer | null
  readonly headers: Record<string, Buffer | Buffer[] | undefined>
}
export interface DecodedBatch {
  readonly baseOffset: bigint
  readonly nextOffset: bigint
  readonly producerId: bigint
  readonly transactional: boolean
  readonly control: boolean
  readonly records: ReadonlyArray<DecodedRecord>
}
/** Decode complete magic-2 batches, checking CRC before bounded decompression. */
export const decodeBatches = (data: Buffer, limit: number): ReadonlyArray<DecodedBatch> => {
  const input = new Reader(data)
  const batches: DecodedBatch[] = []
  let remaining = limit
  while (input.remaining) {
    if (input.remaining < 12) break // Fetch can end with an incomplete batch.
    const baseOffset = input.i64(), size = input.i32()
    if (size < 49) throw new Error("Invalid record batch length")
    if (size > input.remaining) break
    const raw = input.take(size), r = new Reader(raw)
    r.i32()
    if (r.i8() !== 2) throw new Error("Only magic-2 Kafka record batches are supported")
    const expectedCrc = r.take(4).readUInt32BE()
    if (crc32c(raw.subarray(9)) !== expectedCrc) throw new Error("Kafka record batch CRC mismatch")
    const attributes = r.i16(), lastDelta = r.i32()
    if (lastDelta < 0) throw new Error("Invalid record offset delta")
    const firstTimestamp = r.i64(), maxTimestamp = r.i64(), producerId = r.i64()
    r.i16(); r.i32()
    const count = r.i32()
    const payload = r.take(r.remaining), compression = attributes & 7
    if (remaining <= 0) throw new Error("Decompressed fetch exceeds limit")
    const decoded = compression === 0 ? payload : compression === 1 ? gunzipSync(payload, { maxOutputLength: remaining })
      : compression === 4 ? zstdDecompressSync(payload, { maxOutputLength: remaining }) : undefined
    if (!decoded) throw new Error(`Unsupported Kafka compression codec ${compression}; supported codecs are none, gzip, and zstd`)
    remaining -= decoded.length
    if (remaining < 0 || count < 0 || count > decoded.length) throw new Error("Invalid or oversized decompressed records")
    const recordsReader = new Reader(decoded), records: DecodedRecord[] = []
    let previous = -1n
    for (let i = 0; i < count; i++) {
      const record = new Reader(recordsReader.take(Number(recordsReader.varint())))
      record.i8()
      const deltaTime = record.varint(), deltaOffset = record.varint()
      if (deltaOffset < 0n || deltaOffset > BigInt(lastDelta) || deltaOffset <= previous) throw new Error("Invalid record offset ordering")
      previous = deltaOffset
      const readBytes = () => { const size = record.varint(); return size === -1n ? null : record.take(Number(size)) }
      const key = readBytes(), value = readBytes(), headers: DecodedRecord["headers"] = Object.create(null)
      const count = Number(record.varint())
      if (!Number.isSafeInteger(count) || count < 0 || count > record.remaining) throw new Error("Invalid header count")
      for (let h = 0; h < count; h++) {
        const name = record.take(Number(record.varint())).toString("utf8")
        const value = readBytes()
        if (value === null) { if (!(name in headers)) headers[name] = undefined; continue }
        const prior = headers[name]
        headers[name] = prior === undefined ? value : Array.isArray(prior) ? [...prior, value] : [prior, value]
      }
      record.end()
      records.push({ offset: baseOffset + deltaOffset, timestamp: ((attributes & 8) ? maxTimestamp : firstTimestamp + deltaTime).toString(), key, value, headers })
    }
    recordsReader.end()
    batches.push({ baseOffset, nextOffset: baseOffset + BigInt(lastDelta) + 1n, producerId, transactional: !!(attributes & 16), control: !!(attributes & 32), records })
  }
  if (data.length && !batches.length) throw new Error("Fetch did not contain a complete record batch")
  return batches
}
