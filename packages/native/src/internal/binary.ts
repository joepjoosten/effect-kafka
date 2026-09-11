/** Bounded Kafka binary primitives. Wire integers are big-endian. */
export class Writer {
  private readonly chunks: Buffer[] = []
  private length = 0
  constructor(readonly limit = 16 * 1024 * 1024) {}
  raw(value: Uint8Array): this {
    this.length += value.byteLength
    if (this.length > this.limit) throw new Error(`Encoded request exceeds ${this.limit} bytes`)
    this.chunks.push(Buffer.from(value.buffer, value.byteOffset, value.byteLength))
    return this
  }
  i8(value: number): this { const b = Buffer.alloc(1); b.writeInt8(value); return this.raw(b) }
  i16(value: number): this { const b = Buffer.alloc(2); b.writeInt16BE(value); return this.raw(b) }
  i32(value: number): this { const b = Buffer.alloc(4); b.writeInt32BE(value); return this.raw(b) }
  i64(value: bigint): this { const b = Buffer.alloc(8); b.writeBigInt64BE(value); return this.raw(b) }
  string(value: string | null): this {
    if (value === null) return this.i16(-1)
    const b = Buffer.from(value)
    if (b.length > 32767) throw new Error("Kafka string exceeds 32767 bytes")
    return this.i16(b.length).raw(b)
  }
  bytes(value: Uint8Array): this { return this.i32(value.byteLength).raw(value) }
  varint(value: bigint | number): this {
    const signed = BigInt(value)
    if (signed < -(1n << 63n) || signed >= 1n << 63n) throw new Error("Varint is outside signed int64 range")
    let n = signed >= 0n ? signed << 1n : ((-signed) << 1n) - 1n
    const bytes: number[] = []
    do { bytes.push(Number(n & 127n) | (n > 127n ? 128 : 0)); n >>= 7n } while (n > 0n)
    return this.raw(Buffer.from(bytes))
  }
  finish(): Buffer { return Buffer.concat(this.chunks, this.length) }
}

export class Reader {
  private offset = 0
  constructor(readonly buffer: Buffer) {}
  get remaining(): number { return this.buffer.length - this.offset }
  take(length: number): Buffer {
    if (!Number.isInteger(length) || length < 0 || length > this.remaining) throw new Error("Truncated or invalid Kafka response")
    const result = this.buffer.subarray(this.offset, this.offset + length)
    this.offset += length
    return result
  }
  i8(): number { return this.take(1).readInt8() }
  i16(): number { return this.take(2).readInt16BE() }
  i32(): number { return this.take(4).readInt32BE() }
  i64(): bigint { return this.take(8).readBigInt64BE() }
  string(nullable: true): string | null
  string(nullable?: false): string
  string(nullable = false): string | null {
    const size = this.i16()
    if (size === -1 && nullable) return null
    return this.take(size).toString("utf8")
  }
  array<A>(read: () => A): A[] {
    const count = this.i32()
    // Every element in the supported schemas consumes at least one byte.
    if (count < 0 || count > this.remaining) throw new Error("Invalid Kafka array length")
    const result: A[] = []
    for (let i = 0; i < count; i++) result.push(read())
    return result
  }
  end(): void { if (this.remaining !== 0) throw new Error("Unexpected trailing Kafka response bytes") }
}

/** Incremental, length-bounded frame decoder; does not repeatedly concatenate chunks. */
export class FrameDecoder {
  private readonly header = Buffer.alloc(4)
  private headerLength = 0
  private body: Buffer | undefined
  private bodyLength = 0
  constructor(private readonly limit: number) {}
  push(chunk: Buffer): Buffer | undefined {
    let offset = 0
    if (this.headerLength < 4) {
      const count = Math.min(4 - this.headerLength, chunk.length)
      chunk.copy(this.header, this.headerLength, 0, count)
      this.headerLength += count
      offset += count
      if (this.headerLength < 4) return undefined
      const size = this.header.readInt32BE()
      if (size < 4 || size > this.limit) throw new Error(`Invalid Kafka frame size ${size}`)
      this.body = Buffer.allocUnsafe(size)
    }
    const body = this.body!
    const count = chunk.length - offset
    if (count > body.length - this.bodyLength) throw new Error("Unexpected bytes after Kafka frame")
    chunk.copy(body, this.bodyLength, offset)
    this.bodyLength += count
    return this.bodyLength === body.length ? body : undefined
  }
}
