import { expect, test } from "vitest"
import { FrameDecoder, Reader, Writer } from "../src/internal/binary.js"
import { crc32c, murmur2, recordBatch } from "../src/internal/records.js"
import { checkVersions, produceResponse, responseBody } from "../src/internal/protocol.js"
import { endpoint } from "../src/internal/transport.js"

test("CRC32C matches the standard Castagnoli check vector", () => {
  expect(crc32c(Buffer.from("123456789"))).toBe(0xe3069283)
  expect(crc32c(Buffer.alloc(0))).toBe(0)
})
test("Murmur2 matches Kafka's Java hash vectors", () => {
  expect(murmur2(Buffer.from(""))).toBe(275646681)
  expect(murmur2(Buffer.from("hello"))).toBe(2132663229)
  expect(murmur2(Buffer.from("kafka"))).toBe(3496464228)
})
test("signed varints encode negative deltas and int64 boundaries", () => {
  expect(new Writer().varint(-1).varint(0).varint(1).varint(-64).varint(64).finish().toString("hex")).toBe("0100027f8001")
  expect(new Writer().varint(-9223372036854775808n).finish().toString("hex")).toBe("ffffffffffffffffff01")
  expect(() => new Writer().varint(9223372036854775808n)).toThrow("int64")
})
test("frame decoding accepts every possible split and byte-at-a-time delivery", () => {
  const body = Buffer.from("0000002a0102030405", "hex")
  const frame = new Writer().bytes(body).finish()
  for (let split = 1; split < frame.length; split++) {
    const decoder = new FrameDecoder(100)
    expect(decoder.push(frame.subarray(0, split))).toBeUndefined()
    expect(decoder.push(frame.subarray(split))).toEqual(body)
  }
  const decoder = new FrameDecoder(100)
  for (let i = 0; i < frame.length - 1; i++) expect(decoder.push(frame.subarray(i, i + 1))).toBeUndefined()
  expect(decoder.push(frame.subarray(-1))).toEqual(body)
})
test.each([-1, 0, 3, 101, 2147483647])("rejects frame length %s before allocating its body", (size) => {
  expect(() => new FrameDecoder(100).push(new Writer().i32(size).finish())).toThrow("frame size")
})
test("rejects trailing frame data and mismatched correlation IDs", () => {
  expect(() => new FrameDecoder(100).push(new Writer().i32(4).i32(1).i8(0).finish())).toThrow("Unexpected bytes")
  expect(() => responseBody(new Writer().i32(4).finish(), 5)).toThrow("correlation")
})
test("bounds Kafka arrays, strings, and encoded requests", () => {
  expect(() => new Reader(new Writer().i32(2147483647).finish()).array(() => 0)).toThrow("array length")
  expect(() => new Reader(new Writer().i16(10).i8(0).finish()).string()).toThrow("Truncated")
  expect(() => new Reader(new Writer().i16(-1).finish()).string()).toThrow("invalid")
  expect(() => new Writer(4).raw(Buffer.alloc(5))).toThrow("exceeds")
})
test("rejects unsupported API versions explicitly", () => {
  const versions = new Writer().i16(0).i32(1).i16(0).i16(4).i16(12).finish()
  expect(() => checkVersions(versions, 0, 3)).toThrow("does not support")
})
test("produce responses retain int64 offsets and require every requested partition", () => {
  const body = new Writer().i32(1).string("events").i32(1).i32(2).i16(0).i64(9007199254740993n).i64(-1n).i32(0).finish()
  expect(produceResponse(body, "events", [2])[0]?.baseOffset).toBe("9007199254740993")
  expect(() => produceResponse(body, "events", [2, 3])).toThrow("Missing partition")
  expect(() => produceResponse(body, "other", [2])).toThrow("Unexpected partition")
})
test("batch header preserves timestamp order and validates timestamp/size bounds", () => {
  const batch = recordBatch([{ value: "a", timestamp: "100" }, { value: null, timestamp: "99" }], 1024)
  expect(batch.readInt32BE(8)).toBe(batch.length - 12)
  expect(batch[16]).toBe(2)
  expect(batch.readBigInt64BE(27)).toBe(100n)
  expect(batch.readBigInt64BE(35)).toBe(100n)
  expect(batch.readInt32BE(57)).toBe(2)
  expect(() => recordBatch([{ value: "a", timestamp: "-1" }], 1024)).toThrow("Timestamp")
  expect(() => recordBatch([{ value: "a", timestamp: "9223372036854775808" }], 1024)).toThrow("int64")
  expect(() => recordBatch([{ value: Buffer.alloc(1024) }], 128)).toThrow("exceeds")
})
test("broker addresses support DNS and bracketed IPv6", () => {
  expect(endpoint("kafka:29092")).toEqual({ host: "kafka", port: 29092 })
  expect(endpoint("[::1]:9092")).toEqual({ host: "::1", port: 9092 })
  for (const invalid of ["host", "host:0", "host:65536", "host:-1", "::1:9092", "host:9092/path"]) expect(() => endpoint(invalid)).toThrow()
})
