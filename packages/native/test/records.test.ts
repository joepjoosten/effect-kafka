import { expect, test } from "vitest"
import { crc32c, decodeBatches, recordBatch } from "../src/internal/records.js"
import { assignment, readAssignment, readSubscription, subscriptionMetadata } from "../src/consumer.js"
import { decode, encode } from "../src/internal/wire.js"
import { Reader, Writer } from "../src/internal/binary.js"

for (const compression of ["none", "gzip", "zstd"] as const) {
  test(`${compression}: decodes binary values, tombstones, repeated headers and int64 offsets`, () => {
    const batch = recordBatch([{ key: "k", value: new Uint8Array([0, 255]), timestamp: "9007199254740993", headers: { x: ["a", "b"] } }, { value: null, timestamp: "9007199254740992" }], 10000, 0n, { compression, producerId: 42n, producerEpoch: 1, sequence: 0, transactional: true })
    batch.writeBigInt64BE(9007199254740993n, 0)
    const [decoded] = decodeBatches(batch, 10000)
    expect(decoded).toMatchObject({ baseOffset: 9007199254740993n, nextOffset: 9007199254740995n, producerId: 42n, transactional: true, control: false })
    expect(decoded!.records.map((r) => [r.offset, r.timestamp, r.value])).toEqual([[9007199254740993n, "9007199254740993", Buffer.from([0, 255])], [9007199254740994n, "9007199254740992", null]])
    expect(decoded!.records[0]!.headers.x).toEqual([Buffer.from("a"), Buffer.from("b")])
  })
  test(`${compression}: rejects corruption before processing records`, () => {
    const batch = recordBatch([{ value: "payload" }], 10000, 0n, { compression })
    batch[batch.length - 1]! ^= 1
    expect(() => decodeBatches(batch, 10000)).toThrow(/CRC/)
  })
}
test("decompression limits apply across batches and reject expansion bombs", () => {
  const batch = recordBatch([{ value: "x".repeat(10000) }], 20000, 0n, { compression: "gzip" })
  expect(batch.length).toBeLessThan(200)
  expect(() => decodeBatches(batch, 1000)).toThrow()
  expect(() => decodeBatches(Buffer.concat([batch, batch]), 15000)).toThrow()
})
test("rejects unsupported compression and invalid records even with valid CRC", () => {
  const batch = recordBatch([{ value: "x" }], 10000)
  batch.writeInt16BE(2, 21)
  batch.writeUInt32BE(crc32c(batch.subarray(21)), 17)
  expect(() => decodeBatches(batch, 10000)).toThrow(/Unsupported Kafka compression/)
})
test("truncated fetch batches cannot silently stall consumption", () => {
  const batch = recordBatch([{ value: "x" }], 10000)
  expect(() => decodeBatches(batch.subarray(0, -1), 10000)).toThrow(/complete/)
  expect(decodeBatches(Buffer.concat([batch, batch.subarray(0, 20)]), 10000)).toHaveLength(1)
})
test("consumer metadata and assignments use Kafka subscription version zero", () => {
  expect(subscriptionMetadata(["one"]).toString("hex")).toBe("00000000000100036f6e65ffffffff")
  expect(readSubscription(subscriptionMetadata(["a", "b"]))).toEqual(["a", "b"])
  expect(readAssignment(assignment(new Map([["a", [0, 2]], ["b", []]])))).toEqual([{ topic: "a", partition: 0 }, { topic: "a", partition: 2 }])
  expect(() => readAssignment(assignment(new Map([["a", [0, 0]]])))).toThrow(/Invalid/)
})
test("transaction offset commits use flexible strings, arrays, and tagged fields", () => {
  const body = encode("TxnOffsetCommit", { transactionalId: "tx", groupId: "g", producerId: 5n, producerEpoch: 2, generationId: 3, memberId: "m", groupInstanceId: null, topics: [{ name: "t", partitions: [{ partitionIndex: 1, committedOffset: 9007199254740993n, committedLeaderEpoch: -1, committedMetadata: null }] }] }, 1000)
  const r = new Reader(body)
  expect(r.uvarint()).toBe(3); expect(r.take(2).toString()).toBe("tx")
  expect(r.uvarint()).toBe(2); expect(r.take(1).toString()).toBe("g")
  expect(r.i64()).toBe(5n); expect(r.i16()).toBe(2); expect(r.i32()).toBe(3)
  expect(body.subarray(-3)).toEqual(Buffer.alloc(3))
  const response = new Writer().i32(0).uvarint(2).uvarint(2).raw(Buffer.from("t")).uvarint(2).i32(1).i16(0).uvarint(1).uvarint(7).uvarint(2).raw(Buffer.from([1, 2])).uvarint(0).uvarint(0).finish()
  expect(decode("TxnOffsetCommit", response)).toEqual({ throttleTimeMs: 0, topics: [{ name: "t", partitions: [{ partitionIndex: 1, errorCode: 0 }] }] })
  expect(() => decode("TxnOffsetCommit", response.subarray(0, -1))).toThrow()
})
