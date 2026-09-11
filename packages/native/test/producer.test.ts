import * as Net from "node:net"
import { once } from "node:events"
import { Effect } from "effect"
import { Producer } from "@effect-kafka/core"
import { expect, test } from "vitest"
import { producerLayer, type ProducerOptions } from "../src/index.js"
import { Reader, Writer, FrameDecoder } from "../src/internal/binary.js"

async function broker(options: { produceError?: number; metadataError?: number; wrongCorrelation?: boolean; leaderPort?: number } = {}) {
  const apis: number[] = []
  const partitions: number[] = []
  const server = Net.createServer((socket) => {
    const decoder = new FrameDecoder(1024 * 1024)
    socket.on("error", () => {})
    socket.on("data", (chunk: Buffer) => {
      const frame = decoder.push(chunk)
      if (!frame) return
      const reader = new Reader(frame)
      const api = reader.i16()
      reader.i16()
      const correlation = reader.i32()
      reader.string(true)
      apis.push(api)
      const body = new Writer()
      if (api === 18) {
        body.i16(0).i32(2).i16(0).i16(3).i16(12).i16(3).i16(0).i16(13)
      } else if (api === 3) {
        body.i32(0).i32(1).i32(1).string("127.0.0.1").i32(options.leaderPort ?? port).string(null).string("cluster").i32(1)
          .i32(1).i16(options.metadataError ?? 0).string("events").i8(0).i32(3)
        for (let id = 0; id < 3; id++) body.i16(0).i32(id).i32(1).i32(1).i32(1).i32(1).i32(1)
      } else if (api === 0) {
        reader.string(true)
        expect(reader.i16()).toBe(-1)
        reader.i32()
        expect(reader.i32()).toBe(1)
        expect(reader.string()).toBe("events")
        const ids = reader.array(() => {
          const id = reader.i32()
          reader.take(reader.i32())
          partitions.push(id)
          return id
        })
        reader.end()
        body.i32(1).string("events").i32(ids.length)
        for (const id of ids) body.i32(id).i16(options.produceError ?? 0).i64(9007199254740993n).i64(-1n)
        body.i32(0)
      }
      const response = new Writer().i32(correlation + (options.wrongCorrelation ? 1 : 0)).raw(body.finish()).finish()
      socket.end(new Writer().bytes(response).finish())
    })
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const port = (server.address() as Net.AddressInfo).port
  return {
    apis, partitions, address: `127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve()))
  }
}
const send = (options: ProducerOptions, partition?: number) => Producer.pipe(
  Effect.flatMap((producer) => producer.send({ topic: "events", messages: [{ key: "key", value: "value", ...(partition === undefined ? {} : { partition }) }] })),
  Effect.provide(producerLayer({ metadataRetries: 0, ...options }))
)

test("discovers leaders, checks versions, and preserves acknowledged int64 offsets", async () => {
  const b = await broker()
  try {
    const reports = await Effect.runPromise(send({ brokers: [b.address] }, 2))
    expect(reports).toEqual([{ topicName: "events", partition: 2, errorCode: 0, baseOffset: "9007199254740993" }])
    expect(b.apis).toEqual([18, 3, 18, 0])
  } finally { await b.close() }
})
test("bootstrap failover retries reads but never repeats a rejected Produce", async () => {
  const unused = Net.createServer()
  unused.listen(0, "127.0.0.1")
  await once(unused, "listening")
  const deadPort = (unused.address() as Net.AddressInfo).port
  await new Promise<void>((resolve) => unused.close(() => resolve()))
  const b = await broker({ produceError: 6 })
  try {
    const error = await Effect.runPromise(send({ brokers: [`127.0.0.1:${deadPort}`, b.address] }).pipe(Effect.flip))
    expect(error).toMatchObject({ operation: "native.produce", cause: { _tag: "KafkaBrokerError", code: 6, topic: "events" } })
    expect(b.apis.filter((api) => api === 0)).toHaveLength(1)
  } finally { await b.close() }
})
test("metadata authorization failures retain the broker code and stop retries", async () => {
  const b = await broker({ metadataError: 29 })
  try {
    const error = await Effect.runPromise(send({ brokers: [b.address], metadataRetries: 2 }).pipe(Effect.flip))
    expect(error.cause).toMatchObject({ code: 29, api: "Metadata" })
    expect(b.apis).toEqual([18, 3])
  } finally { await b.close() }
})
test("correlation mismatch cannot be mistaken for a successful response", async () => {
  const b = await broker({ wrongCorrelation: true })
  try {
    const error = await Effect.runPromise(send({ brokers: [b.address] }).pipe(Effect.flip))
    expect(String(error.cause)).toContain("correlation")
    expect(b.apis).toEqual([18])
  } finally { await b.close() }
})
test("keyed partitioning is stable across sends", async () => {
  const b = await broker()
  try {
    await Effect.runPromise(Producer.pipe(Effect.flatMap((p) => Effect.gen(function*() {
      for (let i = 0; i < 3; i++) yield* p.send({ topic: "events", messages: [{ key: "same", value: "value" }] })
    })), Effect.provide(producerLayer({ brokers: [b.address] }))))
    expect(new Set(b.partitions).size).toBe(1)
  } finally { await b.close() }
})
test("unknown partitions and oversized values fail before any Produce request", async () => {
  const b = await broker()
  try {
    expect((await Effect.runPromise(send({ brokers: [b.address] }, 99).pipe(Effect.flip))).operation).toBe("native.encode")
    await Effect.runPromise(Producer.pipe(Effect.flatMap((p) => p.send({ topic: "events", messages: [{ value: Buffer.alloc(1000) }] })), Effect.provide(producerLayer({ brokers: [b.address], maxRequestBytes: 256 })), Effect.flip))
    expect(b.apis).not.toContain(0)
  } finally { await b.close() }
})
test("empty sends need no broker, and invalid configuration produces typed errors", async () => {
  const result = await Effect.runPromise(Producer.pipe(Effect.flatMap((p) => p.send({ topic: "events", messages: [] })), Effect.provide(producerLayer({ brokers: ["localhost:1"] }))))
  expect(result).toEqual([])
  for (const options of [{ brokers: [] }, { brokers: ["host"] }, { brokers: ["host:9092"], requestTimeoutMs: -1 }, { brokers: ["host:9092"], acks: 0 }]) {
    const error = await Effect.runPromise(Producer.pipe(Effect.provide(producerLayer(options as ProducerOptions)), Effect.flip))
    expect(error.operation).toBe("native.config")
  }
})

test("routes Produce to the advertised leader rather than the bootstrap socket", async () => {
  const leader = await broker()
  const bootstrap = await broker({ leaderPort: Number(leader.address.split(":")[1]) })
  try {
    await Effect.runPromise(send({ brokers: [bootstrap.address] }))
    expect(bootstrap.apis).toEqual([18, 3])
    expect(leader.apis).toEqual([18, 0])
  } finally { await bootstrap.close(); await leader.close() }
})
