import { Effect } from "effect"
import { expect, test, vi, beforeEach } from "vitest"
import { Producer, Consumer } from "@effect-kafka/core"
import { producerLayer, consumerLayer } from "../src/index.js"

const mock = vi.hoisted(() => {
  const producer = { connect: vi.fn(async () => {}), disconnect: vi.fn(async () => {}), send: vi.fn(async (_record: unknown) => []) }
  const consumer = {
    connect: vi.fn(async () => {}), disconnect: vi.fn(async () => {}),
    subscribe: vi.fn(async (_subscription: unknown) => {}),
    run: vi.fn(async (_config: any) => {}),
    commitOffsets: vi.fn(async (_offsets: unknown) => {}),
    events: { CRASH: "crash" }, on: vi.fn((_event: string, _listener: (event: { payload: { restart: boolean; error: unknown } }) => void) => () => {})
  }
  const makeProducer = vi.fn((_config: unknown) => producer)
  const makeConsumer = vi.fn((_config: unknown) => consumer)
  class Kafka {
    constructor(_config?: unknown) {}
    producer = makeProducer
    consumer = makeConsumer
  }
  return { producer, consumer, makeProducer, makeConsumer, Kafka }
})
vi.mock("kafkajs", () => ({ default: { Kafka: mock.Kafka, Partitioners: {} } }))
beforeEach(() => { vi.clearAllMocks() })

const client = { brokers: ["localhost:9092"] }

test("converts Uint8Array slices and repeated headers without losing tombstones", async () => {
  const value = new Uint8Array([99, 1, 2, 99]).subarray(1, 3)
  await Effect.runPromise(Producer.pipe(Effect.flatMap((p) => p.send({
    topic: "events", messages: [
      { value, key: new Uint8Array([3]), headers: { repeated: ["text", new Uint8Array([4])] } },
      { value: null, key: "deleted" }
    ]
  })), Effect.provide(producerLayer({ client }))))
  const sent = mock.producer.send.mock.calls[0]![0] as any
  expect(sent.messages[0].value).toEqual(Buffer.from([1, 2]))
  expect(sent.messages[0].key).toEqual(Buffer.from([3]))
  expect(sent.messages[0].headers.repeated).toEqual(["text", Buffer.from([4])])
  expect(sent.messages[1]).toEqual({ value: null, key: "deleted" })
  expect(mock.producer.disconnect).toHaveBeenCalledOnce()
})

test("commits the next offset without rounding large Kafka offsets", async () => {
  const heartbeat = vi.fn(async () => {})
  mock.consumer.run.mockImplementationOnce(async (config) => {
    void config.eachMessage({
      topic: "events", partition: 2,
      message: { value: null, key: null, offset: "9007199254740993", timestamp: "0" }, heartbeat
    }).catch(() => {})
  })
  const error = await Effect.runPromise(Consumer.pipe(Effect.flatMap((c) => c.consume({
    topics: ["events"], fromBeginning: true
  }, (record) => Effect.gen(function*() {
    expect(record.value).toBeNull()
    yield* record.heartbeat
    yield* record.commit
    return yield* Effect.fail("done")
  }))), Effect.provide(consumerLayer({ client, consumer: { groupId: "test" } })), Effect.flip))
  expect(error).toBe("done")
  expect(heartbeat).toHaveBeenCalledOnce()
  expect(mock.consumer.commitOffsets).toHaveBeenCalledWith([
    { topic: "events", partition: 2, offset: "9007199254740994" }
  ])
  expect(mock.consumer.subscribe).toHaveBeenCalledWith({ topics: ["events"], fromBeginning: true })
  expect(mock.consumer.disconnect).toHaveBeenCalledOnce()
})

test("allows restartable crashes to recover and surfaces only terminal crashes", async () => {
  const transient = new Error("coordinator loading")
  const terminal = new Error("retries exhausted")
  mock.consumer.run.mockImplementationOnce(async () => {
    const listener = mock.consumer.on.mock.calls[0]![1]
    listener({ payload: { restart: true, error: transient } })
    queueMicrotask(() => listener({ payload: { restart: false, error: terminal } }))
  })
  const error = await Effect.runPromise(Consumer.pipe(
    Effect.flatMap((consumer) => consumer.consume({ topics: ["events"] }, () => Effect.void)),
    Effect.provide(consumerLayer({ client, consumer: { groupId: "test" } })),
    Effect.flip
  ))
  expect(error).toMatchObject({ operation: "consumer.run", cause: terminal })
  expect(mock.consumer.disconnect).toHaveBeenCalledOnce()
})
