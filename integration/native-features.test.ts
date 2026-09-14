import KafkaJS from "kafkajs"
import { Effect, Layer } from "effect"
import { expect, test } from "vitest"
import { Consumer, KafkaError, Producer, type ConsumerRecord } from "@effect-kafka/core"
import * as Native from "@effect-kafka/native"
import { createTopic } from "./helpers.js"

const broker = process.env.KAFKA_BROKER ?? "localhost:9092"
const authBroker = process.env.KAFKA_SASL_BROKER ?? "localhost:9094"
let counter = 0
async function fixture(partitions = 1) {
  const topic = `native-features-${Date.now()}-${counter++}`
  const admin = new KafkaJS.Kafka({ brokers: [broker], logLevel: KafkaJS.logLevel.NOTHING }).admin(); await admin.connect()
  try { await createTopic(admin, topic, partitions) }
  catch (error) { await admin.disconnect(); throw error }
  return { topic, admin, close: async () => { await admin.deleteTopics({ topics: [topic] }); await admin.disconnect() } }
}
const send = (topic: string, values: string[], options: Partial<Native.ProducerOptions> = {}) => Effect.runPromise(Producer.pipe(Effect.flatMap((p) => p.send({ topic, messages: values.map((value) => ({ value, partition: 0 })) })), Effect.provide(Native.producerLayer({ brokers: [broker], ...options }))))
async function collect(topic: string, count: number, options: Partial<Native.ConsumerOptions> = {}, handle?: (record: ConsumerRecord) => Effect.Effect<void>) {
  const seen: string[] = []
  const controller = new AbortController()
  let done!: () => void
  const received = new Promise<void>((r) => { done = r })
  const work = Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: [topic], fromBeginning: true }, (record) => Effect.gen(function*() {
    if (handle) yield* handle(record)
    seen.push(Buffer.from(record.value ?? []).toString())
    yield* record.commit
    if (seen.length === count) setImmediate(done)
  }))), Effect.provide(Native.consumerLayer({ brokers: [broker], groupId: topic + "-group", ...options })))
  const run = Effect.runPromiseExit(work, { signal: controller.signal })
  const timeout = setTimeout(() => controller.abort(), 30000)
  try { await Promise.race([received, run.then((exit) => { throw new Error("Consumer stopped: " + JSON.stringify(exit)) })]); return seen }
  finally { clearTimeout(timeout); controller.abort(); await run }
}
for (const compression of ["none", "gzip", "zstd"] as const) {
  test(`native ${compression}: producer and consumer round trip`, async () => {
    const f = await fixture()
    try { await send(f.topic, ["a", "b", "c"], { compression }); expect(await collect(f.topic, 3)).toEqual(["a", "b", "c"]) }
    finally { await f.close() }
  })
}
for (const mechanism of ["plain", "scram-sha-256", "scram-sha-512"] as const) {
  test(`native SASL ${mechanism}: authenticated produce and consume`, async () => {
    const f = await fixture()
    const sasl = { mechanism, username: mechanism === "plain" ? "test" : "te\u00adst", password: mechanism === "plain" ? "test-secret" : "te\u00adst-secret" }
    try {
      await send(f.topic, [mechanism], { brokers: [authBroker], sasl })
      expect(await collect(f.topic, 1, { brokers: [authBroker], sasl })).toEqual([mechanism])
      const error = await Effect.runPromise(Producer.pipe(Effect.flatMap((p) => p.send({ topic: f.topic, messages: [{ value: "rejected" }] })), Effect.provide(Native.producerLayer({ brokers: [authBroker], sasl: { ...sasl, password: "wrong-secret" } })), Effect.flip))
      expect(error.operation).toBe("native.sasl")
      expect(String(error.cause)).not.toContain("wrong-secret")
    } finally { await f.close() }
  })
}
test("native consumer resumes committed offsets and keeps heartbeating during a handler", async () => {
  const f = await fixture()
  try {
    await send(f.topic, ["first"])
    expect(await collect(f.topic, 1, { sessionTimeoutMs: 6000, heartbeatIntervalMs: 500 }, () => Effect.sleep(7000))).toEqual(["first"])
    await send(f.topic, ["second"])
    expect(await collect(f.topic, 1)).toEqual(["second"])
  } finally { await f.close() }
})
test("native consumer preserves handler failure and does not commit it", async () => {
  const f = await fixture()
  try {
    await send(f.topic, ["retry"])
    const error = await Effect.runPromise(Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: [f.topic], fromBeginning: true }, () => Effect.fail("handler-error"))), Effect.provide(Native.consumerLayer({ brokers: [broker], groupId: f.topic + "-group" })), Effect.flip))
    expect(error).toBe("handler-error")
    expect(await collect(f.topic, 1)).toEqual(["retry"])
  } finally { await f.close() }
})
test("native transactions commit, abort, reject escaped producers and hide aborted records", async () => {
  const f = await fixture()
  try {
    await Effect.runPromise(Native.Transactions.pipe(Effect.flatMap((txs) => Effect.gen(function*() {
      let escaped: Native.Transaction | undefined
      yield* txs.withTransaction((tx) => { escaped = tx; return tx.send({ topic: f.topic, messages: [{ value: "committed" }] }) })
      expect((yield* escaped!.send({ topic: f.topic, messages: [{ value: "escaped" }] }).pipe(Effect.flip)).operation).toBe("native.transaction.state")
      const failed = yield* txs.withTransaction((tx) => tx.send({ topic: f.topic, messages: [{ value: "aborted" }] }).pipe(Effect.andThen(Effect.fail("abort-me")))).pipe(Effect.flip)
      expect(failed).toBe("abort-me")
      yield* txs.withTransaction((tx) => tx.send({ topic: f.topic, messages: [{ value: "after-abort" }] }))
    })), Effect.provide(Native.transactionLayer({ brokers: [broker], transactionalId: f.topic, compression: "gzip" }))))
    await send(f.topic, ["end"])
    expect(await collect(f.topic, 3)).toEqual(["committed", "after-abort", "end"])
    expect(await collect(f.topic, 4, { groupId: f.topic + "-uncommitted", isolationLevel: "read_uncommitted" })).toEqual(["committed", "aborted", "after-abort", "end"])
  } finally { await f.close() }
})
test("native transactions atomically commit consumed offsets", async () => {
  const input = await fixture(), output = await fixture()
  const groupId = input.topic + "-group"
  const controller = new AbortController()
  try {
    await send(input.topic, ["input"])
    let done!: () => void
    const completed = new Promise<void>((r) => { done = r })
    const run = Effect.runPromiseExit(Effect.gen(function*() {
      const consumer = yield* Consumer, transactions = yield* Native.Transactions
      return yield* consumer.consume({ topics: [input.topic], fromBeginning: true }, (record) => transactions.withTransaction((tx) => Effect.gen(function*() {
        yield* tx.send({ topic: output.topic, messages: [{ value: "output" }] })
        yield* tx.sendOffsets(record.groupMetadata!, [{ topic: record.topic, partition: record.partition, offset: (BigInt(record.offset) + 1n).toString() }])
      })).pipe(Effect.tap(() => Effect.sync(() => { setImmediate(done) }))))
    }).pipe(Effect.provide(Layer.merge(Native.consumerLayer({ brokers: [broker], groupId, autoCommit: false }), Native.transactionLayer({ brokers: [broker], transactionalId: output.topic })))), { signal: controller.signal })
    const timeout = setTimeout(() => controller.abort(), 30000)
    try { await Promise.race([completed, run.then((exit) => { throw new Error(JSON.stringify(exit)) })]) }
    finally { clearTimeout(timeout); controller.abort(); await run }
    const offsets = await input.admin.fetchOffsets({ groupId, topics: [input.topic] })
    expect(offsets[0]?.partitions[0]?.offset).toBe("1")
    expect(await collect(output.topic, 1)).toEqual(["output"])
  } finally { controller.abort(); await input.close(); await output.close() }
})

test("native transactions abort after interruption or a caught send failure", async () => {
  const f = await fixture()
  try {
    const controller = new AbortController()
    let ready!: () => void
    const sent = new Promise<void>((r) => { ready = r })
    const run = Effect.runPromiseExit(Native.Transactions.pipe(Effect.flatMap((transactions) => transactions.withTransaction((tx) => tx.send({ topic: f.topic, messages: [{ value: "interrupted" }] }).pipe(Effect.andThen(Effect.sync(ready)), Effect.andThen(Effect.never)))), Effect.provide(Native.transactionLayer({ brokers: [broker], transactionalId: f.topic }))), { signal: controller.signal })
    const timeout = setTimeout(() => controller.abort(), 30000)
    try { await Promise.race([sent, run.then((exit) => { throw new Error(JSON.stringify(exit)) })]) }
    finally { clearTimeout(timeout); controller.abort(); await run }
    await Effect.runPromise(Native.Transactions.pipe(Effect.flatMap((transactions) => transactions.withTransaction((tx) => Effect.gen(function*() {
      yield* tx.send({ topic: f.topic, messages: [{ value: "poisoned" }] })
      yield* tx.send({ topic: "invalid topic", messages: [{ value: "invalid" }] }).pipe(Effect.ignore)
    })).pipe(Effect.flip)), Effect.provide(Native.transactionLayer({ brokers: [broker], transactionalId: f.topic }))))
    await send(f.topic, ["end"])
    expect(await collect(f.topic, 1)).toEqual(["end"])
  } finally { await f.close() }
})
test("native transactional offsets reject a stale consumer generation", async () => {
  const f = await fixture()
  try {
    await send(f.topic, ["input"])
    const error = await Effect.runPromise(Effect.gen(function*() {
      const c = yield* Consumer, txs = yield* Native.Transactions
      return yield* c.consume({ topics: [f.topic], fromBeginning: true }, (record) => txs.withTransaction((tx) => tx.sendOffsets({ ...record.groupMetadata!, generationId: record.groupMetadata!.generationId + 1 }, [{ topic: record.topic, partition: record.partition, offset: "1" }])))
    }).pipe(Effect.provide(Layer.merge(Native.consumerLayer({ brokers: [broker], groupId: f.topic, autoCommit: false }), Native.transactionLayer({ brokers: [broker], transactionalId: f.topic }))), Effect.flip))
    expect(error.cause).toMatchObject({ api: "TxnOffsetCommit", code: 22 })
    expect((await f.admin.fetchOffsets({ groupId: f.topic, topics: [f.topic] }))[0]?.partitions[0]?.offset).toBe("-1")
  } finally { await f.close() }
})
test("native groups rebalance, interrupt handlers, and fence retained commits", async () => {
  const f = await fixture(2)
  const controller = new AbortController()
  const runs: Array<Promise<unknown>> = []
  let blocked = false, finalized = false
  let stale: ConsumerRecord | undefined
  let entered!: () => void, done!: () => void
  const first = new Promise<void>((r) => { entered = r })
  const completed = new Promise<void>((r) => { done = r })
  const owners = new Map<number, string>()
  const start = () => {
    const run = Effect.runPromiseExit(Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: [f.topic], fromBeginning: true, partitionsConsumedConcurrently: 1 }, (record) => Effect.gen(function*() {
      if (!blocked) {
        blocked = true; stale = record; entered()
        return yield* Effect.never.pipe(Effect.ensuring(Effect.sync(() => { finalized = true })))
      }
      owners.set(record.partition, record.groupMetadata!.memberId)
      yield* record.commit
      if (owners.size === 2) setImmediate(done)
    }))), Effect.provide(Native.consumerLayer({ brokers: [broker], groupId: f.topic, heartbeatIntervalMs: 250 }))), { signal: controller.signal })
    runs.push(run); return run
  }
  const timeout = setTimeout(() => controller.abort(), 30000)
  try {
    await Effect.runPromise(Producer.pipe(Effect.flatMap((p) => p.send({ topic: f.topic, messages: [{ value: "zero", partition: 0 }, { value: "one", partition: 1 }] })), Effect.provide(Native.producerLayer({ brokers: [broker] }))))
    const firstRun = start()
    await Promise.race([first, firstRun.then((e) => { throw new Error(JSON.stringify(e)) })])
    const secondRun = start()
    await Promise.race([completed, firstRun.then((e) => { throw new Error(JSON.stringify(e)) }), secondRun.then((e) => { throw new Error(JSON.stringify(e)) })])
    expect(finalized).toBe(true)
    expect(new Set(owners.values()).size).toBe(2)
    expect((await Effect.runPromise(stale!.commit.pipe(Effect.flip))).cause).toMatchObject({ code: 22 })
  } finally { clearTimeout(timeout); controller.abort(); await Promise.allSettled(runs); await f.close() }
})

test("native consumer reads KafkaJS gzip records, repeated headers and tombstones", async () => {
  const f = await fixture()
  const producer = new KafkaJS.Kafka({ brokers: [broker], logLevel: KafkaJS.logLevel.NOTHING }).producer({ createPartitioner: KafkaJS.Partitioners.DefaultPartitioner })
  await producer.connect()
  try {
    await producer.send({ topic: f.topic, compression: KafkaJS.CompressionTypes.GZIP, messages: [{ key: "k", value: "value", headers: { trace: ["a", "b"] } }, { key: "deleted", value: null }] })
    expect(await collect(f.topic, 2, {}, (record) => Effect.sync(() => {
      if (record.value !== null) expect(record.headers?.trace).toEqual([Buffer.from("a"), Buffer.from("b")])
      else expect(Buffer.from(record.key!).toString()).toBe("deleted")
    }))).toEqual(["value", ""])
  } finally { await producer.disconnect(); await f.close() }
})
test("a second transactional producer fences the earlier producer", async () => {
  const f = await fixture()
  const layer = () => Native.transactionLayer({ brokers: [broker], transactionalId: f.topic })
  try {
    const error = await Effect.runPromise(Native.Transactions.pipe(Effect.flatMap((first) => first.withTransaction((tx) => Effect.gen(function*() {
      yield* tx.send({ topic: f.topic, messages: [{ value: "fenced" }] })
      yield* Native.Transactions.pipe(Effect.flatMap((second) => second.withTransaction((tx) => tx.send({ topic: f.topic, messages: [{ value: "winner" }] }))), Effect.provide(layer()))
      yield* tx.send({ topic: f.topic, messages: [{ value: "rejected" }] })
    }))), Effect.provide(layer()), Effect.flip))
    expect(error.cause).toMatchObject({ _tag: "KafkaBrokerError" })
    expect([47, 90]).toContain((error.cause as Native.KafkaBrokerError).code)
    await send(f.topic, ["end"])
    expect(await collect(f.topic, 2)).toEqual(["winner", "end"])
  } finally { await f.close() }
})


test("concurrent native handlers preserve a KafkaError even while siblings are interrupted", async () => {
  const f = await fixture(2)
  try {
    await Effect.runPromise(Producer.pipe(Effect.flatMap((p) => p.send({ topic: f.topic, messages: [{ value: "wait", partition: 0 }, { value: "fail", partition: 1 }] })), Effect.provide(Native.producerLayer({ brokers: [broker] }))))
    const failure = new KafkaError({ operation: "application", cause: new Native.KafkaBrokerError({ api: "application", code: 27 }) })
    let entered!: () => void
    const processing = new Promise<void>((resolve) => { entered = resolve })
    let finalized = false
    const error = await Effect.runPromise(Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: [f.topic], fromBeginning: true, partitionsConsumedConcurrently: 2 }, (record) => record.partition === 0
      ? Effect.sync(entered).pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sync(() => { finalized = true })))
      : Effect.promise(() => processing).pipe(Effect.andThen(Effect.fail(failure))))), Effect.provide(Native.consumerLayer({ brokers: [broker], groupId: f.topic })), Effect.flip, Effect.timeout("20 seconds")))
    expect(error).toBe(failure)
    expect(finalized).toBe(true)
  } finally { await f.close() }
})
