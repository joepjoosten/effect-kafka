import KafkaJS from "kafkajs"
import { Effect } from "effect"
import { expect, test } from "vitest"
import { Consumer, Producer } from "@effect-kafka/core"
import * as KafkaJs from "@effect-kafka/kafkajs"
import * as Confluent from "@effect-kafka/confluent"

const broker = process.env.KAFKA_BROKER ?? "localhost:9092"

for (const adapter of ["kafkajs", "confluent"] as const) {
  test(`${adapter}: sends and consumes bytes, repeated headers, and tombstones`, async () => {
    const topic = `effect-kafka-${adapter}-${Date.now()}`
    const admin = new KafkaJS.Kafka({ brokers: [broker], logLevel: KafkaJS.logLevel.NOTHING }).admin()
    await admin.connect()
    try {
      await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] })
      const producerLive = adapter === "kafkajs"
        ? KafkaJs.producerLayer({ client: { brokers: [broker] }, producer: { createPartitioner: KafkaJs.Partitioners.DefaultPartitioner } })
        : Confluent.producerLayer({ client: { "bootstrap.servers": broker } })
      const consumerLive = adapter === "kafkajs"
        ? KafkaJs.consumerLayer({ client: { brokers: [broker] }, consumer: { groupId: topic }, autoCommit: false })
        : Confluent.consumerLayer({ client: { "bootstrap.servers": broker }, consumer: { "group.id": topic, "enable.auto.commit": false } })
      const reports = await Effect.runPromise(Producer.pipe(Effect.flatMap((p) => p.send({ topic, messages: [
        { key: "one", value: new Uint8Array([99, 1, 2, 99]).subarray(1, 3), headers: { trace: ["a", "b"] } },
        { key: "deleted", value: null }
      ] })), Effect.provide(producerLive)))
      expect(reports.length).toBeGreaterThan(0)
      expect(reports.every((r) => r.errorCode === 0)).toBe(true)
      const seen: Array<{ key: string | null; value: number[] | null }> = []
      const controller = new AbortController()
      let completed!: () => void
      const received = new Promise<void>((resolve) => { completed = resolve })
      const consumption = Effect.runPromiseExit(Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: [topic], fromBeginning: true }, (record) => Effect.gen(function*() {
        seen.push({ key: record.key === null ? null : Buffer.from(record.key).toString(), value: record.value === null ? null : [...record.value] })
        if (seen.length === 1) {
          const values = record.headers?.trace
          expect(Array.isArray(values)).toBe(true)
          expect((values as Array<string | Uint8Array>).map((v) => typeof v === "string" ? v : Buffer.from(v).toString())).toEqual(["a", "b"])
        }
        yield* record.commit
        if (seen.length === 2) setImmediate(completed)
      }))), Effect.provide(consumerLive)), { signal: controller.signal })
      const timeout = setTimeout(() => controller.abort(), 30_000)
      try {
        await Promise.race([received, consumption.then(() => { throw new Error("Consumer ended before receiving both records") })])
      } finally {
        clearTimeout(timeout)
        controller.abort()
        await consumption
      }
      expect(seen).toEqual([{ key: "one", value: [1, 2] }, { key: "deleted", value: null }])
    } finally {
      await admin.deleteTopics({ topics: [topic] })
      await admin.disconnect()
    }
  }, 60_000)
}
