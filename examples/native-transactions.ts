import { Effect, Layer, Redacted } from "effect"
import { Consumer } from "@effect-kafka/core"
import * as Native from "@effect-kafka/native"

const connection = {
  brokers: ["kafka:9093"],
  tls: {},
  sasl: { mechanism: "scram-sha-256" as const, username: "app", password: Redacted.make(process.env.KAFKA_PASSWORD ?? "") }
}
export const program = Effect.gen(function*() {
  const consumer = yield* Consumer
  const transactions = yield* Native.Transactions
  return yield* consumer.consume({ topics: ["input"] }, (record) =>
    transactions.withTransaction((tx) => Effect.gen(function*() {
      yield* tx.send({ topic: "output", messages: [{ key: record.key, value: record.value }] })
      yield* tx.sendOffsets(record.groupMetadata!, [{
        topic: record.topic, partition: record.partition, offset: (BigInt(record.offset) + 1n).toString()
      }])
    }))
  )
}).pipe(Effect.provide(Layer.merge(
  Native.consumerLayer({ ...connection, groupId: "transformer", autoCommit: false }),
  Native.transactionLayer({ ...connection, transactionalId: "transformer-worker-0", compression: "zstd" })
)))
