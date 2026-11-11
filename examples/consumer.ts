import { Consumer } from "@effect-kafka/core"
import * as KafkaJS from "@effect-kafka/kafkajs"
import { Effect } from "effect"

export const program = Effect.gen(function*() {
  const consumer = yield* Consumer
  return yield* consumer.consume(
    { topics: ["events"], fromBeginning: true },
    (record) => Effect.log({ topic: record.topic, offset: record.offset, value: record.value })
  )
}).pipe(Effect.provide(KafkaJS.consumerLayer({
  client: { brokers: ["localhost:9092"] },
  consumer: { groupId: "example" }
})))
