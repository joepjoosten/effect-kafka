import { Effect } from "effect"
import { Producer } from "@effect-kafka/core"
import * as KafkaJS from "@effect-kafka/kafkajs"

const live = KafkaJS.producerLayer({
  client: { brokers: ["kafka:29092"] },
  producer: {
    allowAutoTopicCreation: true,
    createPartitioner: KafkaJS.Partitioners.DefaultPartitioner
  }
})

// Pass the Uint8Array returned by @effect-avro/kafka as value to send Avro.
export const program = Effect.gen(function*() {
  const producer = yield* Producer
  return yield* producer.send({ topic: "events", messages: [{ key: "1", value: "hello" }] })
}).pipe(Effect.provide(live))
