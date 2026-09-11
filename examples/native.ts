import { Effect } from "effect"
import { Producer } from "@effect-kafka/core"
import * as Native from "@effect-kafka/native"

export const program = Producer.pipe(
  Effect.flatMap((producer) => producer.send({ topic: "events", messages: [{ key: "1", value: "hello" }] })),
  Effect.provide(Native.producerLayer({ brokers: ["kafka:29092"], allowAutoTopicCreation: true }))
)
