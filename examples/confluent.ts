import * as Confluent from "@effect-kafka/confluent"
import { Producer } from "@effect-kafka/core"
import { Effect } from "effect"

export const program = Producer.pipe(
  Effect.flatMap((producer) => producer.send({ topic: "events", messages: [{ value: "hello" }] })),
  Effect.provide(Confluent.producerLayer({ client: { "bootstrap.servers": "localhost:9092" } }))
)
