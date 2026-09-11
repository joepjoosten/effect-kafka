import { Effect } from "effect"
import { Producer } from "@effect-kafka/core"
import * as Confluent from "@effect-kafka/confluent"

export const program = Producer.pipe(
  Effect.flatMap((producer) => producer.send({ topic: "events", messages: [{ value: "hello" }] })),
  Effect.provide(Confluent.producerLayer({ client: { "bootstrap.servers": "localhost:9092" } }))
)
