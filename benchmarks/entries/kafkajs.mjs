import { Effect } from "effect"
import { Producer } from "../../packages/core/dist/index.js"
import { producerLayer, Partitioners } from "../../packages/kafkajs/dist/index.js"
export const run = (brokers, records) => Effect.runPromise(Effect.gen(function*() {
  const producer = yield* Producer
  for (const record of records) yield* producer.send(record)
}).pipe(Effect.provide(producerLayer({ client: { brokers }, producer: { createPartitioner: Partitioners.DefaultPartitioner } }))))
