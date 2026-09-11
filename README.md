# Effect Kafka

Effect-native Kafka producer and consumer services for **Effect 4.0.0-rc.112**.

| Package | Responsibility |
| --- | --- |
| `@effect-kafka/core` | Shared services, message types, typed errors, and adapter contracts |
| `@effect-kafka/kafkajs` | KafkaJS producer and consumer layers |
| `@effect-kafka/confluent` | Confluent's librdkafka-backed producer and consumer layers |
| `@effect-kafka/native` | Native Kafka protocol producer using Effect and Node TCP/TLS |

Kafka transport is independent of serialization. Use JSON, bytes, Protobuf, or the
`Uint8Array` produced by [`@effect-avro/kafka`](https://github.com/joepjoosten/effect-avro).
This project is separate from the existing `effect-kafka` npm package.

## Install

Choose one adapter. Node 24 is used in CI; Node 22+ is required.

```sh
pnpm add @effect-kafka/core @effect-kafka/kafkajs kafkajs effect@4.0.0-rc.112
# Or:
pnpm add @effect-kafka/core @effect-kafka/confluent @confluentinc/kafka-javascript effect@4.0.0-rc.112
```

The Confluent driver needs native bindings. With pnpm, allow the
`@confluentinc/kafka-javascript` installation script (`pnpm approve-builds`). See
[Confluent's installation requirements](https://docs.confluent.io/kafka-clients/javascript/current/overview.html#installation).

## Produce with KafkaJS

```ts
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

const program = Effect.gen(function*() {
  const producer = yield* Producer
  return yield* producer.send({
    topic: "events",
    messages: [{ key: "event-1", value: "hello" }]
  })
}).pipe(Effect.provide(live))

await Effect.runPromise(program)
```

A KafkaJS or Confluent producer connects when its layer is acquired and disconnects when that scope
closes. Provide the layer around the lifetime of your application to reuse its
connection. `send` accepts strings, `Uint8Array` values, null tombstones, keys,
partitions, timestamps, and repeated headers, and returns delivery reports.

## Consume with KafkaJS

```ts
import { Effect } from "effect"
import { Consumer } from "@effect-kafka/core"
import * as KafkaJS from "@effect-kafka/kafkajs"

const program = Effect.gen(function*() {
  const consumer = yield* Consumer
  return yield* consumer.consume(
    { topics: ["events"], fromBeginning: true },
    (record) => Effect.log({ topic: record.topic, offset: record.offset })
  )
}).pipe(Effect.provide(KafkaJS.consumerLayer({
  client: { brokers: ["localhost:9092"] },
  consumer: { groupId: "my-app" }
})))

const controller = new AbortController()
process.once("SIGINT", () => controller.abort())
process.once("SIGTERM", () => controller.abort())
await Effect.runPromise(program, { signal: controller.signal })
```

Each `consume` call owns a new connection and runs until interrupted or failed.
Handlers may require Effect services and have their own typed errors. Processing
is sequential within each partition; `partitionsConsumedConcurrently` controls
parallelism across partitions.

The driver callback resolves only after its handler succeeds. A handler failure
ends consumption, preserves the original Effect failure, and rejects the driver
callback. Shutdown interrupts active handlers, waits for their finalizers, then
disconnects. KafkaJS crash events also fail the consuming Effect. Confluent handles
broker recovery internally; its start and handler failures are surfaced here.

KafkaJS enables auto-commit by default. For manual commits, set `autoCommit: false`
in its layer options and run `yield* record.commit` after processing. Confluent
uses `"enable.auto.commit": false` in its consumer configuration instead.
`record.commit` commits the next offset using integer-safe arithmetic. Committing
a later offset also commits preceding messages in that partition. A commit that
already succeeded cannot be undone by a subsequent handler failure.

`fromBeginning` applies when the group has no valid committed offset. For long
KafkaJS handlers, call `yield* record.heartbeat` periodically and configure session
timeouts appropriately. Confluent manages heartbeats internally.

## Native producer (no Kafka client dependency)

```sh
pnpm add @effect-kafka/core @effect-kafka/native effect@4.0.0-rc.112
```

Use the same `Producer` service with `Native.producerLayer({ brokers: ["kafka:29092"] })`,
imported from `@effect-kafka/native`. The native package implements the Kafka wire
protocol itself using Effect and Node TCP/TLS. It supports keyed and explicit
partitioning, uncompressed record batches, headers, and tombstones.

This first implementation is producer-only. It uses a scoped socket per request
and serializes sends; it has no connection pool, SASL, compression, transactions,
or idempotent retries. See [native package documentation](packages/native/README.md)
for configuration, supported protocol versions, and delivery semantics.

## Confluent adapter

The same `Producer` and `Consumer` services work with Confluent's native settings:

```ts
import * as Confluent from "@effect-kafka/confluent"

const producerLive = Confluent.producerLayer({
  client: { "bootstrap.servers": "localhost:9092" },
  producer: { "enable.idempotence": true }
})
const consumerLive = Confluent.consumerLayer({
  client: { "bootstrap.servers": "localhost:9092" },
  consumer: { "group.id": "my-app", "enable.auto.commit": false }
})
```

Native client configurations retain their driver types. Confluent's `kafkaJS`
compatibility configuration is also accepted. An explicit subscription
`fromBeginning` overrides the adapter's offset-reset setting.

## Errors and delivery semantics

Driver operations fail with `KafkaError`, containing `operation` and the original
`cause`. Use `Effect.catchTag("KafkaError", ...)` to handle them. Cleanup failures
are defects so they remain visible rather than being silently discarded.

Retries, acknowledgments, and idempotence are configured on the underlying driver.
The wrapper adds no send retries. Interrupting an Effect cannot cancel an already
submitted Kafka send; delivery may still occur. Connect and subscribe wait for
their driver promises before cleanup, so configure finite client timeouts.
Consumers can redeliver messages after failures. Make processing idempotent where
needed. Transactions, admin operations, batch handlers, and exactly-once processing
are outside the initial API.

## Development and releases

```sh
pnpm install
pnpm build
pnpm check
pnpm test
pnpm check:examples
# Start a local broker before this command:
pnpm test:integration
```

CI runs both adapters against Apache Kafka, including binary payloads, repeated
headers, tombstones, and explicit offset commits. Unit tests cover lifecycle,
cancellation, handler errors, backpressure, and adapter conversion.

Use `pnpm changeset` for releasable changes. The four packages version together.
The release workflow opens a Changesets version PR when needed and publishes
unpublished versions on `main` using the GitHub Actions `NPM_TOKEN` secret.
