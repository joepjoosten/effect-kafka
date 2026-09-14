# @effect-kafka/native

Kafka producers, consumers and transactions implemented with Effect and Node
TCP/TLS. Requires **Effect 4.0.0-rc.112** and **Node 22.15+**. No KafkaJS,
Confluent or librdkafka dependency; SASLprep uses `@mongodb-js/saslprep`.

```sh
pnpm add @effect-kafka/core @effect-kafka/native effect@4.0.0-rc.112
```

## Produce

```ts
import { Effect } from "effect"
import { Consumer, Producer } from "@effect-kafka/core"
import * as Native from "@effect-kafka/native"

const program = Producer.pipe(
  Effect.flatMap((producer) => producer.send({
    topic: "events",
    messages: [{ key: "event-1", value: "hello" }]
  })),
  Effect.provide(Native.producerLayer({
    brokers: ["kafka:29092"],
    compression: "gzip",
    allowAutoTopicCreation: true
  }))
)
await Effect.runPromise(program)
```

Supports strings, binary values (including `@effect-avro/kafka` output), null
tombstones, timestamps, repeated headers, explicit partitions, and Java-compatible
Murmur2 keyed partitioning. Unkeyed messages use round-robin partitioning.
`acks` is `-1` by default or `1`. Compression is `none` (default), `gzip`, or
`zstd`. Consumers decode these three codecs and reject unsupported codecs.

## Consume

```ts
const program = Consumer.pipe(
  Effect.flatMap((consumer) => consumer.consume(
    { topics: ["events"], fromBeginning: true, partitionsConsumedConcurrently: 2 },
    (record) => Effect.log({ offset: record.offset, value: record.value })
  )),
  Effect.provide(Native.consumerLayer({
    brokers: ["kafka:29092"],
    groupId: "my-app"
  }))
)
```

Each invocation joins a classic consumer group with the `range` assignor and
leaves when interrupted. Use explicit topic names; regular-expression
subscriptions are rejected. Partition processing is ordered; parallelism applies
across assigned partitions. Group rebalances interrupt processing and wait for
handler finalizers before rejoining. Heartbeats run independently of handlers.
A CPU-blocking handler can still prevent Node from sending heartbeats.

Successful handlers commit the next offset by default. Set `autoCommit: false`
and use `yield* record.commit` for manual commits, or use a transaction below.
Failures preserve the handler's original error and do not auto-commit it.
An explicit commit that already succeeded cannot be undone by a later failure.
Committed offsets take precedence over `fromBeginning`, which selects earliest
or latest when no offset is stored. Out-of-range committed offsets fail explicitly.
Retained record callbacks reject commits after their group generation ends.

`isolationLevel` defaults to `read_committed`: aborted transaction records and
control batches are hidden. `read_uncommitted` includes aborted data records.
The default session timeout is 30 seconds, heartbeat interval 3 seconds, and
rebalance timeout 60 seconds. Configure `requestTimeoutMs` longer than the
rebalance timeout; its consumer default is 65 seconds. Topic partition changes
are discovered when the group next rebalances.

## SASL and TLS

All three layers accept the same connection configuration:

```ts
import { Redacted } from "effect"

const live = Native.producerLayer({
  brokers: ["broker.example.com:9093"],
  tls: { ca: trustedCaPem },
  sasl: {
    mechanism: "scram-sha-256",
    username: "app",
    password: Redacted.make(password)
  }
})
```

Mechanisms: `plain`, `scram-sha-256`, `scram-sha-512`. Passwords may be strings
or Effect `Redacted` values. Authentication runs on the same socket as each Kafka
request. SCRAM validates the nonce and server signature; authentication errors
omit credentials and server-provided error text. Use TLS for authenticated
connections. Node's default certificate and hostname checks apply; custom CAs
and mutual TLS client `cert`/`key` are supported.

## Transactions

```ts
const program = Native.Transactions.pipe(
  Effect.flatMap((transactions) => transactions.withTransaction((tx) =>
    tx.send({ topic: "events", messages: [{ value: "atomic" }] })
  )),
  Effect.provide(Native.transactionLayer({
    brokers: ["kafka:29092"],
    transactionalId: "my-app-worker-0",
    compression: "zstd"
  }))
)
```

`withTransaction` commits on success and aborts on failure or interruption.
Use a stable, unique transactional ID per concurrently running worker; another
producer using the same ID fences the earlier producer. Transactions within one
layer are serialized. Each transaction initializes its producer identity before
use. Transactional sends require `acks: -1`, carry per-partition sequences, and
are not retried automatically. Catching a failed send inside the callback cannot
turn that transaction into a commit. Using `tx` after the callback ends fails.

For consume-transform-produce, set the native consumer's `autoCommit: false` and
run both operations within one transaction:

```ts
consumer.consume({ topics: ["input"] }, (record) =>
  transactions.withTransaction((tx) => Effect.gen(function*() {
    yield* tx.send({ topic: "output", messages: [{ value: record.value }] })
    yield* tx.sendOffsets(record.groupMetadata!, [{
      topic: record.topic,
      partition: record.partition,
      offset: (BigInt(record.offset) + 1n).toString()
    }])
  }))
)
```

`sendOffsets` requires active native group metadata, including generation and
member ID, so Kafka can reject stale consumers. Offsets are **next offsets**.
Do not also call `record.commit` in this flow. Consumers of the output must use
`read_committed`. Kafka transactions cover Kafka records and offsets, not external
databases or other side effects. Keep transaction work within
`transactionTimeoutMs` (default 60 seconds); the callback deadline leaves 500 ms
for starting cleanup. Cleanup itself uses bounded request timeouts.

If an EndTxn response is lost, the outcome is uncertain and the Effect fails;
this implementation cannot promise rollback after a commit reached Kafka.
A later transaction initialization fences the prior producer epoch. Do not
blindly replay side effects after an uncertain result.

## Operational scope

Each request owns a scoped socket; SASL authentication precedes the request on
that socket. There is no connection pool or background producer batching.
Metadata is refreshed for each send and consumer poll. This favors simple
resource ownership over high throughput. Ordinary sends are serialized and
never retried automatically: a timeout or interruption can happen after delivery,
and a multi-leader send can partially succeed. Retrying can produce duplicates.
Consumers recover coordinator, leader, transport, and rebalance failures and
can redeliver records; processing should tolerate redelivery.

Defaults: maximum request 1 MiB, response 16 MiB, Produce timeout 30 seconds,
producer request deadline 35 seconds per authentication/request stage.
Decompression is bounded across each fetch. Broker-advertised endpoints must
be reachable. Topic auto-creation is disabled by default.

Operational failures use `KafkaError`. Broker rejections preserve a
`KafkaBrokerError` cause with API and numeric code. A best-effort LeaveGroup
failure is ignored during shutdown; the session timeout releases membership.

Not implemented: Snappy/LZ4, OAuth/Kerberos, static membership, the newer consumer
group protocol, regex subscriptions, admin operations, and automatic idempotent
send retries. Other adapters remain available for those requirements.

## Protocol references

The client checks advertised API versions before use. It uses Metadata v4,
Produce v3/v7, Fetch v11, JoinGroup v5, SyncGroup v3, Heartbeat v3, LeaveGroup v1,
OffsetFetch v4, OffsetCommit v2, ListOffsets v2, FindCoordinator v1, transaction
APIs v0, and TxnOffsetCommit v3 (flexible encoding with group fencing).

See Apache Kafka's [message format](https://kafka.apache.org/40/implementation/message-format/)
and [protocol definitions](https://github.com/apache/kafka/tree/4.0/clients/src/main/resources/common/message).
Protocol layout attribution is in `NOTICE` and `LICENSE-APACHE`.
Test TLS keys are localhost fixtures and are excluded from npm packages.
