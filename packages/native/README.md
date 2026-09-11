# @effect-kafka/native

A Kafka protocol producer implemented with Effect and Node's built-in TCP/TLS
modules. Requires **Effect 4.0.0-rc.112** and Node 22+. It has no KafkaJS, Confluent,
or librdkafka dependency.

```sh
pnpm add @effect-kafka/core @effect-kafka/native effect@4.0.0-rc.112
```

```ts
import { Effect } from "effect"
import { Producer } from "@effect-kafka/core"
import * as Native from "@effect-kafka/native"

const program = Producer.pipe(
  Effect.flatMap((producer) => producer.send({
    topic: "events",
    messages: [{ key: "event-1", value: "hello" }]
  })),
  Effect.provide(Native.producerLayer({
    brokers: ["kafka:29092"],
    allowAutoTopicCreation: true
  }))
)

await Effect.runPromise(program)
```

The same `Producer` service accepts the bytes produced by `@effect-avro/kafka`.

## Supported behavior

- ApiVersions v0 capability checks, Metadata v4, and Produce v3.
- Uncompressed record batches (magic 2), CRC32C, and signed varint encoding.
- Strings, binary values, null tombstones, timestamps, keys, and repeated headers.
- Explicit partitions and Java/KafkaJS-compatible Murmur2 keyed partitioning.
  Unkeyed messages use round-robin partitioning.
- Bootstrap broker failover, bounded metadata retries, and routing to advertised leaders.
- TCP and TLS, including custom CAs/client certificates through Node TLS options.
- `acks: -1` (default, all in-sync replicas) and `acks: 1` (leader).
- Bounded request/response sizes, absolute request deadlines, and scoped socket cleanup.

Tested against Apache Kafka 4.0.0. Other brokers must advertise support for the
specific protocol versions above. Unsupported versions fail explicitly.

## Scope and delivery semantics

**This initial native implementation is producer-only.** Use the KafkaJS or
Confluent adapter for consumers. SASL, compression, idempotence, transactions,
admin operations, and unacknowledged (`acks: 0`) sends are not implemented.

Each request owns a socket, closed on success, failure, or interruption. Metadata
is refreshed for every send. Sends sharing one producer service are serialized.
There is no connection pool or background batching: each `send` groups its records
into partition batches. This implementation prioritizes predictable behavior over
high-throughput connection reuse.

Produce requests are never retried automatically. A timeout or interruption may
occur after the broker writes a batch. If a send spans multiple leaders, some
batches may be committed before another fails. Retrying the whole send can create
duplicates. Serial execution does not provide idempotent or exactly-once delivery.

Broker endpoints advertised in metadata must be reachable from your application.
`allowAutoTopicCreation` defaults to false; enabling it also requires broker support.
The default maximum request size is 1 MiB across record batches, and the default
response limit is 16 MiB. Broker-side Produce timeout defaults to 30 seconds;
the absolute deadline for each socket request defaults to 35 seconds.

All operational failures use the shared `KafkaError`. For protocol rejections its
`cause` is a `KafkaBrokerError`, exposing the API, numeric error code, and relevant
topic/partition. Missing or mismatched acknowledgments are failures.

## TLS

```ts
const live = Native.producerLayer({
  brokers: ["broker.example.com:9093"],
  tls: { ca: trustedCaPem }
})
```

Certificate and hostname verification use Node's TLS defaults. `tls` also accepts
client `cert` and `key` for mutual TLS. SASL-authenticated listeners are unsupported.

## Protocol references

The implementation follows Apache Kafka's
[message format](https://kafka.apache.org/40/implementation/message-format/) and
[protocol definitions](https://github.com/apache/kafka/tree/4.0/clients/src/main/resources/common/message).

Unit tests use a localhost-only, self-signed TLS certificate and its public test
private key from `test/fixtures`; these fixtures are not included in npm packages.
