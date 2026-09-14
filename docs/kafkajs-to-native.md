# Migrating a KafkaJS producer to native

The shared `Producer` service accepts the same encoded bytes, keys, and headers.
Replacing the Layer can preserve application behavior while changing connection,
partitioning, and delivery behavior. Application tests alone cannot validate those
operational differences against a broker.

```ts
import { Effect } from "effect"
import { Producer } from "@effect-kafka/core"
import * as Native from "@effect-kafka/native"

const live = Native.producerLayer({
  brokers: ["kafka:29092"],
  allowAutoTopicCreation: true
})
const publish = Producer.pipe(
  Effect.flatMap((producer) => producer.send({
    topic: "events",
    messages: [{ key: "event-123", value: "already encoded bytes also work" }]
  })),
  Effect.withSpan("application.publish"),
  Effect.provide(live)
)
```

Existing Avro encoding and trace headers need no wire-format changes. Native spans
use the application's Effect tracer; they do not inject propagation headers.
Requires Node 22.15+ and Effect 4.0.0-rc.112.

## Configuration and lifecycle

| KafkaJS | Native |
| --- | --- |
| `client.brokers` | `brokers` |
| `client.ssl` | `tls` (Node TLS options) |
| `client.sasl` | `sasl`: PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512 |
| `producer.allowAutoTopicCreation`, default true | `allowAutoTopicCreation`, default **false** |
| Default keyed partitioner | Java-compatible Murmur2 for keys; explicit partitions preserved |
| Default unkeyed partitioning | Native uses round-robin; reassess ordering assumptions |
| Send `acks`, default -1 | Layer `acks`, -1 or 1; no acks=0 |
| Compression codecs | Native supports none, gzip and zstd |
| Persistent producer connections | A new socket for each native request |
| Configurable send retries/idempotence | No automatic ordinary Produce retries or idempotent producer option |
| Eager adapter connection during Layer acquisition | Native Layer validates configuration; broker contact starts with a nonempty send |

Reusing a native Layer shares the producer's serialization and partition state;
it does not create a persistent connection pool. An empty send is not a readiness
probe. Validate readiness with a real operation against the intended broker.

## Connection cost

A normal nonempty send to one leader opens **four sockets**: ApiVersions, Metadata,
ApiVersions, then Produce. With L distinct leaders it opens **2 + 2L** sockets.
This describes the successful ordinary producer path; discovery retries and
transaction coordination add requests. Versions and metadata are not cached.

Each socket repeats TCP establishment and, when configured, TLS negotiation and
SASL authentication. PLAIN adds two Kafka exchanges per socket (handshake and
authentication); SCRAM adds three and a password derivation. A one-leader send
therefore normally entails 12 Kafka exchanges with PLAIN or 16 with SCRAM, plus
connection establishment. Authentication is on the same socket as its API request.

KafkaJS can amortize these costs across persistent connections. Native latency
will depend strongly on network RTT, TLS negotiation, SCRAM iterations, broker
load, and batch size. There is no universal millisecond overhead: use the
[reproducible comparison](../benchmarks/README.md) on your deployment topology.
Request timeouts apply to individual exchanges, not the entire send deadline.

## Delivery and backpressure

Sends are serialized per native producer, including metadata discovery. Increasing
Effect concurrency on that service creates waiting work; it does not create
parallel Produce requests. Use bounded concurrency and explicit message batches
to amortize connection costs. Separate producer instances can run independently,
but increase connections and do not preserve ordering across instances.

Native does not automatically retry sends. Metadata discovery retries do not
constitute a delivery retry policy. A successful acknowledgement confirms the
configured broker acknowledgement level. A failed or interrupted send does not
necessarily mean that nothing was written:

- A connection can fail after the broker appends records but before the response
  arrives. A timeout cannot resolve whether delivery happened.
- A send spanning multiple leaders can partially succeed before another fails.
- Interrupting an Effect cannot retract an already transmitted request.

Do not apply an unconditional `Effect.retry` and assume exactly-once delivery.
For retryable business events, assign a stable event ID before the first attempt,
persist pending work (for example in an outbox), and make downstream handling
idempotent. Kafka message keys select partitions; they do **not** deduplicate
records. Retrying an uncertain send can produce duplicates. Preserve enough
application context to reconcile an uncertain result without logging payloads or
credentials. Failures known to precede transmission can be handled separately,
but a broad transport error category alone does not establish that fact.

Native transactions coordinate Kafka records and consumed offsets. They do not
atomically include database writes or other external side effects. Losing an
EndTxn response can also leave an uncertain result; reconcile/fence using your
transactional workflow rather than blindly replaying external effects.

See KafkaJS's [producer](https://kafka.js.org/docs/producing) and
[transaction](https://kafka.js.org/docs/transactions) documentation when comparing
existing retry and transaction settings.

## Tracing latency and failures

These spans use the existing Effect tracer, without a new runtime dependency:

| Span | Coverage |
| --- | --- |
| `kafka.native.send` | Entire send, including semaphore queue time |
| `kafka.native.metadata` | Discovery, including bootstrap attempts |
| `kafka.native.request` | One API exchange, including connection/authentication and correlation validation |
| `kafka.native.authenticate` | SASL handshake and authentication, including initial TCP/TLS establishment |
| `kafka.native.produce` | Produce exchange and decoded broker acknowledgement validation |

A typical tree under `application.publish` is send → metadata → request →
authenticate, followed by send → request (ApiVersions), then send → produce →
request → authenticate. Without SASL there is no authentication span. Authentication
is not a pure password-derivation timer; its first exchange establishes the socket.
A request can finish successfully while Produce fails with a broker error; inspect
the semantic Produce/send span as well as the network exchange.

Attributes identify the topic, broker, API key/version, TLS use, SASL mechanism,
acks, compression and counts where relevant. Payloads, message keys, headers,
usernames and passwords are not added as attributes. Effect retains normal
success/error values; custom tracers remain responsible for safe export. Topics
and broker addresses can be sensitive or high-cardinality: configure filtering and
sampling in your exporter. `Effect.withTracerEnabled(false)` disables these spans
for an Effect. Configure your application's tracer/exporter as usual.

## Evidence and deployment evaluation

The reported 524 passing application tests establish compatibility for that
application. They are not this repository's test count or proof of production
readiness. Repository broker integration tests and benchmark results provide
additional, bounded evidence. The benchmark uses a single broker and cannot
establish replicated durability or outage behavior. Before deployment, exercise
your actual TLS/SASL configuration, replication settings, network latency, broker
failover, cancellation and uncertain-delivery recovery with representative load.
