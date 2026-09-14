# Native versus KafkaJS producer comparison

From the repository root, with pnpm and Docker Compose installed:

```sh
pnpm install --frozen-lockfile
pnpm benchmark:up
BENCH_BROKER_VERSION=4.0.0 pnpm benchmark
pnpm benchmark:down
```

`benchmark:up` resets only the dedicated `effect-kafka-benchmark` Compose project.
Its topics and data are disposable. Ports 19092 and 19094–19096 must be free.
The fixture pins Apache Kafka 4.0.0, generates a seven-day localhost TLS certificate,
and configures PLAIN and SCRAM-SHA-256 credentials exclusively for this fixture.
The private key and generated results are gitignored under `.benchmark/`.
Do not use the fixture credentials or certificate in another environment.

The benchmark compares both Effect adapters against plaintext, TLS, SASL/PLAIN,
SASL/SCRAM-SHA-256, and both SASL mechanisms over TLS. TLS verifies the generated
CA and hostname. Each measurement owns a unique one-partition topic with replication
factor 1. It verifies the final offset equals the acknowledged record count and
removes the topic. Use a disposable broker with topic creation/deletion permission.

## Measurements

- Cold first acknowledgement includes Layer acquisition and the first send.
- After warmup, sequential single-message sends measure p50/p95/p99 latency.
- Concurrent explicit batches measure messages/s, payload MiB/s and send latency.
  Native serializes these calls; its queue time is included. KafkaJS retains its
  normal concurrency behavior. Both retain one Layer throughout each measurement.
- Modes rotate and adapter order alternates between repetitions. Raw samples,
  environment, source commit/dirty state and settings are saved in `results.json`.
  `complete: false` means a run failed or stopped; never present it as a complete run.

Both use acks=-1, explicit partition 0, identical keys/headers/payloads, no
compression, no automatic send retries and no auto-topic creation. Setup,
verification and teardown are outside steady-state timing. Tracing is enabled
without an exporter by default. KafkaJS persistent connections and native
per-request sockets are intentionally preserved: the comparison measures the
actual migration tradeoff. Native metadata retries are disabled for comparison.

| Environment variable | Default |
| --- | --- |
| `BENCH_SAMPLES` | 100 single sends and 100 batches per measurement |
| `BENCH_REPETITIONS` | 3 |
| `BENCH_WARMUP` | 10 single sends |
| `BENCH_BATCH_SIZE` | 100 records |
| `BENCH_CONCURRENCY` | 8 |
| `BENCH_PAYLOAD_BYTES` | 256 |
| `BENCH_TRACING` | 1; set 0 to disable Effect tracing |
| `BENCH_MODES` | All six modes; comma-separated names from `run.mjs` |
| `BENCH_OUTPUT` | `.benchmark` |
| `BENCH_BROKER_VERSION` | Unknown; set the actual broker version |

External disposable brokers can override `BENCH_ADMIN_BROKER` (plaintext control
plane), `BENCH_PLAINTEXT_BROKER`, `BENCH_TLS_BROKER`, `BENCH_SASL_BROKER`,
`BENCH_SASL_TLS_BROKER`, `BENCH_CA_FILE`, `BENCH_USERNAME`, `BENCH_PASSWORD`.
Broker advertised listeners must be reachable. Credentials are not written to the
report. The fixture uses 4096 SCRAM iterations; record deviations when sharing results.

## Emitted bundles

`pnpm benchmark:bundle` requires no broker. esbuild 0.28.2 emits and minifies Node
24 ESM from equivalent producer entrypoints, preserving Node builtins as externals.
It reports raw and gzip bytes for both fully bundled applications and applications
with Effect externalized. The latter estimates incremental adapter cost when
Effect is already supplied by the application; it excludes Effect's own bytes.
The full bundles are imported, and the native entrypoint also executes an empty
program without contacting a broker. Metafiles identify the included code and
external dependencies. Native bundles are checked for unwanted KafkaJS/Confluent
and former SASLprep dependencies. Gzip size is a transfer metric, not heap usage.

## Reproducibility and limits

CI smoke-tests all six modes with small samples and uploads JSON evidence. The
manual **Producer benchmark** workflow runs the full defaults and uploads samples,
bundle sizes and metafiles. Download artifacts before their retention expires.
Use repeated full runs on an idle machine; small smoke runs do not support p99 or
throughput conclusions. JIT warmup, OS scheduling, TLS session behavior, container
networking, broker storage and GC affect results. This measures uncompressed raw
records, not Avro encoding, consumers, transactions, WAN performance or replicated
durability. Measure those separately with your topology and failure scenarios.
