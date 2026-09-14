# Reference run: local Kafka 4.1.2

This is one full run on macOS/Apple Silicon with Node 26.7.0, Kafka 4.1.2
running directly on Java 21, and both clients on the same host. It is **not** the
Docker/Node 24 CI environment or a production sizing recommendation. See the
[raw report](local-kafka-4.1.2.json) for the exact CPU, source revision and settings,
and the [harness documentation](../README.md) to reproduce it.

All 36 measurements completed and verified their record counts. Each cell below
is the median of three repetitions; p50 is the median of each repetition's
single-message p50, not a pooled percentile. Throughput uses 100-message batches
with concurrency 8. Each repetition measures 100 single sends and 100 batches,
after 10 warmup sends. Tracing was enabled without an exporter. No failure
injection, replication, Avro encoding or remote network was involved.

| Mode | Native p50 ms | KafkaJS p50 ms | Native messages/s | KafkaJS messages/s |
| --- | ---: | ---: | ---: | ---: |
| plaintext | 2.06 | 0.19 | 32,952 | 158,802 |
| tls | 11.93 | 0.21 | 7,326 | 166,185 |
| sasl_plain | 8.42 | 0.20 | 9,287 | 169,317 |
| sasl_scram256 | 15.36 | 0.18 | 6,396 | 148,296 |
| sasl_tls_plain | 21.42 | 0.21 | 4,157 | 156,032 |
| sasl_tls_scram256 | 26.61 | 0.19 | 3,727 | 158,307 |

Native's per-request connections are a substantial latency and throughput cost in
this workload, especially with TLS/SASL. The smaller dependency footprint does
not imply faster publishing. Persistent connections and version/metadata caching
are clear candidates for future performance work; they are not implemented by
this release. Hardware, network latency and broker topology can change these
numbers substantially.

The equivalent fully bundled producer entrypoints were 128,077 bytes native and
359,060 bytes KafkaJS, minified (44,822 versus 101,843 bytes gzip). With Effect
externalized they were 37,408 and 271,914 bytes (13,210 and 71,566 bytes gzip).
These are emitted entrypoint sizes, not installed package size or memory usage.
[Bundle metadata](local-bundle-sizes.json) records the bundler settings and externals.
