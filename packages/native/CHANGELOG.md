# @effect-kafka/native

## 0.3.0

### Minor Changes

- 53d7441: Add native classic consumer groups, SASL PLAIN and SCRAM authentication, gzip and
  Zstandard compression, and scoped Kafka transactions with fenced consumer offset
  commits. Expose optional consumer group metadata in the shared record contract.
  Native requires Node 22.15+ and remains independent of KafkaJS and librdkafka.

### Patch Changes

- Updated dependencies [53d7441]
  - @effect-kafka/core@0.3.0

## 0.2.0

### Minor Changes

- 0e9b464: Add a native Effect Kafka producer with TCP/TLS transport, broker discovery, keyed partitioning, and uncompressed record batches. No Kafka client library is required.

### Patch Changes

- @effect-kafka/core@0.2.0
