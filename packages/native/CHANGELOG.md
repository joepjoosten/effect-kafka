# @effect-kafka/native

## 0.4.0

### Minor Changes

- ccc1686: Expose local SASLprep through `SaslPrep.prepare`, `SaslPrep.prepareUnsafe`, and the
  `@effect-kafka/native/SaslPrep` subpath. Use the same implementation for SCRAM and
  remove `@mongodb-js/saslprep` and its transitive dependencies. Include reproducible
  Unicode 3.2 tables and credential-free typed errors. Correct empty mapped strings
  and reject the previously omitted U+FFFFE/U+FFFFF noncharacters.

### Patch Changes

- @effect-kafka/core@0.4.0

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
