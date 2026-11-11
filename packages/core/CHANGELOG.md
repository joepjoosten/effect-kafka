# @effect-kafka/core

## 0.6.2

### Patch Changes

- 92fa6d7: Internal: lint with oxlint, format with dprint and type check with `@effect/tsgo`, matching the Effect repository's tooling. Behaviour and public types are unchanged.

## 0.6.1

### Patch Changes

- 279f27f: Build and test against `effect@4.1.4`. The `effect` peer range is still `^4.0.0`.

## 0.6.0

### Minor Changes

- 54e015a: Upgrade to the stable Effect 4.0.0 release. The `effect` peer dependency is now `^4.0.0` instead of the pinned `4.0.0-rc.112`.

## 0.5.0

## 0.4.0

## 0.3.0

### Minor Changes

- 53d7441: Add native classic consumer groups, SASL PLAIN and SCRAM authentication, gzip and
  Zstandard compression, and scoped Kafka transactions with fenced consumer offset
  commits. Expose optional consumer group metadata in the shared record contract.
  Native requires Node 22.15+ and remains independent of KafkaJS and librdkafka.

## 0.2.0
