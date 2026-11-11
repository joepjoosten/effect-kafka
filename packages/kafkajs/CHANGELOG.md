# @effect-kafka/kafkajs

## 0.6.2

### Patch Changes

- 92fa6d7: Internal: lint with oxlint, format with dprint and type check with `@effect/tsgo`, matching the Effect repository's tooling. Behaviour and public types are unchanged.
- Updated dependencies [92fa6d7]
  - @effect-kafka/core@0.6.2

## 0.6.1

### Patch Changes

- 279f27f: Build and test against `effect@4.1.4`. The `effect` peer range is still `^4.0.0`.
- Updated dependencies [279f27f]
  - @effect-kafka/core@0.6.1

## 0.6.0

### Minor Changes

- 54e015a: Upgrade to the stable Effect 4.0.0 release. The `effect` peer dependency is now `^4.0.0` instead of the pinned `4.0.0-rc.112`.

### Patch Changes

- Updated dependencies [54e015a]
  - @effect-kafka/core@0.6.0

## 0.5.0

### Patch Changes

- @effect-kafka/core@0.5.0

## 0.4.0

### Patch Changes

- @effect-kafka/core@0.4.0

## 0.3.0

### Patch Changes

- Updated dependencies [53d7441]
  - @effect-kafka/core@0.3.0

## 0.2.0

### Patch Changes

- b7743aa: Preserve KafkaJS recovery for restartable consumer crashes, and fail the consuming Effect only when the driver reports a terminal crash.
  - @effect-kafka/core@0.2.0
