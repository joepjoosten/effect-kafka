---
"@effect-kafka/kafkajs": patch
---

Preserve KafkaJS recovery for restartable consumer crashes, and fail the consuming Effect only when the driver reports a terminal crash.
