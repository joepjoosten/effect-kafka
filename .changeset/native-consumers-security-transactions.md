---
"@effect-kafka/native": minor
"@effect-kafka/core": minor
---

Add native classic consumer groups, SASL PLAIN and SCRAM authentication, gzip and
Zstandard compression, and scoped Kafka transactions with fenced consumer offset
commits. Expose optional consumer group metadata in the shared record contract.
Native requires Node 22.15+ and remains independent of KafkaJS and librdkafka.
