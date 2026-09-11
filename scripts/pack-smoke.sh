#!/usr/bin/env bash
set -euo pipefail
smoke_dir="$(mktemp -d)"
native_dir="$(mktemp -d)"
trap 'rm -rf "$smoke_dir" "$native_dir"' EXIT
pnpm build
for package in core kafkajs confluent native; do
  (cd "packages/$package" && pnpm pack --pack-destination "$smoke_dir")
done
cd "$smoke_dir"
printf '{"private":true,"type":"module"}\n' > package.json
npm install --no-audit --no-fund "$smoke_dir"/*.tgz effect@4.0.0-rc.112 kafkajs@2.2.4 @confluentinc/kafka-javascript@1.10.1
node --input-type=module <<'JS'
import assert from "node:assert/strict"
import { Effect } from "effect"
import { Producer, Consumer, producerLayer } from "@effect-kafka/core"
import * as KafkaJs from "@effect-kafka/kafkajs"
import * as Confluent from "@effect-kafka/confluent"
assert.equal(typeof KafkaJs.producerLayer, "function")
assert.equal(typeof Confluent.consumerLayer, "function")
assert.ok(Consumer)
let disconnected = false
const reports = await Effect.runPromise(Producer.pipe(
  Effect.flatMap((p) => p.send({ topic: "test", messages: [{ value: "ok" }] })),
  Effect.provide(producerLayer(() => ({
    connect: async () => {}, disconnect: async () => { disconnected = true },
    send: async () => [{ topicName: "test", partition: 0, errorCode: 0 }]
  })))
))
assert.equal(reports.length, 1)
assert.equal(disconnected, true)
console.log("Adapter packages import and execute successfully")
JS

# A separate directory proves the native package does not resolve either Kafka client.
cd "$native_dir"
printf '{"private":true,"type":"module"}\n' > package.json
npm install --no-audit --no-fund "$smoke_dir"/effect-kafka-core-*.tgz "$smoke_dir"/effect-kafka-native-*.tgz effect@4.0.0-rc.112
node --input-type=module <<'JS'
import assert from "node:assert/strict"
import { createRequire } from "node:module"
import { Effect } from "effect"
import { Producer } from "@effect-kafka/core"
import * as Native from "@effect-kafka/native"
const require = createRequire(import.meta.url)
for (const client of ["kafkajs", "@confluentinc/kafka-javascript"]) {
  assert.throws(() => require.resolve(client), { code: "MODULE_NOT_FOUND" })
}
const result = await Effect.runPromise(Producer.pipe(
  Effect.flatMap((producer) => producer.send({ topic: "empty", messages: [] })),
  Effect.provide(Native.producerLayer({ brokers: ["localhost:1"] }))
))
assert.deepEqual(result, [])
console.log("Native package executes without either Kafka client installed")
JS
