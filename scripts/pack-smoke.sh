#!/usr/bin/env bash
set -euo pipefail
smoke_dir="$(mktemp -d)"
trap 'rm -rf "$smoke_dir"' EXIT
pnpm build
for package in core kafkajs confluent; do
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
console.log("All three packed packages import and execute successfully")
JS
