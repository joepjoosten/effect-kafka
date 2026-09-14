import KafkaJS from "kafkajs"
import { Effect } from "effect"
import { Producer } from "../packages/core/dist/index.js"
import * as Native from "../packages/native/dist/index.js"
import * as KafkaJs from "../packages/kafkajs/dist/index.js"
import { mkdir, readFile, writeFile } from "node:fs/promises"
import { execFileSync } from "node:child_process"
import { cpus, platform, release, totalmem } from "node:os"
import { resolve } from "node:path"
import { performance } from "node:perf_hooks"
import { randomUUID } from "node:crypto"

const number = (name, fallback, min, max) => {
  const value = Number(process.env[name] ?? fallback)
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`Invalid ${name}`)
  return value
}
const config = {
  samples: number("BENCH_SAMPLES", 100, 2, 100000),
  repetitions: number("BENCH_REPETITIONS", 3, 1, 100),
  warmup: number("BENCH_WARMUP", 10, 0, 10000),
  batchSize: number("BENCH_BATCH_SIZE", 100, 1, 1000),
  concurrency: number("BENCH_CONCURRENCY", 8, 1, 128),
  payloadBytes: number("BENCH_PAYLOAD_BYTES", 256, 1, 65536),
  tracingEnabled: process.env.BENCH_TRACING !== "0",
  acks: -1, compression: "none", sendRetries: 0, partition: 0,
  timeoutMs: 30000, autoTopicCreation: false
}
if (config.payloadBytes * config.batchSize > 800000) throw new Error("Batch payload exceeds the shared 1 MiB request budget")
const variants = {
  plaintext: { broker: process.env.BENCH_PLAINTEXT_BROKER ?? "localhost:19092" },
  tls: { broker: process.env.BENCH_TLS_BROKER ?? "localhost:19094", tls: true },
  sasl_plain: { broker: process.env.BENCH_SASL_BROKER ?? "localhost:19095", mechanism: "plain" },
  sasl_scram256: { broker: process.env.BENCH_SASL_BROKER ?? "localhost:19095", mechanism: "scram-sha-256" },
  sasl_tls_plain: { broker: process.env.BENCH_SASL_TLS_BROKER ?? "localhost:19096", tls: true, mechanism: "plain" },
  sasl_tls_scram256: { broker: process.env.BENCH_SASL_TLS_BROKER ?? "localhost:19096", tls: true, mechanism: "scram-sha-256" }
}
const modes = (process.env.BENCH_MODES ?? Object.keys(variants).join(",")).split(",")
if (!modes.length || new Set(modes).size !== modes.length || modes.some((m) => !variants[m])) throw new Error("Invalid BENCH_MODES")
const ca = modes.some((m) => variants[m].tls) ? await readFile(process.env.BENCH_CA_FILE ?? ".benchmark/certs/ca.pem") : undefined
const output = resolve(process.env.BENCH_OUTPUT ?? ".benchmark")
await mkdir(output, { recursive: true })
const pkg = async (name) => JSON.parse(await readFile(`packages/${name}/package.json`, "utf8")).version
const git = (...args) => execFileSync("git", args, { encoding: "utf8" }).trim()
const report = {
  schemaVersion: 1, complete: false, startedAt: new Date().toISOString(),
  environment: {
    node: process.version, openssl: process.versions.openssl, os: platform(), osRelease: release(),
    architecture: process.arch, cpu: cpus()[0]?.model, logicalCpus: cpus().length, memoryBytes: totalmem(),
    gitCommit: git("rev-parse", "HEAD"), gitDirty: git("status", "--porcelain").length > 0,
    nativeVersion: await pkg("native"), adapterVersion: await pkg("kafkajs"),
    effectVersion: JSON.parse(await readFile("node_modules/effect/package.json", "utf8")).version,
    kafkaJsVersion: JSON.parse(await readFile("node_modules/kafkajs/package.json", "utf8")).version,
    brokerVersionLabel: process.env.BENCH_BROKER_VERSION ?? "unknown; set BENCH_BROKER_VERSION",
    brokerTopology: "one partition per topic; replication factor 1; verify against your target topology"
  },
  config, modes, results: []
}
const persist = () => writeFile(resolve(output, "results.json"), JSON.stringify(report, null, 2) + "\n")
const admin = new KafkaJS.Kafka({ brokers: [process.env.BENCH_ADMIN_BROKER ?? variants.plaintext.broker], logLevel: KafkaJS.logLevel.NOTHING }).admin()
const delay = (ms) => new Promise((r) => setTimeout(r, ms))
const summarize = (values) => {
  const sorted = [...values].sort((a, b) => a - b)
  const percentile = (p) => sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)]
  return { count: sorted.length, minMs: sorted[0], meanMs: values.reduce((a, b) => a + b, 0) / values.length,
    p50Ms: percentile(0.5), p95Ms: percentile(0.95), p99Ms: percentile(0.99), maxMs: sorted.at(-1) }
}
async function createTopic(topic) {
  await admin.createTopics({ waitForLeaders: false, topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] })
  for (let i = 0; i < 100; i++) {
    try { if ((await admin.fetchTopicOffsets(topic)).length === 1) return }
    catch (error) { if (![3, 5, 6].includes(error.code)) throw error }
    await delay(100)
  }
  throw new Error("Benchmark topic did not become ready")
}
const value = Buffer.alloc(config.payloadBytes, 0x78)
const headers = { traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01" }
async function measure(mode, adapter, repetition) {
  const variant = variants[mode]
  const tls = variant.tls ? { ca } : undefined
  const sasl = variant.mechanism ? { mechanism: variant.mechanism, username: process.env.BENCH_USERNAME ?? "benchmark", password: process.env.BENCH_PASSWORD ?? "benchmark-secret" } : undefined
  const topic = "effect-kafka-bench-" + randomUUID()
  let expected = 0
  try {
    await createTopic(topic)
    const live = adapter === "native"
      ? Native.producerLayer({ brokers: [variant.broker], tls, sasl, acks: -1, compression: "none", metadataRetries: 0, allowAutoTopicCreation: false, requestTimeoutMs: config.timeoutMs, produceTimeoutMs: config.timeoutMs })
      : KafkaJs.producerLayer({ client: { brokers: [variant.broker], ssl: tls, sasl, logLevel: KafkaJS.logLevel.NOTHING, retry: { retries: 0 }, requestTimeout: config.timeoutMs }, producer: { retry: { retries: 0 }, allowAutoTopicCreation: false, idempotent: false, createPartitioner: KafkaJs.Partitioners.DefaultPartitioner } })
    const started = performance.now()
    const result = await Effect.runPromise(Effect.gen(function*() {
      const producer = yield* Producer
      const acquiredAt = performance.now()
      const send = (count) => Effect.gen(function*() {
        const messages = Array.from({ length: count }, () => ({ value, key: "key", partition: 0, headers }))
        const start = performance.now()
        const acknowledgements = yield* producer.send({ topic, messages })
        if (acknowledgements.length !== 1 || acknowledgements[0].errorCode !== 0) throw new Error("Invalid producer acknowledgement")
        expected += count
        return performance.now() - start
      })
      yield* send(1)
      const coldFirstAcknowledgementMs = performance.now() - started
      for (let i = 0; i < config.warmup; i++) yield* send(1)
      const latencySamplesMs = []
      for (let i = 0; i < config.samples; i++) latencySamplesMs.push(yield* send(1))
      const throughputSamplesMs = []
      const throughputStarted = performance.now()
      yield* Effect.forEach(Array.from({ length: config.samples }), () => send(config.batchSize).pipe(Effect.tap((ms) => Effect.sync(() => { throughputSamplesMs.push(ms) }))), { concurrency: config.concurrency, discard: true })
      const throughputElapsedMs = performance.now() - throughputStarted
      return { layerAcquisitionMs: acquiredAt - started, coldFirstAcknowledgementMs,
        latency: summarize(latencySamplesMs), latencySamplesMs,
        throughput: { messagesPerSecond: config.samples * config.batchSize / (throughputElapsedMs / 1000),
          payloadMiBPerSecond: config.samples * config.batchSize * config.payloadBytes / (1024 * 1024) / (throughputElapsedMs / 1000),
          elapsedMs: throughputElapsedMs, sendLatency: summarize(throughputSamplesMs) }, throughputSamplesMs }
    }).pipe(Effect.provide(live), Effect.withTracerEnabled(config.tracingEnabled)))
    // Offsets are read after timing and scope cleanup; never include verification in throughput.
    const offsets = await admin.fetchTopicOffsets(topic)
    if (offsets.length !== 1 || BigInt(offsets[0].offset) !== BigInt(expected)) throw new Error(`Record count mismatch: expected ${expected}`)
    return { mode, adapter, repetition, verifiedRecords: expected, ...result }
  } finally { await admin.deleteTopics({ topics: [topic] }) }
}
await persist()
try {
  await admin.connect()
  for (let repetition = 0; repetition < config.repetitions; repetition++) {
    // Rotate security modes and alternate client order to reduce fixed-order bias.
    const orderedModes = [...modes.slice(repetition % modes.length), ...modes.slice(0, repetition % modes.length)]
    for (const mode of orderedModes) {
      for (const adapter of repetition % 2 ? ["kafkajs", "native"] : ["native", "kafkajs"]) {
        const result = await measure(mode, adapter, repetition)
        report.results.push(result); await persist()
        console.log(`${mode} ${adapter} repeat=${repetition + 1}: p50=${result.latency.p50Ms.toFixed(2)}ms p95=${result.latency.p95Ms.toFixed(2)}ms throughput=${result.throughput.messagesPerSecond.toFixed(0)} messages/s`)
      }
    }
  }
  report.complete = true
  report.finishedAt = new Date().toISOString()
} finally {
  await persist()
  await admin.disconnect()
}
