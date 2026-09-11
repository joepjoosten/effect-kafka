import { Effect, Layer, Semaphore } from "effect"
import { KafkaError, Producer, type DeliveryReport, type Message, type ProducerRecord } from "@effect-kafka/core"
import { bytes, murmur2, recordBatch } from "./internal/records.js"
import { endpoint, exchange, type Endpoint, type TransportOptions } from "./internal/transport.js"
import { checkVersions, frameRequest, KafkaBrokerError, metadataRequest, metadataResponse, produceRequest, produceResponse, responseBody, type Metadata } from "./internal/protocol.js"

export { KafkaBrokerError } from "./internal/protocol.js"

export interface ProducerOptions {
  readonly brokers: ReadonlyArray<string>
  readonly clientId?: string
  readonly tls?: TransportOptions["tls"]
  /** Defaults to all in-sync replicas (-1). No unacknowledged sends. */
  readonly acks?: 1 | -1
  /** Broker-side Produce deadline. Defaults to 30 seconds. */
  readonly produceTimeoutMs?: number
  /** Absolute deadline per TCP/TLS request. Defaults to 35 seconds. */
  readonly requestTimeoutMs?: number
  /** Defaults to 1 MiB, across all record batches in a send. */
  readonly maxRequestBytes?: number
  /** Defaults to 16 MiB. Incoming frame sizes are checked before allocation. */
  readonly maxResponseBytes?: number
  /** Defaults to false. Broker configuration must also allow topic creation. */
  readonly allowAutoTopicCreation?: boolean
  /** Additional metadata rounds across bootstrap brokers. Defaults to 2. Never retries Produce. */
  readonly metadataRetries?: number
}
const checked = <A>(operation: string, evaluate: () => A): Effect.Effect<A, KafkaError> =>
  Effect.try({ try: evaluate, catch: (cause) => new KafkaError({ operation, cause }) })
const integer = (value: number, name: string, min: number, max: number): number => {
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${name} must be an integer between ${min} and ${max}`)
  return value
}

/** Native Kafka producer. Each request owns a scoped Node TCP/TLS socket. */
export const producerLayer = (options: ProducerOptions): Layer.Layer<Producer, KafkaError> => Layer.effect(Producer,
  Effect.gen(function*() {
    const config = yield* checked("native.config", () => {
      if (!Array.isArray(options.brokers) || options.brokers.length === 0) throw new Error("At least one bootstrap broker is required")
      if ("sasl" in options || "compression" in options || "idempotent" in options || "transactionalId" in options) throw new Error("SASL, compression, idempotence, and transactions are not supported by the native producer")
      const clientId = options.clientId ?? "effect-kafka-native"
      if (Buffer.byteLength(clientId) > 32767) throw new Error("clientId exceeds Kafka string limit")
      const acks = options.acks ?? -1
      if (acks !== 1 && acks !== -1) throw new Error("acks must be 1 or -1")
      const produceTimeoutMs = integer(options.produceTimeoutMs ?? 30000, "produceTimeoutMs", 1, 2147483647)
      const transport: TransportOptions = {
        requestTimeoutMs: integer(options.requestTimeoutMs ?? 35000, "requestTimeoutMs", 1, 2147483647),
        maxResponseBytes: integer(options.maxResponseBytes ?? 16 * 1024 * 1024, "maxResponseBytes", 4, 256 * 1024 * 1024),
        ...(options.tls === undefined ? {} : { tls: { ...options.tls } })
      }
      return {
        brokers: options.brokers.map(endpoint), clientId, acks, produceTimeoutMs, transport,
        maxRequestBytes: integer(options.maxRequestBytes ?? 1024 * 1024, "maxRequestBytes", 128, 256 * 1024 * 1024),
        metadataRetries: integer(options.metadataRetries ?? 2, "metadataRetries", 0, 10),
        autoCreate: options.allowAutoTopicCreation ?? false
      }
    })
    const semaphore = yield* Semaphore.make(1)
    let correlation = 0
    let roundRobin = 0
    const request = (address: Endpoint, api: number, version: number, body: Buffer) => Effect.gen(function*() {
      const id = correlation = (correlation + 1) & 0x7fffffff
      const frame = yield* checked("native.encode", () => frameRequest(api, version, id, config.clientId, body, config.maxRequestBytes))
      const response = yield* exchange(address, frame, config.transport)
      return yield* checked("native.decode", () => responseBody(response, id))
    })
    const negotiate = (address: Endpoint, api: number, version: number) => Effect.gen(function*() {
      const body = yield* request(address, 18, 0, Buffer.alloc(0))
      yield* checked("native.versions", () => checkVersions(body, api, version))
    })
    const discover = (topic: string) => Effect.gen(function*() {
      let last: KafkaError | undefined
      for (let attempt = 0; attempt <= config.metadataRetries; attempt++) {
        for (const broker of config.brokers) {
          const result: Metadata | KafkaError = yield* Effect.gen(function*() {
            yield* negotiate(broker, 3, 4)
            const body = yield* request(broker, 3, 4, metadataRequest(topic, config.autoCreate))
            return yield* checked("native.metadata", () => {
              const metadata = metadataResponse(body, topic)
              const unavailable = metadata.partitions.find((p) => p.error !== 0 || p.leader < 0)
              if (unavailable) throw new KafkaBrokerError({ api: "Metadata", code: unavailable.error || 5, topic, partition: unavailable.id })
              return metadata
            })
          }).pipe(Effect.catchTag("KafkaError", (error) => Effect.succeed(error)))
          if (!(result instanceof KafkaError)) return result
          last = result
          if (result.cause instanceof KafkaBrokerError && ![3, 5, 6].includes(result.cause.code)) return yield* Effect.fail(result)
        }
        if (attempt < config.metadataRetries) yield* Effect.sleep(100 * (attempt + 1))
      }
      return yield* Effect.fail(last!)
    })
    return Producer.of({ send: (record: ProducerRecord) => semaphore.withPermits(1)(Effect.gen(function*() {
      yield* checked("native.validate", () => {
        if (typeof record.topic !== "string" || !/^[a-zA-Z0-9._-]{1,249}$/.test(record.topic) || record.topic === "." || record.topic === "..") throw new Error("Invalid Kafka topic name")
        if (record.messages.length > config.maxRequestBytes) throw new Error("Too many messages")
      })
      if (record.messages.length === 0) return []
      const metadata = yield* discover(record.topic)
      const requests = yield* checked("native.encode", () => {
        const groups = new Map<number, Message[]>()
        for (const message of record.messages) {
          const index = message.key == null ? (roundRobin++ >>> 0) % metadata.partitions.length
            : (murmur2(bytes(message.key, config.maxRequestBytes)) & 0x7fffffff) % metadata.partitions.length
          const id = message.partition ?? metadata.partitions[index]!.id
          if (!Number.isInteger(id) || !metadata.partitions.some((p) => p.id === id)) throw new Error(`Unknown partition ${id}`)
          const group = groups.get(id) ?? []
          group.push(message)
          groups.set(id, group)
        }
        const leaders = new Map<number, Map<number, Buffer>>()
        let total = 0
        for (const [id, messages] of groups) {
          const partition = metadata.partitions.find((p) => p.id === id)!
          const batch = recordBatch(messages, config.maxRequestBytes)
          total += batch.length
          if (total > config.maxRequestBytes) throw new Error("Record batches exceed maxRequestBytes")
          const leader = leaders.get(partition.leader) ?? new Map<number, Buffer>()
          leader.set(id, batch)
          leaders.set(partition.leader, leader)
        }
        return [...leaders].map(([leader, batches]) => {
          const address = metadata.brokers.get(leader)
          if (!address) throw new Error(`Leader ${leader} missing from broker metadata`)
          const body = produceRequest(record.topic, batches, config.acks, config.produceTimeoutMs, config.maxRequestBytes)
          // Validate request/header size for every leader before writing any Produce request.
          frameRequest(0, 3, 0, config.clientId, body, config.maxRequestBytes)
          return { address, body, partitions: [...batches.keys()] }
        })
      })
      const reports: DeliveryReport[] = []
      for (const { address, body, partitions } of requests) {
        yield* negotiate(address, 0, 3)
        const response = yield* request(address, 0, 3, body)
        reports.push(...yield* checked("native.produce", () => produceResponse(response, record.topic, partitions)))
      }
      return reports
    })) })
  }))
