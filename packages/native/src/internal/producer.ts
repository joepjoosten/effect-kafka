import { Effect, Layer, Semaphore } from "effect"
import { KafkaError, Producer, type DeliveryReport, type Message, type ProducerRecord } from "@effect-kafka/core"
import { bytes, murmur2, recordBatch, type Compression, type BatchOptions } from "./records.js"
import { frameRequest, produceRequest, produceResponse } from "./protocol.js"
import { checked, integer, makeClient, type Client, type ClientOptions } from "./client.js"
export interface ProducerOptions extends ClientOptions {
  readonly acks?: 1 | -1
  readonly produceTimeoutMs?: number
  readonly compression?: Compression
}
export interface TransactionBatchState {
  readonly transactionalId: string
  readonly producerId: bigint
  readonly producerEpoch: number
  readonly sequences: Map<string, number>
  readonly enlist: (topic: string, partitions: ReadonlyArray<number>) => Effect.Effect<void, KafkaError>
}
/** Internal producer engine shared by ordinary and transactional producers. */
export const makeProducer = (client: Client, options: ProducerOptions, transaction?: TransactionBatchState) => Effect.gen(function*() {
  const { config, discover, negotiate, raw: request } = client
  const settings = yield* checked("native.config", () => {
    const acks = options.acks ?? -1
    if (acks !== 1 && acks !== -1) throw new Error("acks must be 1 or -1")
    if (transaction && acks !== -1) throw new Error("Transactions require acks=-1")
    if ("idempotent" in options || "transactionalId" in options) throw new Error("Use transactionLayer for transactional producers")
    const compression = options.compression ?? "none"
    if (!["none", "gzip", "zstd"].includes(compression)) throw new Error("Unsupported compression codec")
    return { acks, compression, produceTimeoutMs: integer(options.produceTimeoutMs ?? 30000, "produceTimeoutMs", 1, 2147483647) }
  })
  const version = settings.compression === "zstd" ? 7 : 3
  const semaphore = yield* Semaphore.make(1)
  let roundRobin = 0
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
          const sequence = transaction?.sequences.get(record.topic + ":" + id) ?? 0
          if (sequence + messages.length > 2147483647) throw new Error("Transaction sequence exhausted")
          const batchOptions: BatchOptions = { compression: settings.compression, ...(transaction ? {
            producerId: transaction.producerId, producerEpoch: transaction.producerEpoch, sequence, transactional: true
          } : {}) }
          const batch = recordBatch(messages, config.maxRequestBytes, BigInt(Date.now()), batchOptions)
          total += batch.length
          if (total > config.maxRequestBytes) throw new Error("Record batches exceed maxRequestBytes")
          const leader = leaders.get(partition.leader) ?? new Map<number, Buffer>()
          leader.set(id, batch)
          leaders.set(partition.leader, leader)
        }
        return [...leaders].map(([leader, batches]) => {
          const address = metadata.brokers.get(leader)
          if (!address) throw new Error(`Leader ${leader} missing from broker metadata`)
          const body = produceRequest(record.topic, batches, settings.acks, settings.produceTimeoutMs, config.maxRequestBytes, transaction?.transactionalId)
          // Validate request/header size for every leader before writing any Produce request.
          frameRequest(0, version, 0, config.clientId, body, config.maxRequestBytes)
          return { address, body, partitions: [...batches.keys()], counts: new Map([...batches].map(([id, batch]) => [id, batch.readInt32BE(57)])) }
        })
      })
      if (transaction) yield* transaction.enlist(record.topic, requests.flatMap((r) => r.partitions))
      const reports: DeliveryReport[] = []
      for (const { address, body, partitions, counts } of requests) {
        yield* negotiate(address, 0, version)
        const acknowledged = yield* Effect.gen(function*() {
          const response = yield* request(address, 0, version, body)
          return yield* checked("native.produce", () => produceResponse(response, record.topic, partitions, version))
        }).pipe(Effect.withSpan("kafka.native.produce", { kind: "producer", attributes: {
          "messaging.system": "kafka", "messaging.destination.name": record.topic,
          "server.address": address.host, "server.port": address.port,
          "kafka.api.version": version, "kafka.acks": settings.acks,
          "kafka.compression": settings.compression, "kafka.partition.count": partitions.length
        } }, { captureStackTrace: false }))
        reports.push(...acknowledged)
        if (transaction) for (const [id, count] of counts) {
          const key = record.topic + ":" + id
          transaction.sequences.set(key, (transaction.sequences.get(key) ?? 0) + count)
        }
      }
      return reports
    })).pipe(Effect.withSpan("kafka.native.send", { kind: "producer", attributes: {
      "messaging.system": "kafka", "messaging.destination.name": record.topic,
      "messaging.batch.message_count": record.messages.length
    } }, { captureStackTrace: false })) })
})

export const producerLayer = (options: ProducerOptions): Layer.Layer<Producer, KafkaError> => Layer.effect(Producer,
  Effect.gen(function*() { return yield* makeProducer(yield* makeClient(options), options) }))
