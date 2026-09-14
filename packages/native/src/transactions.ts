import { Context, Effect, Exit, Layer, Semaphore } from "effect"
import { KafkaError, type ConsumerGroupMetadata, type ProducerService } from "@effect-kafka/core"
import { makeClient, checked, integer, brokerError } from "./internal/client.js"
import { KafkaBrokerError } from "./internal/protocol.js"
import { makeProducer, type ProducerOptions } from "./internal/producer.js"

export interface TransactionOptions extends ProducerOptions {
  readonly transactionalId: string
  readonly transactionTimeoutMs?: number
}
export interface TransactionOffset {
  readonly topic: string
  readonly partition: number
  /** The next offset to consume (processed offset + 1). */
  readonly offset: string
}
export interface Transaction extends ProducerService {
  readonly sendOffsets: (group: ConsumerGroupMetadata, offsets: ReadonlyArray<TransactionOffset>) => Effect.Effect<void, KafkaError>
}
export class Transactions extends Context.Service<Transactions, {
  readonly withTransaction: <A, E, R>(body: (transaction: Transaction) => Effect.Effect<A, E, R>) => Effect.Effect<A, E | KafkaError, R>
}>()("@effect-kafka/native/Transactions") {}

export const transactionLayer = (options: TransactionOptions): Layer.Layer<Transactions, KafkaError> => Layer.effect(Transactions, Effect.gen(function*() {
  const { transactionalId, transactionTimeoutMs = 60000, ...producerOptions } = options
  yield* checked("native.transaction.config", () => {
    if (!transactionalId || Buffer.byteLength(transactionalId) > 32767) throw new Error("Invalid transactionalId")
    integer(transactionTimeoutMs, "transactionTimeoutMs", 1000, 2147483647)
  })
  const client = yield* makeClient(options)
  const lock = yield* Semaphore.make(1)
  return Transactions.of({ withTransaction: (body) => lock.withPermits(1)(Effect.uninterruptibleMask((restore) => Effect.gen(function*() {
    const initialized = yield* restore(Effect.gen(function*() {
      for (let attempt = 0; ; attempt++) {
        const coordinator = yield* client.coordinator(transactionalId, 1)
        const result = yield* client.rpc(coordinator, "InitProducerId", { transactionalId, transactionTimeoutMs }).pipe(Effect.exit)
        if (Exit.isSuccess(result)) return { coordinator, identity: result.value }
        const error = result.cause.reasons.find((r) => r._tag === "Fail")
        if (attempt >= 10 || !error || error._tag !== "Fail" || !(error.error.cause instanceof KafkaBrokerError) || ![14, 15, 16, 51].includes(error.error.cause.code)) return yield* Effect.failCause(result.cause)
        yield* Effect.sleep(250)
      }
    }))
    const { coordinator, identity } = initialized
    const base = { transactionalId, producerId: identity.producerId, producerEpoch: identity.producerEpoch }
    let active = true
    let started = false
    let poisoned: KafkaError | undefined
    const operations = yield* Semaphore.make(1)
    const guard = <A>(operation: Effect.Effect<A, KafkaError>) => operations.withPermits(1)(Effect.gen(function*() {
      yield* checked("native.transaction.state", () => { if (!active) throw new Error("Transaction has ended"); if (poisoned) throw poisoned })
      return yield* operation.pipe(Effect.catchCause((cause) => {
        poisoned = new KafkaError({ operation: "native.transaction.poisoned", cause })
        return Effect.failCause(cause)
      }))
    }))
    const producer = yield* makeProducer(client, producerOptions, { ...base, sequences: new Map(), enlist: (topic, partitions) => Effect.gen(function*() {
      started = true
      const result = yield* client.rpc(coordinator, "AddPartitionsToTxn", {
        v3AndBelowTransactionalId: transactionalId, v3AndBelowProducerId: identity.producerId, v3AndBelowProducerEpoch: identity.producerEpoch,
        v3AndBelowTopics: [{ name: topic, partitions }]
      })
      yield* checked("native.transaction.enlist", () => {
        const found = result.resultsByTopicV3AndBelow.flatMap((t) => t.resultsByPartition.map((p) => ({ topic: t.name, ...p })))
        if (found.length !== partitions.length || partitions.some((id) => found.filter((p) => p.topic === topic && p.partitionIndex === id).length !== 1)) throw new Error("Invalid AddPartitionsToTxn response")
        for (const p of found) brokerError("AddPartitionsToTxn", p.partitionErrorCode)
      })
    }) })
    const transaction: Transaction = {
      send: (record) => guard(producer.send(record)),
      sendOffsets: (group, offsets) => guard(Effect.gen(function*() {
        const topics = yield* checked("native.transaction.offsets", () => {
          if (!group.groupId || !group.memberId || group.generationId < 0) throw new Error("Active consumer group metadata is required")
          const grouped = new Map<string, Map<number, bigint>>()
          for (const o of offsets) {
            integer(o.partition, "partition", 0, 2147483647)
            const offset = BigInt(o.offset)
            if (!o.topic || offset < 0n || offset > 9223372036854775807n) throw new Error("Invalid transaction offset")
            const partitions = grouped.get(o.topic) ?? new Map<number, bigint>()
            if (partitions.has(o.partition)) throw new Error("Duplicate transaction partition")
            partitions.set(o.partition, offset); grouped.set(o.topic, partitions)
          }
          return [...grouped].map(([name, partitions]) => ({ name, partitions: [...partitions].map(([partitionIndex, committedOffset]) => ({ partitionIndex, committedOffset, committedLeaderEpoch: -1, committedMetadata: null })) }))
        })
        if (!topics.length) return
        started = true
        yield* client.rpc(coordinator, "AddOffsetsToTxn", { ...base, groupId: group.groupId })
        const groupCoordinator = yield* client.coordinator(group.groupId, 0)
        const result = yield* client.rpc(groupCoordinator, "TxnOffsetCommit", { ...base, ...group, groupInstanceId: group.groupInstanceId ?? null, topics })
        yield* checked("native.transaction.offsets", () => {
          if (result.topics.length !== topics.length) throw new Error("Missing offset result")
          for (const t of topics) {
            const actual = result.topics.filter((r) => r.name === t.name)
            if (actual.length !== 1 || actual[0]!.partitions.length !== t.partitions.length) throw new Error("Invalid offset result")
            for (const p of t.partitions) {
              const matches = actual[0]!.partitions.filter((r) => r.partitionIndex === p.partitionIndex)
              if (matches.length !== 1) throw new Error("Missing offset partition")
              brokerError("TxnOffsetCommit", matches[0]!.errorCode)
            }
          }
        })
      }))
    }
    const exit = yield* restore(Effect.suspend(() => body(transaction)).pipe(Effect.timeoutOrElse({ duration: transactionTimeoutMs - 500,
      orElse: () => Effect.fail(new KafkaError({ operation: "native.transaction.timeout", cause: new Error("Transaction deadline exceeded") })) }))).pipe(Effect.exit)
    // Wait for an in-flight operation before closing. Escaped operations are rejected.
    yield* operations.withPermits(1)(Effect.sync(() => { active = false }))
    const commit = Exit.isSuccess(exit) && !poisoned
    if (started) {
      const end = yield* client.rpc(coordinator, "EndTxn", { ...base, committed: commit }).pipe(Effect.exit)
      if (Exit.isFailure(end)) return yield* Effect.failCause(end.cause)
    }
    if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause)
    if (poisoned) return yield* Effect.fail(poisoned)
    return exit.value
  }))) })
}))
