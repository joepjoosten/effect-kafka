import { Cause, Effect, Exit, Layer } from "effect"
import { Consumer, KafkaError, type ConsumerGroupMetadata } from "@effect-kafka/core"
import { checked, integer, makeClient, brokerError, type ClientOptions } from "./internal/client.js"
import { Reader, Writer } from "./internal/binary.js"
import { KafkaBrokerError } from "./internal/protocol.js"
import { decodeBatches } from "./internal/records.js"
import type { Endpoint } from "./internal/transport.js"

export interface ConsumerOptions extends ClientOptions {
  readonly groupId: string
  readonly sessionTimeoutMs?: number
  readonly heartbeatIntervalMs?: number
  readonly rebalanceTimeoutMs?: number
  readonly fetchWaitMs?: number
  readonly autoCommit?: boolean
  readonly isolationLevel?: "read_committed" | "read_uncommitted"
}
export const subscriptionMetadata = (topics: ReadonlyArray<string>): Buffer => {
  const w = new Writer().i16(0).i32(topics.length)
  for (const topic of topics) w.string(topic)
  return w.i32(-1).finish()
}
export const readSubscription = (body: Buffer): ReadonlyArray<string> => {
  const r = new Reader(body)
  const version = r.i16()
  if (version < 0 || version > 3) throw new Error("Unsupported consumer subscription version")
  const topics = r.array(() => r.string())
  r.bytes()
  // Newer subscription versions append owned partitions and rack information.
  return topics
}
export const assignment = (topics: ReadonlyMap<string, ReadonlyArray<number>>): Buffer => {
  const w = new Writer().i16(0).i32(topics.size)
  for (const [topic, partitions] of topics) { w.string(topic).i32(partitions.length); for (const p of partitions) w.i32(p) }
  return w.i32(-1).finish()
}
export const readAssignment = (body: Buffer): Array<{ topic: string; partition: number }> => {
  const r = new Reader(body)
  if (r.i16() !== 0) throw new Error("Unsupported assignment version")
  const result = r.array(() => { const topic = r.string(); return r.array(() => ({ topic, partition: r.i32() })) }).flat()
  r.bytes(); r.end()
  const keys = result.map((p) => p.topic + ":" + p.partition)
  if (new Set(keys).size !== keys.length || result.some((p) => p.partition < 0)) throw new Error("Invalid partition assignment")
  return result
}
export const consumerLayer = (options: ConsumerOptions): Layer.Layer<Consumer, KafkaError> => Layer.effect(Consumer, Effect.gen(function*() {
  const client = yield* makeClient({ ...options, requestTimeoutMs: options.requestTimeoutMs ?? Math.max(35000, (options.rebalanceTimeoutMs ?? 60000) + 5000) })
  const settings = yield* checked("native.consumer.config", () => {
    if (!options.groupId || Buffer.byteLength(options.groupId) > 32767) throw new Error("Invalid groupId")
    const session = integer(options.sessionTimeoutMs ?? 30000, "sessionTimeoutMs", 1000, 2147483647)
    const heartbeat = integer(options.heartbeatIntervalMs ?? 3000, "heartbeatIntervalMs", 1, session - 1)
    const rebalance = integer(options.rebalanceTimeoutMs ?? 60000, "rebalanceTimeoutMs", session, 2147483647)
    const wait = integer(options.fetchWaitMs ?? 500, "fetchWaitMs", 0, client.config.transport.requestTimeoutMs - 1)
    const isolation = options.isolationLevel ?? "read_committed"
    if (isolation !== "read_committed" && isolation !== "read_uncommitted") throw new Error("Invalid isolationLevel")
    return { session, heartbeat, rebalance, wait, isolation: isolation === "read_committed" ? 1 : 0 }
  })
  return Consumer.of({ consume: (subscription, handler) => Effect.scoped(Effect.gen(function*() {
    const topics = yield* checked("native.consumer.subscribe", () => {
      if (!subscription.topics.length || subscription.topics.some((t) => typeof t !== "string" || !/^[a-zA-Z0-9._-]{1,249}$/.test(t) || t === "." || t === "..")) throw new Error("Native subscriptions require explicit topic names")
      integer(subscription.partitionsConsumedConcurrently ?? 1, "partitionsConsumedConcurrently", 1, 1024)
      return [...new Set(subscription.topics as ReadonlyArray<string>)].sort()
    })
    let memberId = ""
    let coordinator: Endpoint | undefined
    let current: ConsumerGroupMetadata | undefined
    let handlerFailed = false
    yield* Effect.addFinalizer(() => Effect.suspend(() => {
      current = undefined
      return coordinator && memberId ? client.rpc(coordinator, "LeaveGroup", { groupId: options.groupId, memberId }).pipe(Effect.ignore) : Effect.void
    }))
    const cycle = Effect.gen(function*() {
      coordinator = yield* client.coordinator(options.groupId, 0)
      const join = () => client.rpc(coordinator!, "JoinGroup", { groupId: options.groupId, memberId, groupInstanceId: null,
        sessionTimeoutMs: settings.session, rebalanceTimeoutMs: settings.rebalance, protocolType: "consumer",
        protocols: [{ name: "range", metadata: subscriptionMetadata(topics) }] })
      let joined = yield* join()
      if (joined.errorCode === 79) { memberId = joined.memberId; joined = yield* join() }
      yield* checked("native.consumer.join", () => brokerError("JoinGroup", joined.errorCode))
      memberId = joined.memberId
      const group: ConsumerGroupMetadata = { groupId: options.groupId, generationId: joined.generationId, memberId, groupInstanceId: null }
      const assignments: Array<{ memberId: string; assignment: Buffer }> = []
      if (joined.leader === memberId) {
        const members = yield* checked("native.consumer.assignment", () => joined.members.map((m) => ({ id: m.memberId, topics: readSubscription(m.metadata) })).sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
        const allocated = new Map(members.map((m) => [m.id, new Map<string, number[]>()]))
        for (const topic of [...new Set(members.flatMap((m) => [...m.topics]))].sort()) {
          const metadata = yield* client.discover(topic)
          const eligible = members.filter((m) => m.topics.includes(topic))
          let cursor = 0
          for (const [index, member] of eligible.entries()) {
            const count = Math.floor(metadata.partitions.length / eligible.length) + (index < metadata.partitions.length % eligible.length ? 1 : 0)
            allocated.get(member.id)!.set(topic, metadata.partitions.slice(cursor, cursor + count).map((p) => p.id)); cursor += count
          }
        }
        for (const [id, partitions] of allocated) assignments.push({ memberId: id, assignment: assignment(partitions) })
      }
      const synced = yield* client.rpc(coordinator, "SyncGroup", { ...group, groupInstanceId: null, assignments })
      const partitions = yield* checked("native.consumer.assignment", () => {
        const result = readAssignment(synced.assignment)
        if (result.some((p) => !topics.includes(p.topic))) throw new Error("Assignment contains unsubscribed topic")
        return result
      })
      current = group
      const alive = checked("native.consumer.generation", () => { if (current !== group) throw new KafkaBrokerError({ api: "ConsumerGeneration", code: 22 }) })
      const heartbeat = Effect.gen(function*() { yield* alive; yield* client.rpc(coordinator!, "Heartbeat", { ...group, groupInstanceId: null }) })
      const commit = (topic: string, partition: number, offset: bigint) => Effect.gen(function*() {
        yield* alive
        const response = yield* client.rpc(coordinator!, "OffsetCommit", { groupId: group.groupId, generationIdOrMemberEpoch: group.generationId, memberId: group.memberId, retentionTimeMs: -1n,
          topics: [{ name: topic, partitions: [{ partitionIndex: partition, committedOffset: offset, committedMetadata: null }] }] })
        yield* checked("native.consumer.commit", () => {
          if (response.topics.length !== 1 || response.topics[0]!.name !== topic || response.topics[0]!.partitions.length !== 1 || response.topics[0]!.partitions[0]!.partitionIndex !== partition) throw new Error("Invalid OffsetCommit response")
          brokerError("OffsetCommit", response.topics[0]!.partitions[0]!.errorCode)
        })
      })
      const process = Effect.gen(function*() {
        const positions = new Map<string, bigint>()
        const aborted = new Map<string, Set<bigint>>()
        const poll = ({ topic, partition }: { topic: string; partition: number }) => Effect.gen(function*() {
          const key = topic + ":" + partition
          const metadata = yield* client.discover(topic)
          const leader = metadata.partitions.find((p) => p.id === partition)?.leader
          const address = leader === undefined ? undefined : metadata.brokers.get(leader)
          if (!address) return yield* Effect.fail(new KafkaError({ operation: "native.consumer.metadata", cause: new KafkaBrokerError({ api: "Metadata", code: 6 }) }))
          let offset = positions.get(key)
          if (offset === undefined) {
            const committed = yield* client.rpc(coordinator!, "OffsetFetch", { groupId: options.groupId, topics: [{ name: topic, partitionIndexes: [partition] }] })
            offset = yield* checked("native.consumer.offset", () => {
              const p = committed.topics.find((t) => t.name === topic)?.partitions.find((p) => p.partitionIndex === partition)
              if (!p) throw new Error("Missing committed offset")
              brokerError("OffsetFetch", p.errorCode); return p.committedOffset
            })
            if (offset < 0n) {
              const result = yield* client.rpc(address, "ListOffsets", { replicaId: -1, isolationLevel: settings.isolation, topics: [{ name: topic, partitions: [{ partitionIndex: partition, timestamp: subscription.fromBeginning ? -2n : -1n }] }] })
              offset = yield* checked("native.consumer.offset", () => {
                const p = result.topics.find((t) => t.name === topic)?.partitions.find((p) => p.partitionIndex === partition)
                if (!p) throw new Error("Missing initial offset")
                brokerError("ListOffsets", p.errorCode); if (p.offset < 0n) throw new Error("Invalid initial offset"); return p.offset
              })
            }
            positions.set(key, offset)
          }
          const fetched = yield* client.rpc(address, "Fetch", { replicaId: -1, maxWaitMs: settings.wait, minBytes: 1, maxBytes: client.config.transport.maxResponseBytes - 1024, isolationLevel: settings.isolation,
            sessionId: 0, sessionEpoch: -1, forgottenTopicsData: [], rackId: "",
            topics: [{ topic, partitions: [{ partition, currentLeaderEpoch: -1, fetchOffset: offset, logStartOffset: -1n, partitionMaxBytes: client.config.transport.maxResponseBytes - 1024 }] }] })
          const response = yield* checked("native.consumer.fetch", () => {
            const p = fetched.responses.find((t) => t.topic === topic)?.partitions.find((p) => p.partitionIndex === partition)
            if (!p) throw new Error("Missing Fetch partition")
            brokerError("Fetch", p.errorCode); return p
          })
          const batches = yield* checked("native.consumer.decode", () => decodeBatches(response.records ?? Buffer.alloc(0), client.config.transport.maxResponseBytes))
          const activeAborts = aborted.get(key) ?? new Set<bigint>(); aborted.set(key, activeAborts)
          const pending = [...response.abortedTransactions ?? []].sort((a, b) => a.firstOffset < b.firstOffset ? -1 : 1)
          for (const batch of batches) {
            while (pending.length && pending[0]!.firstOffset <= batch.baseOffset) activeAborts.add(pending.shift()!.producerId)
            if (batch.control) { activeAborts.delete(batch.producerId) }
            else if (!(settings.isolation === 1 && batch.transactional && activeAborts.has(batch.producerId))) {
              for (const record of batch.records) {
                if (record.offset < offset || (settings.isolation === 1 && record.offset >= response.lastStableOffset)) continue
                yield* alive
                const exit = yield* Effect.suspend(() => handler({ topic, partition, offset: record.offset.toString(), timestamp: record.timestamp.toString(), key: record.key, value: record.value, headers: record.headers,
                  groupMetadata: group, heartbeat, commit: commit(topic, partition, record.offset + 1n) })).pipe(Effect.exit)
                if (Exit.isFailure(exit)) { if (!Cause.hasInterrupts(exit.cause)) handlerFailed = true; return yield* Effect.failCause(exit.cause) }
                if (options.autoCommit !== false) yield* commit(topic, partition, record.offset + 1n)
              }
            }
            const next = settings.isolation === 1 && batch.nextOffset > response.lastStableOffset ? response.lastStableOffset : batch.nextOffset
            if (next > offset) { offset = next; positions.set(key, offset) }
          }
        })
        return yield* Effect.forever(Effect.forEach(partitions, poll, { concurrency: subscription.partitionsConsumedConcurrently ?? 1, discard: true }).pipe(Effect.andThen(Effect.sleep(partitions.length ? 10 : settings.heartbeat))))
      })
      return yield* Effect.raceFirst(process, Effect.forever(Effect.sleep(settings.heartbeat).pipe(Effect.andThen(heartbeat)))).pipe(Effect.ensuring(Effect.sync(() => { current = undefined })))
    })
    const retry = (): typeof cycle => cycle.pipe(Effect.catchTag("KafkaError", (error) => {
      if (handlerFailed || !(error instanceof KafkaError)) return Effect.fail(error)
      const code = error.cause instanceof KafkaBrokerError ? error.cause.code : undefined
      if (code === undefined ? !["native.connect", "native.request"].includes(error.operation) : ![3, 5, 6, 14, 15, 16, 22, 25, 27].includes(code)) return Effect.fail(error)
      if (code === 22 || code === 25) memberId = ""
      current = undefined
      return Effect.sleep(250).pipe(Effect.andThen(Effect.suspend(retry)))
    }))
    return yield* retry()
  })) })
}))
