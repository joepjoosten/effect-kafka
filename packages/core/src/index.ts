import { Context, Data, Effect, Exit, Layer } from "effect"

/** A Kafka operation failed. The original driver error is preserved in cause. */
export class KafkaError extends Data.TaggedError("KafkaError")<{
  readonly operation: string
  readonly cause: unknown
}> {}

export type Bytes = string | Uint8Array
export type Headers = Readonly<Record<string, Bytes | ReadonlyArray<Bytes> | undefined>>
export interface Message {
  readonly value: Bytes | null
  readonly key?: Bytes | null
  readonly partition?: number
  readonly timestamp?: string
  readonly headers?: Headers
}
export interface ProducerRecord {
  readonly topic: string
  readonly messages: ReadonlyArray<Message>
}
export interface DeliveryReport {
  readonly topicName: string
  readonly partition: number
  readonly errorCode: number
  readonly baseOffset?: string
  readonly offset?: string
}
export interface Subscription {
  readonly topics: ReadonlyArray<string | RegExp>
  readonly fromBeginning?: boolean
  readonly partitionsConsumedConcurrently?: number
}
export interface ConsumerGroupMetadata {
  readonly groupId: string
  readonly generationId: number
  readonly memberId: string
  readonly groupInstanceId?: string | null
}
export interface ConsumerRecord {
  /** Native group identity for transactionally committing consumed offsets. */
  readonly groupMetadata?: ConsumerGroupMetadata
  readonly topic: string
  readonly partition: number
  readonly offset: string
  readonly timestamp: string
  readonly key: Uint8Array | null
  readonly value: Uint8Array | null
  readonly headers?: Headers
  /** KafkaJS handlers should heartbeat periodically during long processing. */
  readonly heartbeat: Effect.Effect<void, KafkaError>
  /** Commit offset + 1. Only call after processing; later offsets cover earlier records. */
  readonly commit: Effect.Effect<void, KafkaError>
}
export interface ProducerService {
  readonly send: (record: ProducerRecord) => Effect.Effect<ReadonlyArray<DeliveryReport>, KafkaError>
}
export class Producer extends Context.Service<Producer, ProducerService>()("@effect-kafka/core/Producer") {}
export interface ConsumerService {
  /** Runs until interrupted or failed. Each invocation owns a fresh client. */
  readonly consume: <E, R>(
    subscription: Subscription,
    handler: (record: ConsumerRecord) => Effect.Effect<void, E, R>
  ) => Effect.Effect<never, KafkaError | E, R>
}
export class Consumer extends Context.Service<Consumer, ConsumerService>()("@effect-kafka/core/Consumer") {}

/** Adapter boundary; also useful for deterministic application tests. */
export interface ProducerDriver {
  readonly connect: () => Promise<void>
  readonly disconnect: () => Promise<void>
  readonly send: (record: ProducerRecord) => Promise<ReadonlyArray<DeliveryReport>>
}
export interface ConsumerDriver {
  readonly connect: () => Promise<void>
  readonly disconnect: () => Promise<void>
  readonly subscribe: () => Promise<void>
  /** Resolves when started, not when consumption ends. Await each handler. */
  readonly run: (handler: (record: ConsumerRecord) => Promise<void>) => Promise<void>
  readonly onFailure?: (handler: (cause: unknown) => void) => (() => void)
}
export const attempt = <A>(operation: string, run: () => Promise<A>): Effect.Effect<A, KafkaError> =>
  Effect.tryPromise({ try: run, catch: (cause) => new KafkaError({ operation, cause }) })

/** Acquires a producer and releases it when the layer's scope closes. */
export const producerLayer = (create: () => ProducerDriver): Layer.Layer<Producer, KafkaError> =>
  Layer.effect(Producer, Effect.gen(function*() {
    const driver = yield* Effect.acquireRelease(
      Effect.try({ try: create, catch: (cause) => new KafkaError({ operation: "producer.create", cause }) }),
      (driver) => attempt("producer.disconnect", () => driver.disconnect()).pipe(Effect.orDie)
    )
    // Drivers do not support AbortSignal for connect: wait for settlement before cleanup.
    yield* attempt("producer.connect", () => driver.connect()).pipe(Effect.uninterruptible)
    return Producer.of({ send: (record) => attempt("producer.send", () => driver.send(record)) })
  }))

/** Builds an Effect consumer while preserving handler errors and required services. */
export const consumerLayer = (create: (subscription: Subscription) => ConsumerDriver): Layer.Layer<Consumer> =>
  Layer.succeed(Consumer, Consumer.of({
    consume: <E, R>(subscription: Subscription, handler: (record: ConsumerRecord) => Effect.Effect<void, E, R>) =>
      Effect.scoped(Effect.gen(function*() {
        if (subscription.topics.length === 0) {
          return yield* Effect.fail(new KafkaError({ operation: "consumer.subscribe", cause: new Error("At least one topic is required") }))
        }
        const context = yield* Effect.context<R>()
        const driver = yield* Effect.acquireRelease(
          Effect.try({ try: () => create(subscription), catch: (cause) => new KafkaError({ operation: "consumer.create", cause }) }),
          (driver) => attempt("consumer.disconnect", () => driver.disconnect()).pipe(Effect.orDie)
        )
        yield* attempt("consumer.connect", () => driver.connect()).pipe(Effect.uninterruptible)
        yield* attempt("consumer.subscribe", () => driver.subscribe()).pipe(Effect.uninterruptible)
        let cleanup: Effect.Effect<void> = Effect.void
        return yield* Effect.callback<never, KafkaError | E>((resume, signal) => {
          let ended = false
          const controller = new AbortController()
          const pending = new Set<Promise<unknown>>()
          const abort = () => controller.abort()
          signal.addEventListener("abort", abort, { once: true })
          const fail = (cause: unknown) => {
            if (!ended) {
              ended = true
              resume(Effect.fail(new KafkaError({ operation: "consumer.run", cause })))
            }
          }
          const remove = driver.onFailure?.(fail)
          const run = Effect.runPromiseExitWith(context)
          // The microtask also captures synchronous driver.run exceptions.
          void Promise.resolve().then(() => driver.run(async (record) => {
            if (ended || signal.aborted) throw new Error("Consumer is stopping")
            const task = run(Effect.suspend(() => handler(record)), { signal: controller.signal })
            pending.add(task)
            const exit = await task.finally(() => pending.delete(task))
            if (Exit.isFailure(exit)) {
              if (!ended) {
                ended = true
                resume(Effect.failCause(exit.cause))
              }
              // Reject so the driver cannot acknowledge a failed/interrupted record.
              throw new Error("Kafka message handler failed", { cause: exit.cause })
            }
          })).catch(fail)
          cleanup = Effect.promise(async () => {
            ended = true
            controller.abort()
            await Promise.allSettled(pending)
            signal.removeEventListener("abort", abort)
            remove?.()
          })
        }).pipe(Effect.ensuring(Effect.suspend(() => cleanup)))
      }))
  }))
