import { Context, Data, Effect, Exit, Layer } from "effect"
import { describe, expect, test, vi } from "vitest"
import { Consumer, Producer, consumerLayer, producerLayer, type ConsumerDriver, type ConsumerRecord } from "../src/index.js"

const record: ConsumerRecord = {
  topic: "events", partition: 0, offset: "9007199254740993", timestamp: "0",
  key: null, value: new Uint8Array([1]), heartbeat: Effect.void, commit: Effect.void
}
const latch = <A>() => {
  let resolve!: (a: A) => void
  const promise = new Promise<A>((r) => { resolve = r })
  return { resolve, promise }
}
function fixture() {
  const started = latch<void>()
  let handler!: (record: ConsumerRecord) => Promise<void>
  let fail!: (cause: unknown) => void
  const remove = vi.fn()
  const driver: ConsumerDriver = {
    connect: vi.fn(async () => {}), disconnect: vi.fn(async () => {}),
    subscribe: vi.fn(async () => {}),
    run: vi.fn(async (h) => { handler = h; started.resolve() }),
    onFailure: (f) => { fail = f; return remove }
  }
  return { driver, started, remove, handle: () => handler(record), fail: (cause: unknown) => fail(cause) }
}

class HandlerError extends Data.TaggedError("HandlerError")<{ readonly reason: string }> {}

describe("producer", () => {
  test("is lazy, connects once, sends, and disconnects on scope exit", async () => {
    const calls: string[] = []
    const driver = {
      connect: async () => { calls.push("connect") },
      disconnect: async () => { calls.push("disconnect") },
      send: async () => { calls.push("send"); return [] }
    }
    const create = vi.fn(() => driver)
    const live = producerLayer(create)
    expect(create).not.toHaveBeenCalled()
    await Effect.runPromise(Effect.gen(function*() {
      const p = yield* Producer
      yield* p.send({ topic: "events", messages: [] })
      yield* p.send({ topic: "events", messages: [] })
    }).pipe(Effect.provide(live)))
    expect(create).toHaveBeenCalledTimes(1)
    expect(calls).toEqual(["connect", "send", "send", "disconnect"])
  })
  test.each(["connect", "send"] as const)("preserves %s failures and releases the client", async (operation) => {
    const cause = new Error("offline")
    const driver = {
      connect: vi.fn(async () => { if (operation === "connect") throw cause }),
      disconnect: vi.fn(async () => {}),
      send: vi.fn(async () => { throw cause })
    }
    const error = await Effect.runPromise(Effect.gen(function*() {
      const p = yield* Producer
      return yield* p.send({ topic: "events", messages: [] })
    }).pipe(Effect.provide(producerLayer(() => driver)), Effect.flip))
    expect(error).toMatchObject({ _tag: "KafkaError", operation: `producer.${operation}`, cause })
    expect(driver.disconnect).toHaveBeenCalledOnce()
  })
  test("reports constructor exceptions as typed errors", async () => {
    const error = await Effect.runPromise(Producer.pipe(Effect.provide(producerLayer(() => { throw "bad config" })), Effect.flip))
    expect(error).toMatchObject({ _tag: "KafkaError", operation: "producer.create", cause: "bad config" })
  })
})

describe("consumer", () => {
  test("preserves handler errors, rejects the driver callback, and disconnects", async () => {
    const f = fixture()
    const error = new HandlerError({ reason: "invalid message" })
    const result = Effect.runPromise(Effect.gen(function*() {
      const c = yield* Consumer
      return yield* c.consume({ topics: ["events"] }, () => Effect.fail(error))
    }).pipe(Effect.provide(consumerLayer(() => f.driver)), Effect.flip))
    await f.started.promise
    await expect(f.handle()).rejects.toThrow("Kafka message handler failed")
    expect(await result).toBe(error)
    expect(f.driver.disconnect).toHaveBeenCalledOnce()
    expect(f.remove).toHaveBeenCalledOnce()
  })
  test("provides caller services and aborts active handlers before disconnecting", async () => {
    class Greeting extends Context.Service<Greeting, string>()("Greeting") {}
    const f = fixture()
    const entered = latch<string>()
    const events: string[] = []
    const driver = { ...f.driver, disconnect: async () => { events.push("disconnect") } }
    const controller = new AbortController()
    const result = Effect.runPromiseExit(Effect.gen(function*() {
      const c = yield* Consumer
      return yield* c.consume({ topics: ["events"] }, () => Effect.gen(function*() {
        const greeting = yield* Greeting
        entered.resolve(greeting)
        yield* Effect.never
      }).pipe(Effect.ensuring(Effect.sync(() => { events.push("handler cleanup") }))))
    }).pipe(Effect.provide(Layer.merge(consumerLayer(() => driver), Layer.succeed(Greeting, "hello")))), { signal: controller.signal })
    await f.started.promise
    const handled = f.handle().catch(() => {})
    expect(await entered.promise).toBe("hello")
    controller.abort()
    expect(Exit.isFailure(await result)).toBe(true)
    await handled
    expect(events).toEqual(["handler cleanup", "disconnect"])
  })
  test("waits for successful processing before resolving the driver callback", async () => {
    const f = fixture()
    const release = latch<void>()
    const controller = new AbortController()
    const result = Effect.runPromiseExit(Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: ["events"] }, () => Effect.promise(() => release.promise))), Effect.provide(consumerLayer(() => f.driver))), { signal: controller.signal })
    await f.started.promise
    const completed = vi.fn()
    const handled = f.handle().then(completed)
    await Promise.resolve()
    expect(completed).not.toHaveBeenCalled()
    release.resolve()
    await handled
    expect(completed).toHaveBeenCalledOnce()
    controller.abort()
    await result
  })
  test.each(["connect", "subscribe", "run"] as const)("surfaces %s failures and disconnects", async (operation) => {
    const f = fixture()
    const cause = new Error("broker failed")
    const driver = { ...f.driver, [operation]: async () => { throw cause } }
    const error = await Effect.runPromise(Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: ["events"] }, () => Effect.void)), Effect.provide(consumerLayer(() => driver)), Effect.flip))
    expect(error).toMatchObject({ _tag: "KafkaError", operation: `consumer.${operation}`, cause })
    expect(driver.disconnect).toHaveBeenCalledOnce()
  })
  test("surfaces background driver crashes", async () => {
    const f = fixture()
    const cause = new Error("crashed")
    const result = Effect.runPromise(Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: ["events"] }, () => Effect.void)), Effect.provide(consumerLayer(() => f.driver)), Effect.flip))
    await f.started.promise
    f.fail(cause)
    expect(await result).toMatchObject({ _tag: "KafkaError", cause })
    expect(f.remove).toHaveBeenCalledOnce()
  })
  test("rejects empty subscriptions before allocating a client", async () => {
    const create = vi.fn(() => fixture().driver)
    const error = await Effect.runPromise(Consumer.pipe(Effect.flatMap((c) => c.consume({ topics: [] }, () => Effect.void)), Effect.provide(consumerLayer(create)), Effect.flip))
    expect(error.operation).toBe("consumer.subscribe")
    expect(create).not.toHaveBeenCalled()
  })
})
