import { Effect, Redacted } from "effect"
import { KafkaError } from "@effect-kafka/core"
import { endpoint, exchange, type Endpoint, type TransportOptions } from "./transport.js"
import { checkVersions, frameRequest, KafkaBrokerError, metadataRequest, metadataResponse, responseBody, type Metadata } from "./protocol.js"
import { apis, encode, decode, type Requests } from "./wire.js"
import type { SaslOptions } from "./sasl.js"

export interface ClientOptions {
  readonly brokers: ReadonlyArray<string>
  readonly clientId?: string
  readonly tls?: TransportOptions["tls"]
  readonly sasl?: SaslOptions
  readonly requestTimeoutMs?: number
  readonly maxRequestBytes?: number
  readonly maxResponseBytes?: number
  readonly allowAutoTopicCreation?: boolean
  readonly metadataRetries?: number
}
export const checked = <A>(operation: string, evaluate: () => A): Effect.Effect<A, KafkaError> =>
  Effect.try({ try: evaluate, catch: (cause) => new KafkaError({ operation, cause }) })
export const integer = (value: number, name: string, min: number, max: number): number => {
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${name} must be an integer between ${min} and ${max}`)
  return value
}
export const brokerError = (api: string, code: number): void => { if (code) throw new KafkaBrokerError({ api, code }) }
export const makeClient = (options: ClientOptions) => Effect.gen(function*() {
  const config = yield* checked("native.config", () => {
    if (!Array.isArray(options.brokers) || !options.brokers.length) throw new Error("At least one bootstrap broker is required")
    const clientId = options.clientId ?? "effect-kafka-native"
    if (Buffer.byteLength(clientId) > 32767) throw new Error("clientId exceeds Kafka string limit")
    if (options.sasl && (!["plain", "scram-sha-256", "scram-sha-512"].includes(options.sasl.mechanism) || typeof options.sasl.username !== "string" ||
      !(typeof options.sasl.password === "string" || Redacted.isRedacted(options.sasl.password)))) throw new Error("Invalid SASL configuration")
    const transport: TransportOptions = {
      requestTimeoutMs: integer(options.requestTimeoutMs ?? 35000, "requestTimeoutMs", 1, 2147483647),
      maxResponseBytes: integer(options.maxResponseBytes ?? 16 * 1024 * 1024, "maxResponseBytes", 4, 256 * 1024 * 1024),
      ...(options.tls === undefined ? {} : { tls: { ...options.tls } }),
      ...(options.sasl === undefined ? {} : { sasl: { ...options.sasl, password: typeof options.sasl.password === "string" ? Redacted.make(options.sasl.password) : options.sasl.password } })
    }
    return {
      brokers: options.brokers.map(endpoint), clientId, transport,
      maxRequestBytes: integer(options.maxRequestBytes ?? 1024 * 1024, "maxRequestBytes", 128, 256 * 1024 * 1024),
      metadataRetries: integer(options.metadataRetries ?? 2, "metadataRetries", 0, 10),
      autoCreate: options.allowAutoTopicCreation ?? false
    }
  })
  let correlation = 0
  const raw = (address: Endpoint, api: number, version: number, body: Buffer, flexible = false) => Effect.gen(function*() {
    const id = correlation = (correlation + 1) & 0x7fffffff
    const frame = yield* checked("native.encode", () => frameRequest(api, version, id, config.clientId, body, config.maxRequestBytes, flexible))
    const response = yield* exchange(address, frame, config.transport)
    return yield* checked("native.decode", () => responseBody(response, id, flexible))
  })
  const negotiate = (address: Endpoint, api: number, version: number) => Effect.gen(function*() {
    const body = yield* raw(address, 18, 0, Buffer.alloc(0))
    yield* checked("native.versions", () => checkVersions(body, api, version))
  })
  const rpc = <K extends keyof Requests>(address: Endpoint, api: K, value: Requests[K]) => Effect.gen(function*() {
    const [key, version, flexible] = apis[api]
    yield* negotiate(address, key, version)
    const body = yield* checked("native.encode", () => encode(api, value, config.maxRequestBytes))
    const response = yield* raw(address, key, version, body, flexible)
    return yield* checked(`native.${api}`, () => {
      const result = decode(api, response)
      if ("errorCode" in result && !(api === "JoinGroup" && result.errorCode === 79)) brokerError(api, result.errorCode)
      return result
    })
  })
  const bootstrap = <A>(operation: (address: Endpoint) => Effect.Effect<A, KafkaError>) => Effect.gen(function*() {
    let last: KafkaError | undefined
    for (let attempt = 0; attempt <= config.metadataRetries; attempt++) {
      for (const address of config.brokers) {
        const result = yield* operation(address).pipe(Effect.map((value) => ({ value })), Effect.catchTag("KafkaError", (error) => Effect.succeed({ error })))
        if ("value" in result) return result.value
        last = result.error
        if (last.operation === "native.sasl" || (last.cause instanceof KafkaBrokerError && ![3, 5, 6, 14, 15, 16].includes(last.cause.code))) return yield* Effect.fail(last)
      }
      if (attempt < config.metadataRetries) yield* Effect.sleep(100 * (attempt + 1))
    }
    return yield* Effect.fail(last!)
  })
  const discover = (topic: string): Effect.Effect<Metadata, KafkaError> => bootstrap((broker) => Effect.gen(function*() {
    yield* negotiate(broker, 3, 4)
    const body = yield* raw(broker, 3, 4, metadataRequest(topic, config.autoCreate))
    return yield* checked("native.metadata", () => {
      const metadata = metadataResponse(body, topic)
      const unavailable = metadata.partitions.find((p) => p.error !== 0 || p.leader < 0)
      if (unavailable) throw new KafkaBrokerError({ api: "Metadata", code: unavailable.error || 5, topic, partition: unavailable.id })
      return metadata
    })
  }))
  const coordinator = (key: string, keyType: 0 | 1) => bootstrap((address) => Effect.gen(function*() {
    const response = yield* rpc(address, "FindCoordinator", { key, keyType })
    return { host: response.host, port: response.port }
  }))
  return { config, raw, negotiate, rpc, bootstrap, discover, coordinator }
})
export type Client = Effect.Success<ReturnType<typeof makeClient>>
