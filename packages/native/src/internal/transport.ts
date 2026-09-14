import * as Net from "node:net"
import * as Tls from "node:tls"
import { Effect } from "effect"
import { KafkaError } from "@effect-kafka/core"
import { authenticate, type SaslOptions } from "./sasl.js"
import { FrameDecoder } from "./binary.js"

export interface Endpoint { readonly host: string; readonly port: number }
export interface TransportOptions {
  readonly sasl?: SaslOptions
  readonly requestTimeoutMs: number
  readonly maxResponseBytes: number
  readonly tls?: Omit<Tls.ConnectionOptions, "host" | "port" | "socket" | "path">
}
export const endpoint = (address: string): Endpoint => {
  const match = /^(?:\[([^\]]+)\]|([^:\s/]+)):(\d+)$/.exec(address)
  if (!match) throw new Error(`Invalid broker address: ${address}; expected host:port or [IPv6]:port`)
  return validateEndpoint({ host: match[1] ?? match[2]!, port: Number(match[3]) })
}
export const validateEndpoint = (value: Endpoint): Endpoint => {
  if (!value.host || !Number.isInteger(value.port) || value.port < 1 || value.port > 65535) throw new Error("Invalid broker host or port")
  return value
}

/** One request per scoped socket: bounded memory, absolute deadlines, and interruption cleanup. */
export const exchange = (address: Endpoint, request: Buffer, options: TransportOptions): Effect.Effect<Buffer, KafkaError> =>
  Effect.scoped(Effect.gen(function*() {
    const socket = yield* Effect.acquireRelease(
      Effect.try({
        try: () => {
          const socket = options.tls === undefined
            ? Net.createConnection(address)
            : Tls.connect({ ...options.tls, host: address.host, port: address.port,
              ...(!Net.isIP(address.host) && options.tls.servername === undefined ? { servername: address.host } : {}) })
          // Keep an error listener until close, including after interruption removes request listeners.
          socket.on("error", () => {})
          return socket
        },
        catch: (cause) => new KafkaError({ operation: "native.connect", cause })
      }),
      (socket) => Effect.sync(() => { socket.destroy() })
    )
    let connected = false
    const perform = (request: Buffer) => Effect.suspend(() => {
    let cleanup = () => {}
    return Effect.callback<Buffer, KafkaError>((resume) => {
      const decoder = new FrameDecoder(options.maxResponseBytes)
      let ended = false
      const finish = (result: Effect.Effect<Buffer, KafkaError>) => {
        if (ended) return
        ended = true
        resume(result)
      }
      const fail = (cause: unknown) => finish(Effect.fail(new KafkaError({ operation: "native.request", cause })))
      const onData = (chunk: Buffer) => {
        try {
          const frame = decoder.push(chunk)
          if (frame !== undefined) finish(Effect.succeed(frame))
        } catch (cause) { fail(cause) }
      }
      const onClose = () => fail(new Error("Broker closed before a complete response"))
      const onConnect = () => {
        connected = true
        try { socket.write(request) } catch (cause) { fail(cause) }
      }
      const readyEvent = options.tls === undefined ? "connect" : "secureConnect"
      const timer = setTimeout(() => fail(new Error(`Kafka request timed out after ${options.requestTimeoutMs}ms`)), options.requestTimeoutMs)
      socket.on("data", onData)
      socket.once("error", fail)
      socket.once("close", onClose)
      if (connected) onConnect()
      else socket.once(readyEvent, onConnect)
      cleanup = () => {
        ended = true
        clearTimeout(timer)
        socket.off("data", onData)
        socket.off("error", fail)
        socket.off("close", onClose)
        socket.off(readyEvent, onConnect)
      }
    }).pipe(Effect.ensuring(Effect.sync(() => cleanup())))
    })
    if (options.sasl) yield* authenticate(options.sasl, perform)
    return yield* perform(request)
  }))
