import * as Net from "node:net"
import * as Tls from "node:tls"
import { readFileSync } from "node:fs"
import { once } from "node:events"
import { Cause, Effect, Exit } from "effect"
import { recorder } from "./tracer.js"
import { expect, test } from "vitest"
import { exchange } from "../src/internal/transport.js"

const options = { requestTimeoutMs: 1000, maxResponseBytes: 1024 }
async function listen(server: Net.Server) {
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  return { host: "127.0.0.1", port: (server.address() as Net.AddressInfo).port }
}
const close = (server: Net.Server) => new Promise<void>((resolve) => server.close(() => resolve()))

test("TCP requests receive fragmented frames and release their sockets", async () => {
  const server = Net.createServer((socket) => socket.once("data", () => {
    socket.write(Buffer.from([0, 0]))
    setImmediate(() => socket.end(Buffer.from([0, 4, 0, 0, 0, 42])))
  }))
  const address = await listen(server)
  try { expect(await Effect.runPromise(exchange(address, Buffer.from([1]), options))).toEqual(Buffer.from([0, 0, 0, 42])) }
  finally { await close(server) }
})
test("an absolute timeout closes a silent broker connection", async () => {
  const server = Net.createServer((socket) => socket.resume())
  const address = await listen(server)
  try {
    const error = await Effect.runPromise(exchange(address, Buffer.from([1]), { ...options, requestTimeoutMs: 30 }).pipe(Effect.flip))
    expect(error.operation).toBe("native.request")
    expect(String(error.cause)).toContain("timed out")
  } finally { await close(server) }
})
test("interruption closes the socket without waiting for the request timeout", async () => {
  let connected!: () => void
  const ready = new Promise<void>((resolve) => { connected = resolve })
  const server = Net.createServer((socket) => { socket.resume(); connected() })
  const address = await listen(server)
  const controller = new AbortController()
  try {
    const result = Effect.runPromiseExit(exchange(address, Buffer.from([1]), { ...options, requestTimeoutMs: 60000 }), { signal: controller.signal })
    await ready
    controller.abort()
    expect(Exit.isFailure(await result)).toBe(true)
  } finally { await close(server) }
})
test("premature close and oversized responses fail rather than hanging", async () => {
  for (const frame of [Buffer.from([0, 0]), Buffer.from([127, 255, 255, 255])]) {
    const server = Net.createServer((socket) => socket.once("data", () => socket.end(frame)))
    const address = await listen(server)
    try {
      const result = await Effect.runPromiseExit(exchange(address, Buffer.from([1]), options))
      expect(Exit.isFailure(result)).toBe(true)
    } finally { await close(server) }
  }
})
test("TLS verifies broker certificates and accepts an explicitly trusted CA", async () => {
  const cert = readFileSync(new URL("./fixtures/localhost-cert.pem", import.meta.url))
  const key = readFileSync(new URL("./fixtures/localhost-key.pem", import.meta.url))
  const server = Tls.createServer({ cert, key }, (socket) => socket.once("data", () => socket.end(Buffer.from([0, 0, 0, 4, 0, 0, 0, 42]))))
  const address = await listen(server)
  try {
    expect(Exit.isFailure(await Effect.runPromiseExit(exchange(address, Buffer.from([1]), { ...options, tls: { servername: "localhost" } })))).toBe(true)
    expect(Exit.isFailure(await Effect.runPromiseExit(exchange(address, Buffer.from([1]), { ...options, tls: { ca: cert, servername: "wrong.example" } })))).toBe(true)
    expect(await Effect.runPromise(exchange(address, Buffer.from([1]), { ...options, tls: { ca: cert, servername: "localhost" } }))).toEqual(Buffer.from([0, 0, 0, 42]))
  } finally { await close(server) }
})

test("authentication spans end on interruption without recording credentials", async () => {
  let connected!: () => void
  const ready = new Promise<void>((resolve) => { connected = resolve })
  const server = Net.createServer((socket) => { socket.resume(); connected() })
  const address = await listen(server)
  const controller = new AbortController()
  const { tracer, spans } = recorder()
  try {
    const run = Effect.runPromiseExit(exchange(address, Buffer.from([1]), { ...options, requestTimeoutMs: 60000,
      sasl: { mechanism: "plain", username: "private-user", password: "private-password" }
    }).pipe(Effect.withTracer(tracer)), { signal: controller.signal })
    await ready; controller.abort()
    const exit = await run
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
    const auth = spans.find((s) => s.name === "kafka.native.authenticate")!
    expect(auth.status._tag).toBe("Ended")
    if (auth.status._tag === "Ended") expect(Exit.isFailure(auth.status.exit) && Cause.hasInterrupts(auth.status.exit.cause)).toBe(true)
    expect(auth.attributes.get("kafka.sasl.mechanism")).toBe("plain")
    expect(JSON.stringify([...auth.attributes])).not.toContain("private-")
  } finally { await close(server) }
})
