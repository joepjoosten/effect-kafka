import { createHash, createHmac, pbkdf2, randomBytes, timingSafeEqual } from "node:crypto"
import saslprep from "@mongodb-js/saslprep"
import { Cause, Effect, Redacted } from "effect"
import { KafkaError } from "@effect-kafka/core"
import { Reader, Writer } from "./binary.js"
import { frameRequest, responseBody } from "./protocol.js"

export interface SaslOptions {
  readonly mechanism: "plain" | "scram-sha-256" | "scram-sha-512"
  readonly username: string
  readonly password: string | Redacted.Redacted<string>
}
const attributes = (text: string): Map<string, string> => {
  const result = new Map<string, string>()
  for (const item of text.split(",")) {
    if (item.length < 3 || item[1] !== "=" || result.has(item[0]!)) throw new Error("Invalid SCRAM attributes")
    result.set(item[0]!, item.slice(2))
  }
  if (result.has("m")) throw new Error("Unsupported SCRAM extension")
  return result
}
const base64 = (text: string): Buffer => {
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(text)) throw new Error("Invalid SCRAM base64")
  const value = Buffer.from(text, "base64")
  if (value.toString("base64").replace(/=+$/, "") !== text.replace(/=+$/, "")) throw new Error("Invalid SCRAM base64")
  return value
}
export const authenticate = (options: SaslOptions, perform: (request: Buffer) => Effect.Effect<Buffer, KafkaError>): Effect.Effect<void, KafkaError> =>
  Effect.gen(function*() {
    const mechanism = options.mechanism.toUpperCase()
    if (!["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"].includes(mechanism)) throw new Error("Unsupported SASL mechanism")
    let id = -1
    const call = (api: number, body: Buffer) => Effect.gen(function*() {
      const correlation = id--
      const result = yield* perform(frameRequest(api, 1, correlation, "effect-kafka-sasl", body, 65536))
      return responseBody(result, correlation)
    })
    const handshake = new Reader(yield* call(17, new Writer().string(mechanism).finish()))
    const code = handshake.i16()
    const mechanisms = handshake.array(() => handshake.string())
    handshake.end()
    if (code !== 0 || !mechanisms.includes(mechanism)) throw new Error("SASL handshake rejected")
    const auth = (payload: string) => Effect.gen(function*() {
      const reader = new Reader(yield* call(36, new Writer(65536).bytes(Buffer.from(payload)).finish()))
      const code = reader.i16()
      reader.string(true) // Never expose authentication server messages or credentials in failures.
      const data = reader.bytes()
      reader.i64()
      reader.end()
      if (code !== 0 || data === null) throw new Error("SASL credentials rejected")
      return data.toString("utf8")
    })
    const password = Redacted.isRedacted(options.password) ? Redacted.value(options.password) : options.password
    if (options.username.includes("\0") || password.includes("\0")) throw new Error("Invalid SASL credential")
    if (mechanism === "PLAIN") { yield* auth(`\0${options.username}\0${password}`); return }
    const hash = mechanism === "SCRAM-SHA-256" ? "sha256" : "sha512"
    const size = hash === "sha256" ? 32 : 64
    const nonce = randomBytes(24).toString("base64")
    const username = saslprep(options.username).replace(/=/g, "=3D").replace(/,/g, "=2C")
    const first = `n=${username},r=${nonce}`
    const serverFirst = yield* auth(`n,,${first}`)
    const fields = attributes(serverFirst)
    const serverNonce = fields.get("r") ?? ""
    const iterations = Number(fields.get("i"))
    if (!serverNonce.startsWith(nonce) || serverNonce.length <= nonce.length || serverNonce.length > 4096 ||
      !/^\d+$/.test(fields.get("i") ?? "") || !Number.isInteger(iterations) || iterations < 4096 || iterations > 1000000) throw new Error("Invalid SCRAM challenge")
    const salt = base64(fields.get("s") ?? "")
    if (salt.length > 4096) throw new Error("SCRAM salt too large")
    const salted = yield* Effect.promise(() => new Promise<Buffer>((resolve, reject) => {
      pbkdf2(saslprep(password), salt, iterations, size, hash, (error, key) => error ? reject(error) : resolve(key))
    }))
    const hmac = (key: Buffer, value: string) => createHmac(hash, key).update(value).digest()
    const clientKey = hmac(salted, "Client Key")
    const stored = createHash(hash).update(clientKey).digest()
    const final = `c=biws,r=${serverNonce}`
    const message = `${first},${serverFirst},${final}`
    const signature = hmac(stored, message)
    const proof = Buffer.from(clientKey.map((byte, index) => byte ^ signature[index]!))
    const expected = hmac(hmac(salted, "Server Key"), message)
    const serverFinal = attributes(yield* auth(`${final},p=${proof.toString("base64")}`))
    const actual = base64(serverFinal.get("v") ?? "")
    if (serverFinal.has("e") || actual.length !== expected.length || !timingSafeEqual(actual, expected)) throw new Error("SCRAM server verification failed")
  }).pipe(Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.failCause(cause) : Effect.fail(new KafkaError({ operation: "native.sasl", cause: new Error("SASL authentication failed") }))))
