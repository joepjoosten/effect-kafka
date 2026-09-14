import { createHash, createHmac, pbkdf2Sync } from "node:crypto"
import { Cause, Effect, Exit, Redacted } from "effect"
import { expect, test } from "vitest"
import { authenticate, type SaslOptions } from "../src/internal/sasl.js"
import { Reader, Writer } from "../src/internal/binary.js"

function server(mechanism: SaslOptions["mechanism"], invalid?: "nonce" | "signature" | "iterations" | "duplicate" | "credentials") {
  let first = "", challenge = "", verified = false
  const perform = (frame: Buffer) => Effect.sync(() => {
    const r = new Reader(frame.subarray(4)), api = r.i16()
    r.i16(); const id = r.i32(); r.string()
    const out = new Writer().i32(id)
    if (api === 17) { expect(r.string()).toBe(mechanism.toUpperCase()); return out.i16(0).i32(1).string(mechanism.toUpperCase()).finish() }
    expect(api).toBe(36)
    const payload = r.bytes()!.toString()
    let reply = ""
    if (mechanism === "plain") { expect(payload).toBe("\0user\0pencil"); verified = true }
    else if (payload.startsWith("n,,")) {
      first = payload.slice(3)
      const nonce = first.slice(first.indexOf(",r=") + 3)
      challenge = `r=${invalid === "nonce" ? "wrong" : nonce + "server"},s=c2FsdA==,i=${invalid === "iterations" ? "1000001" : "4096"}${invalid === "duplicate" ? ",i=4096" : ""}`
      reply = challenge
    } else {
      const hash = mechanism === "scram-sha-256" ? "sha256" : "sha512"
      const hmac = (key: Buffer, text: string) => createHmac(hash, key).update(text).digest()
      const salted = pbkdf2Sync("pencil", "salt", 4096, hash === "sha256" ? 32 : 64, hash)
      const message = `${first},${challenge},${payload.slice(0, payload.indexOf(",p="))}`
      const clientKey = hmac(salted, "Client Key")
      const clientSignature = hmac(createHash(hash).update(clientKey).digest(), message)
      expect(Buffer.from(payload.split(",p=")[1]!, "base64")).toEqual(Buffer.from(clientKey.map((b, i) => b ^ clientSignature[i]!)))
      verified = true
      reply = "v=" + (invalid === "signature" ? Buffer.alloc(clientKey.length) : hmac(hmac(salted, "Server Key"), message)).toString("base64")
    }
    return out.i16(invalid === "credentials" ? 58 : 0).string(invalid === "credentials" ? "secret pencil" : null).bytes(Buffer.from(reply)).i64(0n).finish()
  })
  return { perform, verified: () => verified }
}
for (const mechanism of ["plain", "scram-sha-256", "scram-sha-512"] as const) {
  test(`${mechanism}: verifies credentials and server signature`, async () => {
    const s = server(mechanism)
    await Effect.runPromise(authenticate({ mechanism, username: "user", password: Redacted.make("pencil") }, s.perform))
    expect(s.verified()).toBe(true)
  })
}
for (const invalid of ["nonce", "signature", "iterations", "duplicate", "credentials"] as const) {
  test(`SCRAM rejects ${invalid} without leaking credentials`, async () => {
    const s = server("scram-sha-256", invalid)
    const error = await Effect.runPromise(authenticate({ mechanism: "scram-sha-256", username: "user", password: "pencil" }, s.perform).pipe(Effect.flip))
    expect(error.operation).toBe("native.sasl")
    expect(String(error.cause)).toBe("Error: SASL authentication failed")
    expect(JSON.stringify(error)).not.toContain("pencil")
  })
}
test("authentication preserves interruption", async () => {
  const exit = await Effect.runPromiseExit(authenticate({ mechanism: "plain", username: "user", password: "pencil" }, () => Effect.interrupt))
  expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
})
