import { Effect, Exit } from "effect"
import { expect, test } from "vitest"
import { SaslPrep } from "../src/index.js"
import { prepare, prepareUnsafe, SaslPrepError } from "../src/SaslPrep.js"

for (const [input, output] of [["I\u00adX", "IX"], ["user", "user"], ["USER", "USER"], ["\u00aa", "a"], ["\u2168", "IX"]]) {
  test(`RFC 4013 example ${JSON.stringify(input)}`, () => expect(prepareUnsafe(input!)).toBe(output))
}
const rejects = (input: string, reason: SaslPrep.Reason, options?: SaslPrep.Options) => {
  try { prepareUnsafe(input, options); throw new Error("Expected rejection") }
  catch (error) { expect(error).toBeInstanceOf(SaslPrepError); expect(error).toMatchObject({ reason }) }
}
test("rejects RFC 4013 control and bidirectional examples", () => {
  rejects("\u0007", "ProhibitedCharacter")
  rejects("\u0627\u0031", "InvalidDirectionBoundary")
})
test("maps every non-ASCII space, preserving mapping precedence", () => {
  for (const point of [0xa0, 0x1680, ...Array.from({ length: 12 }, (_, i) => 0x2000 + i), 0x202f, 0x205f, 0x3000]) {
    expect(prepareUnsafe(`a${String.fromCodePoint(point)}b`)).toBe("a b")
  }
})
test("removes mapped-to-nothing characters and accepts fully removed strings", () => {
  for (const point of [0xad, 0x34f, 0x1806, 0x180b, 0x180c, 0x180d, 0x200c, 0x200d, 0x2060, ...Array.from({ length: 16 }, (_, i) => 0xfe00 + i), 0xfeff]) {
    expect(prepareUnsafe(String.fromCodePoint(point))).toBe("")
    expect(prepareUnsafe(`a${String.fromCodePoint(point)}b`)).toBe("ab")
  }
  expect(prepareUnsafe("")).toBe("")
})
test("normalizes compatibility characters, combining marks, and astral characters", () => {
  expect(prepareUnsafe("e\u0301")).toBe("é")
  expect(prepareUnsafe("\uff21\uff22\uff23")).toBe("ABC")
  expect(prepareUnsafe("\u{1d400}\u{1d401}")).toBe("AB")
  expect(prepareUnsafe("\u{10400}")).toBe("\u{10400}")
  expect(prepareUnsafe("\u1100\u1161")).toBe("가")
})
test("rejects each prohibited category, including all plane-ending noncharacters", () => {
  for (const point of [0x00, 0x7f, 0x85, 0x6dd, 0xe000, 0xf0000, 0x100000, 0xfdd0, 0xfdef, 0xd800, 0xdfff, 0xfff9, 0xfffd + 1, 0x2ff0, 0x2ffb, 0x340, 0x200e, 0x202e, 0xe0001, 0xe0020]) {
    // U+0340 normalizes to a permitted combining mark before prohibition.
    if (point === 0x340) { expect(prepareUnsafe("\u0340")).toBe("\u0300"); continue }
    rejects(String.fromCodePoint(point), "ProhibitedCharacter")
  }
  for (let plane = 0; plane <= 16; plane++) {
    rejects(String.fromCodePoint(plane * 0x10000 + 0xfffe), "ProhibitedCharacter")
    rejects(String.fromCodePoint(plane * 0x10000 + 0xffff), "ProhibitedCharacter")
  }
})
test("checks Unicode 3.2 unassigned output and supports query mode", () => {
  for (const input of ["\u0221", "\u0234", "\u{1f600}"]) {
    rejects(input, "UnassignedCodePoint")
    expect(prepareUnsafe(input, { allowUnassigned: true })).toBe(input)
  }
  rejects("\u0007", "ProhibitedCharacter", { allowUnassigned: true })
})
test("checks both bidi categories and first/last code points", () => {
  expect(prepareUnsafe("\u0627\u0031\u0628")).toBe("\u0627\u0031\u0628")
  expect(prepareUnsafe("\u05d0-\u05d1")).toBe("\u05d0-\u05d1")
  rejects("\u0627a\u0628", "MixedDirection")
  rejects("1\u0627", "InvalidDirectionBoundary")
  rejects("\u06271", "InvalidDirectionBoundary")
  rejects("\u0627\u{10400}\u0628", "MixedDirection")
  expect(prepareUnsafe("\u00ad\u0627\u00ad")).toBe("\u0627")
})
test("handles long strings without function argument limits", () => {
  const value = "\u00aa".repeat(200000)
  expect(prepareUnsafe(value)).toBe("a".repeat(200000))
})
test("validates runtime input types", () => {
  for (const input of [undefined, null, 42, {}, new Uint8Array()]) rejects(input as unknown as string, "InvalidInput")
})
test("Effect and synchronous entrypoints agree and are exported as a module", async () => {
  expect(SaslPrep.prepare).toBe(prepare)
  expect(SaslPrep.prepareUnsafe).toBe(prepareUnsafe)
  expect(await Effect.runPromise(prepare("I\u00adX"))).toBe("IX")
  const error = await Effect.runPromise(prepare("secret-password\u0007").pipe(Effect.flip))
  expect(error).toMatchObject({ _tag: "SaslPrepError", reason: "ProhibitedCharacter" })
  expect(JSON.stringify(error)).not.toContain("secret-password")
  expect(String(error)).not.toContain("secret-password")
  expect(Exit.isFailure(await Effect.runPromiseExit(prepare("\u0221")))).toBe(true)
})
