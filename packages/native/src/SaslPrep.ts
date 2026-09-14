import { Data, Effect } from "effect"
import { lCat, mappedToNothing, nonAsciiSpace, prohibited, randALCat, unassigned } from "./internal/saslprep-tables.js"

export interface Options {
  /** Permit Unicode 3.2 unassigned output for queries; false for stored credentials. */
  readonly allowUnassigned?: boolean
}
export type Reason = "InvalidInput" | "ProhibitedCharacter" | "UnassignedCodePoint" | "MixedDirection" | "InvalidDirectionBoundary"
/** Contains no input, normalized credentials, or underlying exception. */
export class SaslPrepError extends Data.TaggedError("SaslPrepError")<{
  readonly reason: Reason
}> {}

const contains = (ranges: ReadonlyArray<number>, point: number): boolean => {
  let low = 0, high = ranges.length / 2 - 1
  while (low <= high) {
    const middle = (low + high) >>> 1, index = middle * 2
    if (point < ranges[index]!) high = middle - 1
    else if (point > ranges[index + 1]!) low = middle + 1
    else return true
  }
  return false
}

/**
 * Prepare a credential using RFC 4013 mappings, NFKC, and Unicode 3.2 tables.
 * Preserves case. Throws SaslPrepError for invalid credentials.
 * Uses Node's NFKC implementation, as the previous SASLprep dependency did.
 */
export const prepareUnsafe = (input: string, options: Options = {}): string => {
  if (typeof input !== "string") throw new SaslPrepError({ reason: "InvalidInput" })
  const mapped: string[] = []
  for (const character of input) {
    const point = character.codePointAt(0)!
    // Space mapping precedes removal, preserving existing handling of U+200B.
    if (contains(nonAsciiSpace, point)) mapped.push(" ")
    else if (!contains(mappedToNothing, point)) mapped.push(character)
  }
  // Avoid spreading code points into a function call: long inputs remain valid.
  const normalized = mapped.join("").normalize("NFKC")
  let first = -1, last = -1, hasRandAL = false, hasL = false, hasUnassigned = false
  for (const character of normalized) {
    const point = character.codePointAt(0)!
    if (contains(prohibited, point)) throw new SaslPrepError({ reason: "ProhibitedCharacter" })
    hasUnassigned ||= contains(unassigned, point)
    hasRandAL ||= contains(randALCat, point)
    hasL ||= contains(lCat, point)
    if (first === -1) first = point
    last = point
  }
  if (options.allowUnassigned !== true && hasUnassigned) throw new SaslPrepError({ reason: "UnassignedCodePoint" })
  if (hasRandAL) {
    if (hasL) throw new SaslPrepError({ reason: "MixedDirection" })
    if (!contains(randALCat, first) || !contains(randALCat, last)) throw new SaslPrepError({ reason: "InvalidDirectionBoundary" })
  }
  return normalized
}

/** Lazily prepare a credential with a typed, credential-free failure channel. */
export const prepare = (input: string, options: Options = {}): Effect.Effect<string, SaslPrepError> =>
  Effect.try({
    try: () => prepareUnsafe(input, options),
    catch: (error) => error instanceof SaslPrepError ? error : new SaslPrepError({ reason: "InvalidInput" })
  })
