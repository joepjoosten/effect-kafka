import { SaslPrep } from "@effect-kafka/native"
import { Effect } from "effect"

export const prepared = SaslPrep.prepare("I\u00adX").pipe(
  Effect.catchTag("SaslPrepError", (error) => Effect.fail(error.reason))
)
export const synchronous = SaslPrep.prepareUnsafe("\u2168")
