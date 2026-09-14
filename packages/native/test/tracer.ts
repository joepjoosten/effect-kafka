import { Tracer } from "effect"

export const recorder = () => {
  const spans: Tracer.NativeSpan[] = []
  const tracer = Tracer.make({ span(options) {
    const span = new Tracer.NativeSpan(options)
    spans.push(span)
    return span
  } })
  return { spans, tracer }
}
