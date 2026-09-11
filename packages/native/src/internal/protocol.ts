import { Data } from "effect"
import type { DeliveryReport } from "@effect-kafka/core"
import { Reader, Writer } from "./binary.js"
import { validateEndpoint, type Endpoint } from "./transport.js"

/** An error returned by a Kafka broker, preserved as KafkaError.cause. */
export class KafkaBrokerError extends Data.TaggedError("KafkaBrokerError")<{
  readonly api: string
  readonly code: number
  readonly topic?: string
  readonly partition?: number
}> {}
export interface Partition { readonly id: number; readonly leader: number; readonly error: number }
export interface Metadata { readonly brokers: ReadonlyMap<number, Endpoint>; readonly partitions: ReadonlyArray<Partition> }

export const frameRequest = (key: number, version: number, correlation: number, clientId: string, body: Buffer, limit: number): Buffer => {
  const request = new Writer(limit).i16(key).i16(version).i32(correlation).string(clientId).raw(body).finish()
  return new Writer(limit + 4).i32(request.length).raw(request).finish()
}
export const responseBody = (frame: Buffer, correlation: number): Buffer => {
  const reader = new Reader(frame)
  if (reader.i32() !== correlation) throw new Error("Kafka response correlation ID mismatch")
  return reader.take(reader.remaining)
}
export const checkVersions = (body: Buffer, api: number, version: number): void => {
  const reader = new Reader(body)
  const code = reader.i16()
  if (code !== 0) throw new KafkaBrokerError({ api: "ApiVersions", code })
  const versions = reader.array(() => ({ api: reader.i16(), min: reader.i16(), max: reader.i16() }))
  reader.end()
  if (!versions.some((entry) => entry.api === api && entry.min <= version && entry.max >= version)) {
    throw new Error(`Broker does not support Kafka API ${api} version ${version}`)
  }
}
export const metadataRequest = (topic: string, autoCreate: boolean): Buffer => new Writer().i32(1).string(topic).i8(autoCreate ? 1 : 0).finish()
export const metadataResponse = (body: Buffer, topic: string): Metadata => {
  const reader = new Reader(body)
  reader.i32() // throttle time
  const entries = reader.array(() => {
    const id = reader.i32()
    const host = reader.string()
    const port = reader.i32()
    reader.string(true) // rack
    return [id, validateEndpoint({ host, port })] as const
  })
  const brokers = new Map(entries)
  if (brokers.size !== entries.length) throw new Error("Duplicate broker in metadata")
  reader.string(true) // cluster ID
  reader.i32() // controller ID
  const topics = reader.array(() => {
    const error = reader.i16()
    const name = reader.string()
    reader.i8() // is_internal
    const partitions = reader.array(() => {
      const error = reader.i16(), id = reader.i32(), leader = reader.i32()
      reader.array(() => reader.i32()) // replicas
      reader.array(() => reader.i32()) // in-sync replicas
      return { error, id, leader }
    })
    return { error, name, partitions }
  })
  reader.end()
  const metadata = topics.find((entry) => entry.name === topic)
  if (!metadata) throw new Error("Requested topic missing from metadata")
  if (metadata.error !== 0) throw new KafkaBrokerError({ api: "Metadata", code: metadata.error, topic })
  if (metadata.partitions.length === 0) throw new Error("Topic has no partitions")
  const partitions = [...metadata.partitions].sort((a, b) => a.id - b.id)
  if (partitions.some((p, i) => p.id < 0 || (i > 0 && partitions[i - 1]!.id === p.id))) throw new Error("Invalid or duplicate partition in metadata")
  return { brokers, partitions }
}
export const produceRequest = (topic: string, batches: ReadonlyMap<number, Buffer>, acks: 1 | -1, timeout: number, limit: number): Buffer => {
  const writer = new Writer(limit).string(null).i16(acks).i32(timeout).i32(1).string(topic).i32(batches.size)
  for (const [partition, batch] of batches) writer.i32(partition).bytes(batch)
  return writer.finish()
}
export const produceResponse = (body: Buffer, topic: string, expected: ReadonlyArray<number>): ReadonlyArray<DeliveryReport> => {
  const reader = new Reader(body)
  const reports = reader.array(() => {
    const topicName = reader.string()
    return reader.array(() => {
      const partition = reader.i32(), errorCode = reader.i16(), baseOffset = reader.i64().toString()
      reader.i64() // log append time
      return { topicName, partition, errorCode, baseOffset }
    })
  }).flat()
  reader.i32() // throttle time; v3 brokers throttle before responding
  reader.end()
  const seen = new Set<number>()
  for (const report of reports) {
    if (report.topicName !== topic || !expected.includes(report.partition) || seen.has(report.partition)) throw new Error("Unexpected partition in Produce response")
    seen.add(report.partition)
    if (report.errorCode !== 0) throw new KafkaBrokerError({ api: "Produce", code: report.errorCode, topic, partition: report.partition })
    if (BigInt(report.baseOffset) < 0n) throw new Error("Invalid acknowledged offset")
  }
  if (seen.size !== expected.length) throw new Error("Missing partition in Produce response")
  return reports
}
