import KafkaJS from "kafkajs"
import type { KafkaConfig, ProducerConfig, ConsumerConfig } from "kafkajs"
import * as Core from "@effect-kafka/core"

export type { KafkaConfig, ProducerConfig, ConsumerConfig } from "kafkajs"
export const Partitioners = KafkaJS.Partitioners
export interface ProducerOptions {
  readonly client: KafkaConfig
  readonly producer?: ProducerConfig
}
export interface ConsumerOptions {
  readonly client: KafkaConfig
  readonly consumer: ConsumerConfig
  /** Defaults to true; set false and use record.commit for explicit commits. */
  readonly autoCommit?: boolean
}

const bytes = (value: Core.Bytes): string | Buffer => typeof value === "string" ? value : Buffer.from(value)
const headers = (value: Core.Headers): Record<string, string | Buffer | Array<string | Buffer> | undefined> =>
  Object.fromEntries(Object.entries(value).map(([key, entry]) => [key,
    entry === undefined ? undefined : typeof entry === "string" || entry instanceof Uint8Array
      ? bytes(entry) : entry.map(bytes)
  ]))
const message = (value: Core.Message) => ({
  ...(value.partition === undefined ? {} : { partition: value.partition }),
  ...(value.timestamp === undefined ? {} : { timestamp: value.timestamp }),
  value: value.value === null ? null : bytes(value.value),
  ...(value.key === undefined ? {} : { key: value.key === null ? null : bytes(value.key) }),
  ...(value.headers === undefined ? {} : { headers: headers(value.headers) })
})

export const producerLayer = (options: ProducerOptions) => Core.producerLayer(() => {
  const producer = new KafkaJS.Kafka(options.client).producer(options.producer)
  return {
    connect: () => producer.connect(),
    disconnect: () => producer.disconnect(),
    send: (record) => producer.send({ topic: record.topic, messages: record.messages.map(message) })
  }
})

export const consumerLayer = (options: ConsumerOptions) => Core.consumerLayer((subscription) => {
  const consumer = new KafkaJS.Kafka(options.client).consumer(options.consumer)
  return {
    connect: () => consumer.connect(),
    disconnect: () => consumer.disconnect(),
    subscribe: () => consumer.subscribe({
      topics: [...subscription.topics],
      ...(subscription.fromBeginning === undefined ? {} : { fromBeginning: subscription.fromBeginning })
    }),
    onFailure: (fail) => consumer.on(consumer.events.CRASH, (event) => {
      if (!event.payload.restart) fail(event.payload.error)
    }),
    run: (handler) => consumer.run({
      autoCommit: options.autoCommit ?? true,
      ...(subscription.partitionsConsumedConcurrently === undefined ? {} : {
        partitionsConsumedConcurrently: subscription.partitionsConsumedConcurrently
      }),
      eachMessage: ({ topic, partition, message, heartbeat }) => handler({
        topic, partition, offset: message.offset, timestamp: message.timestamp,
        key: message.key, value: message.value,
        ...(message.headers === undefined ? {} : { headers: message.headers }),
        heartbeat: Core.attempt("consumer.heartbeat", heartbeat),
        commit: Core.attempt("consumer.commit", () => consumer.commitOffsets([
          { topic, partition, offset: (BigInt(message.offset) + 1n).toString() }
        ]))
      })
    })
  }
})
