import Confluent from "@confluentinc/kafka-javascript"
import type { KafkaJS } from "@confluentinc/kafka-javascript"
import * as Core from "@effect-kafka/core"

export type ProducerOptions = {
  readonly client?: KafkaJS.CommonConstructorConfig
  readonly producer?: KafkaJS.ProducerConstructorConfig
}
export type ConsumerOptions = {
  readonly client?: KafkaJS.CommonConstructorConfig
  readonly consumer: KafkaJS.ConsumerConstructorConfig
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
  const producer = new Confluent.KafkaJS.Kafka(options.client).producer(options.producer)
  return {
    connect: () => producer.connect(),
    disconnect: () => producer.disconnect(),
    send: (record) => producer.send({ topic: record.topic, messages: record.messages.map(message) })
  }
})

export const consumerLayer = (options: ConsumerOptions) => Core.consumerLayer((subscription) => {
  // Confluent configures offset reset at construction, unlike KafkaJS.subscribe.
  const consumer = new Confluent.KafkaJS.Kafka(options.client).consumer({
    ...options.consumer,
    ...(subscription.fromBeginning === undefined ? {} : {
      "auto.offset.reset": subscription.fromBeginning ? "earliest" : "latest"
    })
  })
  return {
    connect: () => consumer.connect(),
    disconnect: () => consumer.disconnect(),
    subscribe: () => consumer.subscribe({ topics: [...subscription.topics] }),
    run: (handler) => consumer.run({
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
