export type { ClientOptions } from "./internal/client.js"
export { producerLayer, type ProducerOptions } from "./internal/producer.js"
export { KafkaBrokerError } from "./internal/protocol.js"
export type { Compression } from "./internal/records.js"
export type { SaslOptions } from "./internal/sasl.js"

export { consumerLayer, type ConsumerOptions } from "./consumer.js"
export {
  type Transaction,
  transactionLayer,
  type TransactionOffset,
  type TransactionOptions,
  Transactions
} from "./transactions.js"

export * as SaslPrep from "./SaslPrep.js"
