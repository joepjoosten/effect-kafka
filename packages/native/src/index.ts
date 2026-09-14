export { producerLayer, type ProducerOptions } from "./internal/producer.js"
export { KafkaBrokerError } from "./internal/protocol.js"
export type { SaslOptions } from "./internal/sasl.js"
export type { ClientOptions } from "./internal/client.js"
export type { Compression } from "./internal/records.js"

export { consumerLayer, type ConsumerOptions } from "./consumer.js"
export { Transactions, transactionLayer, type Transaction, type TransactionOptions, type TransactionOffset } from "./transactions.js"

export * as SaslPrep from "./SaslPrep.js"
