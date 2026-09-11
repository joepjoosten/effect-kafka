import { defineConfig } from "vitest/config"
export default defineConfig({
  resolve: { alias: {
    "@effect-kafka/core": new URL("./packages/core/src/index.ts", import.meta.url).pathname,
    "@effect-kafka/kafkajs": new URL("./packages/kafkajs/src/index.ts", import.meta.url).pathname,
    "@effect-kafka/confluent": new URL("./packages/confluent/src/index.ts", import.meta.url).pathname
  } },
  test: { include: ["test/**/*.test.ts"], exclude: ["**/node_modules/**", "**/dist/**"] }
})
