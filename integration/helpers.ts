import type KafkaJS from "kafkajs"

/** Wait for both metadata propagation and the partition leaders to serve reads. */
export async function createTopic(admin: KafkaJS.Admin, topic: string, numPartitions = 1): Promise<void> {
  await admin.createTopics({ waitForLeaders: false, topics: [{ topic, numPartitions, replicationFactor: 1 }] })
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] })
      const partitions = metadata.topics.find((entry) => entry.name === topic)?.partitions
      if (partitions?.length === numPartitions && partitions.every((p) => p.partitionErrorCode === 0 && p.leader >= 0)) {
        await admin.fetchTopicOffsets(topic)
        return
      }
    } catch (error) {
      if (typeof error !== "object" || error === null || !("code" in error) || ![3, 5, 6].includes(error.code as number)) throw error
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`Topic ${topic} did not acquire leaders within the fixture deadline`)
}
