import { KafkaConfig } from "../config/kafka..config";


// Path: producer/src/services/start.services.ts
export const kafka = new KafkaConfig(["localhost:9092"]);


export const startServices = async () => {
  try {
    await kafka.connect();
    await kafka.createTopics("test-topic");
  } catch (error: any) {
    console.log(`Error starting services: ${error.message}`);
    process.exit(1);
  }
};
