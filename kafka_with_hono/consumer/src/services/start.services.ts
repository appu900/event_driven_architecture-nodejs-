import { KafkaConfig } from "../config/kafka.config";
import { consumeTestMessages } from "./kafkaconsume";

export const startServices = async () => {
  try {
    const kafka = new KafkaConfig(["localhost:9092"]);
    await kafka.connect();
    await kafka.subscribe("test-topic");
    await consumeTestMessages(kafka);
  } catch (error: any) {
    throw new Error(`Error starting services: ${error.message}`);
  }
};
