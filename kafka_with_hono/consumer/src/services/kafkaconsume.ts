import { KafkaConfig } from "../config/kafka.config";

export const consumeTestMessages = async (kafka: KafkaConfig) => {
  try {
    await kafka.consume((message) => {
      console.log(`recived  message: ${message}`);
    });
  } catch (error: any) {
    throw new Error(`Error consuming test messages: ${error.message}`);
  }
};
