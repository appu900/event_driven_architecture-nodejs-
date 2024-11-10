import { Admin, Consumer, Kafka, Message } from "kafkajs";

export class KafkaConfig {
  private kafka: Kafka;
  private consumer: Consumer;

  constructor(brokers: string[]) {
    this.kafka = new Kafka({
      clientId: "producer-service",
      brokers: brokers,
    });
    this.consumer = this.kafka.consumer({
      groupId: "test-group",
    });
  }

  async connect() {
    try {
      await this.consumer.connect();
    } catch (error: any) {
      throw new Error(`Error connecting to Kafka: ${error.message}`);
    }
  }

  // ** method for subscribing the topics

  async subscribe(topic: string) {
    try {
      await this.consumer.subscribe({ topic, fromBeginning: true });
      console.log(`Subscribed to topics: ${topic}`);
    } catch (error: any) {
      throw new Error(`Error subscribing to topics: ${error.message}`);
    }
  }

  // ** messages consuming method

  async consume(onMessage: (message: string) => void) {
    try {
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          message?.value && onMessage(message?.value?.toString());
        },
      });
    } catch (error: any) {
      throw new Error(`Error consuming messages: ${error.message}`);
    }
  }

  async disconnect() {
    try {
      await this.consumer.disconnect();
    } catch (error: any) {
      throw new Error(`Error disconnecting from Kafka: ${error.message}`);
    }
  }
}
