import { Admin, Kafka, Message, Producer } from "kafkajs";

export class KafkaConfig {
  private kafka: Kafka;
  private producer: Producer;
  private admin: Admin;

  constructor(brokers: string[]) {
    this.kafka = new Kafka({
      clientId: "producer-service",
      brokers: brokers,
    });

    this.producer = this.kafka.producer();
    this.admin = this.kafka.admin();
  }

  async connect() {
    try {
      await this.producer.connect();
      await this.admin.connect();
    } catch (error: any) {
      throw new Error(`Error connecting to Kafka: ${error.message}`);
    }
  }

  async createTopics(topic: string) {
    try {
      const createdTopics = await this.admin.listTopics();
      if (!createdTopics.includes(topic)) {
        await this.admin.createTopics({
          topics: [{ topic }],
        });
        console.log(`Topics created: ${topic}`);
      }
      console.log(`Topics already exists: ${topic}`);
    } catch (error: any) {
      throw new Error(`Error creating topics: ${error.message}`);
    }
  }

  //   messages produce method

  async produceMessage(topic: string, messages: Message[]) {
    try {
      await this.producer.send({
        topic,
        messages,
      });
      console.log(`Messages produced: ${messages}`);
    } catch (error: any) {
      throw new Error(`Error producing messages: ${error.message}`);
    }
  }

  async disconnect() {
    try {
      await this.producer.disconnect();
      await this.admin.disconnect();
    } catch (error: any) {
      throw new Error(`Error disconnecting from Kafka: ${error.message}`);
    }
  }
}
