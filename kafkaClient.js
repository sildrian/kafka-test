import { Kafka } from 'kafkajs';
import {} from 'dotenv/config'

class KafkaClient {
  constructor(clientId, brokers) {
    this.kafka = new Kafka({
      clientId,
      brokers: brokers || [`${process.env.KAFKA_BROKERS}`]
    });
    this.producer = null;
    this.consumer = null;
  }

  async connectProducer() {
    try {
      this.producer = this.kafka.producer();
      await this.producer.connect();
      console.log('Producer connected successfully');
    } catch (error) {
      console.error('Error connecting producer:', error);
      throw error;
    }
  }

  async sendMessage(topic, message) {
    try {
      if (!this.producer) {
        await this.connectProducer();
      }

      await this.producer.send({
        topic,
        messages: [
          {
            value: typeof message === 'string' ? message : JSON.stringify(message)
          }
        ]
      });

      console.log('Message sent successfully to topic:', topic);
      return true;
    } catch (error) {
      console.error('Error sending message:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      if (this.producer) {
        await this.producer.disconnect();
      }
      if (this.consumer) {
        await this.consumer.disconnect();
      }
      console.log('Disconnected from Kafka');
    } catch (error) {
      console.error('Error disconnecting:', error);
      throw error;
    }
  }
}

export default KafkaClient;
