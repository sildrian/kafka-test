import { Kafka } from 'kafkajs';
import {} from 'dotenv/config'

class MessageConsumer {
  constructor(groupId, topics) {
    this.kafka = new Kafka({
      clientId: 'api-consumer',
      brokers: [`${process.env.KAFKA_BROKERS}`]
    });
    
    this.consumer = this.kafka.consumer({ groupId });
    this.topics = topics;
  }

  async connect() {
    try {
      await this.consumer.connect();
      console.log('Consumer connected successfully');

      // Subscribe to topics
      for (const topic of this.topics) {
        await this.consumer.subscribe({
          topic,
          fromBeginning: true
        });
      }

      await this.startConsuming();
    } catch (error) {
      console.error('Error in consumer:', error);
      throw error;
    }
  }

  async startConsuming() {
    try {
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const value = message.value.toString();
            console.log('Received message:', {
              topic,
              partition,
              offset: message.offset,
              value: value
            });

            await this.processMessage(value);
          } catch (error) {
            console.error('Error processing message:', error);
          }
        }
      });
    } catch (error) {
      console.error('Error starting consumer:', error);
      throw error;
    }
  }

  async processMessage(message) {
    try {
      const data = JSON.parse(message);
      console.log('Processed data:', data);
      // Add your message processing logic here
    } catch (error) {
      console.error('Error processing message:', error);
    }
  }

  async disconnect() {
    try {
      await this.consumer.disconnect();
      console.log('Consumer disconnected successfully');
    } catch (error) {
      console.error('Error disconnecting consumer:', error);
    }
  }
}

// Create and start the consumer
const consumer = new MessageConsumer(
  process.env.KAFKA_CONSUMER_GROUP_ID || 'api-consumer-group',
  ['test-topic']
);

const start = async () => {
  try {
    await consumer.connect();
  } catch (error) {
    console.error('Failed to start consumer:', error);
    process.exit(1);
  }
};

// Graceful shutdown
const shutdown = async () => {
  try {
    await consumer.disconnect();
    console.log('Consumer shutdown complete');
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Start the consumer
start();
