import express from 'express';
import bodyParser from 'body-parser';
import dotenv from 'dotenv';
import KafkaClient from './kafkaClient.js';

// Initialize environment variables
dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(bodyParser.json());

// Initialize Kafka client
const kafkaClient = new KafkaClient('api-producer', [`${process.env.KAFKA_BROKERS}`]);

// API Routes
app.post('/api/messages', async (req, res) => {
  try {
    const { topic, message } = req.body;

    if (!topic || !message) {
      return res.status(400).json({ 
        error: 'Both topic and message are required' 
      });
    }

    await kafkaClient.sendMessage(topic, message);

    res.status(200).json({ 
      success: true, 
      message: 'Message sent successfully' 
    });
  } catch (error) {
    console.error('Error in /api/messages:', error);
    res.status(500).json({ 
      error: 'Failed to send message',
      details: error.message 
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK' });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ 
    error: 'Something broke!',
    details: err.message 
  });
});

// Start server
const server = app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

// Graceful shutdown
const shutdown = async () => {
  console.log('Shutting down server...');
  await kafkaClient.disconnect();
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
