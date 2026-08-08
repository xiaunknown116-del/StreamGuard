// lib/kafkaClient.js
// Kafka client wrapper with connection pooling and error handling

const { Kafka } = require('kafkajs');
const config = require('../config');
const logger = require('../utils/logger');
const { KafkaConnectionError } = require('../utils/errors');

class KafkaClient {
  constructor() {
    this.kafka = null;
    this.producer = null;
    this.consumer = null;
    this.isConnected = false;
  }

  async initProducer() {
    if (this.producer) return this.producer;

    try {
      if (!this.kafka) {
        this.kafka = new Kafka({
          clientId: config.kafka.clientId,
          brokers: config.kafka.brokers,
          connectionTimeout: config.kafka.connectionTimeout,
          requestTimeout: config.kafka.requestTimeout,
          retry: {
            initialRetryTime: 100,
            retries: 8,
            maxRetryTime: 30000,
          },
        });
      }

      this.producer = this.kafka.producer();

      await this.producer.connect();
      logger.info('Kafka producer connected', {
        brokers: config.kafka.brokers,
        clientId: config.kafka.clientId,
      });

      return this.producer;
    } catch (error) {
      logger.error('Failed to initialize Kafka producer', error, {
        brokers: config.kafka.brokers,
      });
      throw new KafkaConnectionError('Failed to connect to Kafka brokers', {
        brokers: config.kafka.brokers,
        originalError: error.message,
      });
    }
  }

  async initConsumer() {
    if (this.consumer) return this.consumer;

    try {
      if (!this.kafka) {
        this.kafka = new Kafka({
          clientId: config.kafka.clientId,
          brokers: config.kafka.brokers,
          connectionTimeout: config.kafka.connectionTimeout,
          requestTimeout: config.kafka.requestTimeout,
          retry: {
            initialRetryTime: 100,
            retries: 8,
            maxRetryTime: 30000,
          },
        });
      }

      this.consumer = this.kafka.consumer({
        groupId: config.kafka.groupId,
        sessionTimeout: config.consumer.sessionTimeout,
      });

      await this.consumer.connect();
      logger.info('Kafka consumer connected', {
        groupId: config.kafka.groupId,
        brokers: config.kafka.brokers,
      });

      this.isConnected = true;
      return this.consumer;
    } catch (error) {
      logger.error('Failed to initialize Kafka consumer', error, {
        groupId: config.kafka.groupId,
      });
      throw new KafkaConnectionError('Failed to connect to Kafka brokers', {
        groupId: config.kafka.groupId,
        originalError: error.message,
      });
    }
  }

  async sendMessage(topic, message) {
    const producer = await this.initProducer();
    try {
      await producer.send({
        topic,
        messages: [{ value: JSON.stringify(message) }],
      });
      logger.debug('Message sent to Kafka', { topic, messageSize: JSON.stringify(message).length });
    } catch (error) {
      logger.error('Failed to send message to Kafka', error, { topic });
      throw new KafkaConnectionError(`Failed to send message to topic ${topic}`, {
        topic,
        originalError: error.message,
      });
    }
  }

  async disconnectProducer() {
    if (this.producer) {
      try {
        await this.producer.disconnect();
        this.producer = null;
        logger.info('Kafka producer disconnected');
      } catch (error) {
        logger.warn('Error disconnecting Kafka producer', { error: error.message });
      }
    }
  }

  async disconnectConsumer() {
    if (this.consumer) {
      try {
        await this.consumer.disconnect();
        this.consumer = null;
        this.isConnected = false;
        logger.info('Kafka consumer disconnected');
      } catch (error) {
        logger.warn('Error disconnecting Kafka consumer', { error: error.message });
      }
    }
  }

  async disconnect() {
    await Promise.all([this.disconnectProducer(), this.disconnectConsumer()]);
    this.kafka = null;
  }

  getConsumer() {
    if (!this.consumer) {
      throw new KafkaConnectionError('Kafka consumer not initialized');
    }
    return this.consumer;
  }

  isProducerConnected() {
    return !!this.producer;
  }

  isConsumerConnected() {
    return this.isConnected;
  }
}

module.exports = new KafkaClient();
