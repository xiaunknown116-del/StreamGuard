// services/consumerService.js
// Consumer business logic with buffering, batching, and error handling

const config = require('../config');
const logger = require('../utils/logger');
const metrics = require('../utils/metrics');
const { BatchInsertError, MessageParseError, DeadLetterError } = require('../utils/errors');
const Location = require('../models/Location');
const DeadLetter = require('../models/DeadLetter');
const kafkaClient = require('../lib/kafkaClient');

class ConsumerService {
  constructor() {
    this.buffer = [];
    this.flushTimer = null;
    this.isProcessing = false;
    this.errorCount = 0;
  }

  async start() {
    const consumer = await kafkaClient.initConsumer();

    try {
      await consumer.subscribe({ topic: config.kafka.topic });
      logger.info('Subscribed to Kafka topic', { topic: config.kafka.topic });

      // Start periodic flush timer
      this.startFlushTimer();

      await consumer.run({
        eachMessage: (message) => this.handleMessage(message),
        autoCommit: false, // Manual offset management
      });
    } catch (error) {
      logger.error('Consumer failed to start', error);
      throw error;
    }
  }

  startFlushTimer() {
    this.flushTimer = setInterval(() => {
      if (this.buffer.length > 0) {
        logger.debug('Flush interval triggered', {
          bufferSize: this.buffer.length,
          interval: config.consumer.flushIntervalMs,
        });
        this.flushBuffer();
      }
    }, config.consumer.flushIntervalMs);
  }

  async handleMessage({ topic, partition, message }) {
    const startTime = Date.now();

    try {
      // Parse message
      const location = this.parseMessage(message);
      metrics.incrementCounter('messagesReceived');

      // Add to buffer
      this.buffer.push(location);
      metrics.setGauge('bufferSize', this.buffer.length);

      logger.debug('Message buffered', {
        bufferSize: this.buffer.length,
        maxSize: config.consumer.maxBufferSize,
        location,
      });

      // Check if buffer is full
      if (this.buffer.length >= config.consumer.maxBufferSize) {
        await this.flushBuffer();
      }

      const latency = Date.now() - startTime;
      metrics.recordLatency('processingLatencies', latency);
      metrics.incrementCounter('messagesProcessed');
    } catch (error) {
      logger.error('Error handling message', error, {
        topic,
        partition,
        offset: message.offset,
      });
      metrics.incrementCounter('messagesErrored');
      this.errorCount++;
    }
  }

  parseMessage(message) {
    try {
      const location = JSON.parse(message.value.toString());

      // Validate required fields
      if (location.latitude === undefined || location.longitude === undefined) {
        throw new Error('Message must contain latitude and longitude');
      }

      return location;
    } catch (error) {
      throw new MessageParseError('Failed to parse message', {
        message: message.value.toString(),
        originalError: error.message,
      });
    }
  }

  async flushBuffer() {
    if (this.isProcessing || this.buffer.length === 0) return;

    this.isProcessing = true;
    const startTime = Date.now();

    try {
      // Take snapshot and clear buffer
      const batch = this.buffer.splice(0, this.buffer.length);
      metrics.setGauge('bufferSize', this.buffer.length);

      logger.info('Flushing batch to MongoDB', { batchSize: batch.length });

      // Insert with retries
      await this.insertBatchWithRetries(batch);

      const latency = Date.now() - startTime;
      metrics.recordLatency('insertLatencies', latency);
      metrics.incrementCounter('batchesInserted');

      logger.info('Batch inserted successfully', {
        batchSize: batch.length,
        latencyMs: latency,
      });
    } catch (error) {
      logger.error('Failed to flush buffer', error, {
        bufferSize: this.buffer.length,
      });
      metrics.incrementCounter('batchesFailed');
    } finally {
      this.isProcessing = false;
    }
  }

  async insertBatchWithRetries(batch, attempt = 0) {
    try {
      await Location.insertMany(batch);
    } catch (error) {
      attempt++;

      if (attempt < config.consumer.maxRetries) {
        const delay = config.consumer.initialRetryDelayMs * Math.pow(2, attempt - 1);
        logger.warn('Insert attempt failed, retrying', {
          attempt,
          delay,
          batchSize: batch.length,
          error: error.message,
        });

        await new Promise((resolve) => setTimeout(resolve, delay));
        return this.insertBatchWithRetries(batch, attempt);
      }

      // All retries exhausted, write to DLQ
      logger.error('All retry attempts exhausted, writing to DLQ', error, {
        batchSize: batch.length,
      });

      await this.writeToDLQ(batch, error);
    }
  }

  async writeToDLQ(batch, error) {
    try {
      const dlqEntries = batch.map((doc) => ({
        payload: doc,
        error: error.message,
        errorCode: error.code,
        retryCount: config.consumer.maxRetries,
      }));

      await DeadLetter.insertMany(dlqEntries);
      metrics.incrementCounter('dlqInserted');

      logger.info('Documents written to DLQ', { count: dlqEntries.length });
    } catch (dlqError) {
      logger.error('Failed to write to DLQ', dlqError, {
        batchSize: batch.length,
      });
      throw new DeadLetterError('Failed to write batch to dead letter queue', {
        batchSize: batch.length,
        originalError: dlqError.message,
      });
    }
  }

  async stop() {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
    }

    // Flush remaining buffer
    if (this.buffer.length > 0) {
      logger.info('Flushing remaining buffer before shutdown', {
        bufferSize: this.buffer.length,
      });
      await this.flushBuffer();
    }

    await kafkaClient.disconnectConsumer();
  }
}

module.exports = new ConsumerService();
