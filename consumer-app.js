// consumer-app.js
// Refactored consumer application with improved lifecycle management

const config = require('./config');
const logger = require('./utils/logger');
const metrics = require('./utils/metrics');
const mongodbClient = require('./lib/mongodbClient');
const consumerService = require('./services/consumerService');

let shutdownInProgress = false;

async function start() {
  try {
    logger.info('Starting StreamGuard Consumer', {
      kafkabrokers: config.kafka.brokers,
      topic: config.kafka.topic,
      maxBufferSize: config.consumer.maxBufferSize,
      flushIntervalMs: config.consumer.flushIntervalMs,
    });

    // Connect to MongoDB
    await mongodbClient.connect();
    metrics.setGauge('mongoConnected', true);

    // Start consumer
    await consumerService.start();
    metrics.setGauge('kafkaConnected', true);

    logger.info('Consumer started successfully');
  } catch (error) {
    logger.error('Failed to start consumer', error);
    process.exit(1);
  }
}

async function shutdown() {
  if (shutdownInProgress) return;
  shutdownInProgress = true;

  logger.info('Shutting down consumer');

  try {
    await consumerService.stop();
    logger.info('Consumer stopped');

    await mongodbClient.disconnect();
    logger.info('MongoDB disconnected');

    logger.info('Graceful shutdown complete');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', error);
    process.exit(1);
  }
}

// Handle signals
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', error);
  shutdown();
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled rejection', new Error(String(reason)), {
    promise: promise.toString(),
  });
});

start();
