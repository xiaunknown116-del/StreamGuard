// config/index.js
// Centralized configuration management with validation

require('dotenv').config();

const config = {
  // Server Configuration
  server: {
    port: parseInt(process.env.PORT, 10) || 3000,
    nodeEnv: process.env.NODE_ENV || 'development',
  },

  // Kafka Configuration
  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || 'streamguard-client',
    brokers: (process.env.KAFKA_BROKER || 'localhost:9092').split(',').map(b => b.trim()),
    topic: process.env.KAFKA_TOPIC || 'location',
    groupId: process.env.KAFKA_GROUP_ID || 'streamguard-group',
    connectionTimeout: parseInt(process.env.KAFKA_CONNECTION_TIMEOUT, 10) || 10000,
    requestTimeout: parseInt(process.env.KAFKA_REQUEST_TIMEOUT, 10) || 30000,
  },

  // MongoDB Configuration
  mongodb: {
    url: process.env.MONGO_URL || 'mongodb://localhost:27017/streamguard',
    connectionTimeout: parseInt(process.env.MONGO_CONNECTION_TIMEOUT, 10) || 10000,
    socketTimeout: parseInt(process.env.MONGO_SOCKET_TIMEOUT, 10) || 45000,
  },

  // Consumer Configuration
  consumer: {
    maxBufferSize: parseInt(process.env.MAX_DATA_LENGTH_BUFFER, 10) || 100,
    flushIntervalMs: parseInt(process.env.CONSUMER_FLUSH_INTERVAL_MS, 10) || 5000,
    maxRetries: parseInt(process.env.CONSUMER_INSERT_MAX_RETRIES, 10) || 3,
    initialRetryDelayMs: parseInt(process.env.CONSUMER_INITIAL_RETRY_DELAY_MS, 10) || 1000,
    sessionTimeout: parseInt(process.env.CONSUMER_SESSION_TIMEOUT, 10) || 30000,
  },

  // Logging Configuration
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    format: process.env.LOG_FORMAT || 'json', // 'json' or 'text'
  },

  // Metrics Configuration
  metrics: {
    enabled: process.env.METRICS_ENABLED === 'true' || false,
    port: parseInt(process.env.METRICS_PORT, 10) || 9090,
  },
};

// Validation function
function validateConfig() {
  const errors = [];

  // Validate required Kafka settings
  if (!config.kafka.brokers || config.kafka.brokers.length === 0) {
    errors.push('KAFKA_BROKER is required and must be a valid broker address');
  }

  // Validate required MongoDB settings
  if (!config.mongodb.url) {
    errors.push('MONGO_URL is required');
  }

  // Validate consumer buffer size
  if (config.consumer.maxBufferSize <= 0) {
    errors.push('MAX_DATA_LENGTH_BUFFER must be a positive integer');
  }

  // Validate consumer flush interval
  if (config.consumer.flushIntervalMs <= 0) {
    errors.push('CONSUMER_FLUSH_INTERVAL_MS must be a positive integer');
  }

  if (errors.length > 0) {
    throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
  }
}

// Validate on module load
validateConfig();

module.exports = config;
