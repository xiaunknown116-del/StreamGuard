// server.js
// Refactored Express server with improved error handling and health checks

const express = require('express');
const bodyParser = require('body-parser');
const config = require('./config');
const logger = require('./utils/logger');
const metrics = require('./utils/metrics');
const mongodbClient = require('./lib/mongodbClient');
const kafkaClient = require('./lib/kafkaClient');
const producerService = require('./services/producerService');

const app = express();

// Middleware
app.use(bodyParser.json());

// Request logging middleware
app.use((req, res, next) => {
  const startTime = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - startTime;
    logger.debug('HTTP request', {
      method: req.method,
      path: req.path,
      status: res.statusCode,
      durationMs: duration,
    });
  });
  next();
});

// Health check endpoint
app.get('/health', async (req, res) => {
  try {
    const mongoConnected = mongodbClient.isReady();
    const kafkaConnected = kafkaClient.isProducerConnected();

    const health = {
      status: mongoConnected && kafkaConnected ? 'healthy' : 'degraded',
      timestamp: new Date().toISOString(),
      checks: {
        mongodb: mongoConnected ? 'connected' : 'disconnected',
        kafka: kafkaConnected ? 'connected' : 'disconnected',
      },
    };

    const statusCode = health.status === 'healthy' ? 200 : 503;
    res.status(statusCode).json(health);
  } catch (error) {
    logger.error('Health check failed', error);
    res.status(503).json({
      status: 'unhealthy',
      error: error.message,
    });
  }
});

// Metrics endpoint
app.get('/metrics', (req, res) => {
  try {
    const metricsData = metrics.getMetrics();
    res.json(metricsData);
  } catch (error) {
    logger.error('Metrics endpoint failed', error);
    res.status(500).json({ error: error.message });
  }
});

// Location push endpoint
app.post('/push/location', async (req, res) => {
  try {
    const { latitude, longitude, metadata } = req.body;

    const result = await producerService.sendLocation(latitude, longitude, metadata);

    res.status(200).json({
      success: true,
      message: 'Location data received',
      data: result,
    });
  } catch (error) {
    const statusCode = error.statusCode || 500;
    const errorResponse = {
      success: false,
      error: error.message,
      code: error.code || 'UNKNOWN_ERROR',
    };

    if (config.server.nodeEnv === 'development') {
      errorResponse.context = error.context;
    }

    logger.error('Location push failed', error, {
      latitude: req.body.latitude,
      longitude: req.body.longitude,
    });

    res.status(statusCode).json(errorResponse);
  }
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    path: req.path,
  });
});

// Global error handler
app.use((err, req, res, next) => {
  logger.error('Unhandled error', err, {
    path: req.path,
    method: req.method,
  });

  res.status(err.statusCode || 500).json({
    success: false,
    error: err.message || 'Internal server error',
    code: err.code || 'INTERNAL_ERROR',
  });
});

// Initialize and start server
async function start() {
  try {
    // Connect to MongoDB
    await mongodbClient.connect();
    metrics.setGauge('mongoConnected', true);

    // Initialize Kafka producer
    await kafkaClient.initProducer();
    metrics.setGauge('kafkaConnected', true);

    // Start server
    app.listen(config.server.port, () => {
      logger.info('Server started', {
        port: config.server.port,
        nodeEnv: config.server.nodeEnv,
      });
    });
  } catch (error) {
    logger.error('Failed to start server', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  logger.info('Shutting down server');
  try {
    await kafkaClient.disconnect();
    await mongodbClient.disconnect();
    logger.info('Graceful shutdown complete');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', error);
    process.exit(1);
  }
});

start();

module.exports = app;
