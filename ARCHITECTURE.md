# Architecture Improvements - StreamGuard Refactoring Guide

## Overview

This document outlines the comprehensive architectural improvements made to StreamGuard to enhance scalability, maintainability, observability, and error handling.

## Key Improvements

### 1. Centralized Configuration Management (`config/index.js`)

**Problem Solved:**
- Environment variables scattered across multiple files
- No validation of required configuration at startup
- Inconsistent timeout and retry settings

**Solution:**
- Single source of truth for all configuration
- Automatic validation on module load
- Typed configuration with sensible defaults
- Support for multiple environments (development, staging, production)

**Benefits:**
- Fail-fast on configuration errors
- Easy to understand all tunable parameters
- Simplified testing with config overrides

### 2. Structured Logging (`utils/logger.js`)

**Problem Solved:**
- Inconsistent log formats across files
- Difficult to parse logs programmatically
- No log level control

**Solution:**
- Structured JSON logging with timestamps
- Configurable log levels (error, warn, info, debug, trace)
- Context-aware error logging with stack traces
- Fallback to human-readable text format

**Benefits:**
- Logs integrate with ELK, Datadog, CloudWatch
- Better debugging with full error context
- Production-ready error reporting

**Usage:**
```javascript
const logger = require('./utils/logger');

logger.info('User action', { userId: 123, action: 'login' });
logger.error('Database error', error, { query: 'SELECT *' });
logger.debug('Cache hit', { key: 'user:123', ttl: 300 });
```

### 3. Metrics & Observability (`utils/metrics.js`)

**Problem Solved:**
- No visibility into system performance
- Cannot track buffer depth, insert latency, or throughput
- Difficult to diagnose bottlenecks

**Solution:**
- Counter metrics (messages received, processed, errored)
- Gauge metrics (current buffer size, connection state)
- Histogram metrics (insert latency, processing latency with P95)
- Metrics endpoint exposed at `/metrics`

**Benefits:**
- Monitor system health in real-time
- Identify performance degradation
- Data for alerting and capacity planning

### 4. Custom Error Classes (`utils/errors.js`)

**Problem Solved:**
- Generic JavaScript errors without context
- Inconsistent HTTP status codes
- Difficulty distinguishing error types

**Solution:**
- Hierarchy of custom error classes
- Built-in error codes and HTTP status codes
- Contextual information attached to each error
- Structured error responses in API

### 5. Connection Client Wrappers

**Kafka Client (`lib/kafkaClient.js`):**
- Singleton pattern for producer and consumer
- Automatic reconnection with exponential backoff
- Proper resource cleanup on disconnect
- Connection state tracking

**MongoDB Client (`lib/mongodbClient.js`):**
- Singleton MongoDB connection manager
- Connection pooling (min 2, max 10)
- Configurable timeouts and retry logic
- Connection ready state checks

### 6. Enhanced Models

**Location Model (`models/Location.js`):**
- Proper schema validation
- Coordinate boundary checks (-90 to 90 for latitude, -180 to 180 for longitude)
- Automatic timestamps
- Strategic indexing for query performance

**DeadLetter Model (`models/DeadLetter.js`):**
- Dead-letter queue model with retry tracking
- Status tracking (pending/resolved/abandoned)
- Error context preservation

### 7. Service Layer

**ProducerService (`services/producerService.js`):**
- Location validation and publishing
- Coordinate range validation
- Clean separation from HTTP layer

**ConsumerService (`services/consumerService.js`):**
- Message handling, buffering, and batching
- Time-based flush (every 5 seconds by default)
- Size-based flush (when buffer reaches max size)
- Exponential backoff retry with configurable attempts
- Automatic dead-letter queue fallback
- Graceful shutdown with final buffer flush

### 8. Refactored Server (`server.js`)

**New Endpoints:**
- `GET /health` - Service health and dependency status
- `GET /metrics` - Real-time performance metrics
- `POST /push/location` - Location ingestion (unchanged API)

**Improvements:**
- Structured error responses with error codes
- Request logging middleware
- Global error handler
- Graceful shutdown with resource cleanup

### 9. Refactored Consumer (`consumer-app.js`)

**Improvements:**
- Explicit lifecycle management (start, shutdown)
- Signal handling (SIGINT, SIGTERM)
- Uncaught exception handling
- Unhandled rejection tracking
- Detailed startup logging

## Configuration

All configuration is managed via environment variables. See `.env.example` for all options.

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3000 | HTTP server port |
| `NODE_ENV` | development | Environment (development/production) |
| `KAFKA_BROKER` | localhost:9092 | Kafka broker address(es) |
| `KAFKA_TOPIC` | location | Kafka topic to publish/consume |
| `MONGO_URL` | mongodb://localhost:27017/streamguard | MongoDB connection string |
| `MAX_DATA_LENGTH_BUFFER` | 100 | Messages to buffer before batch insert |
| `CONSUMER_FLUSH_INTERVAL_MS` | 5000 | Force flush every N milliseconds |
| `CONSUMER_INSERT_MAX_RETRIES` | 3 | Retry attempts on insert failure |
| `LOG_LEVEL` | info | Logging verbosity (error/warn/info/debug/trace) |
| `LOG_FORMAT` | json | Log format (json/text) |
| `METRICS_ENABLED` | true | Enable metrics collection |

## Migration Guide: Old to New Architecture

### Before (Old Code)
```javascript
// Monolithic server.js
const { connectProducer, sendMessage } = require('./producer');

app.post('/push/location', async (req, res) => {
  const { latitude, longitude } = req.body;
  if (latitude === undefined || longitude === undefined) {
    return res.status(400).send('Latitude and longitude are required');
  }
  await sendMessage(process.env.KAFKA_TOPIC, { latitude, longitude });
  res.send('Location data received');
});
```

### After (New Code)
```javascript
// Refactored with service layer
const producerService = require('./services/producerService');
const logger = require('./utils/logger');

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
    logger.error('Location push failed', error, { latitude, longitude });
    res.status(error.statusCode || 500).json({
      success: false,
      error: error.message,
      code: error.code,
    });
  }
});
```

## Key Benefits

1. **Separation of Concerns**: Business logic separated from HTTP/Kafka layers
2. **Error Handling**: Structured error classes with proper HTTP status codes
3. **Observability**: Comprehensive logging and metrics collection
4. **Resilience**: Automatic retry logic and dead-letter queue
5. **Testability**: Service layer enables unit and integration testing
6. **Scalability**: Connection pooling and configurable batch sizes
7. **Maintainability**: Clear module organization and responsibilities

## Testing

### Health Check
```bash
curl http://localhost:3000/health
```

### Metrics
```bash
curl http://localhost:3000/metrics
```

### Send Location
```bash
curl -X POST http://localhost:3000/push/location \
  -H "Content-Type: application/json" \
  -d '{"latitude": 37.7749, "longitude": -122.4194}'
```

## Deployment

### Docker Compose
```bash
docker-compose up --build
```

### Docker
```bash
docker build -f Dockerfile.server -t streamguard-server .
docker build -f Dockerfile.consumer -t streamguard-consumer .
```

## Backward Compatibility

The refactored code maintains backward compatibility:
- Same `/push/location` API endpoint
- Same request/response format (enhanced with metadata)
- Same MongoDB schema
- Same Kafka topic/group configuration

## Performance Tuning

### Buffer Strategy
- **Low Latency** (< 1 sec): `MAX_DATA_LENGTH_BUFFER=50`, `CONSUMER_FLUSH_INTERVAL_MS=1000`
- **Balanced** (< 5 sec): `MAX_DATA_LENGTH_BUFFER=100`, `CONSUMER_FLUSH_INTERVAL_MS=5000` (default)
- **High Throughput** (< 30 sec): `MAX_DATA_LENGTH_BUFFER=500`, `CONSUMER_FLUSH_INTERVAL_MS=10000`

### Monitoring
- Buffer depth: Should stay well below `MAX_DATA_LENGTH_BUFFER`
- Insert latency (p95): Should be < 1000ms
- Error rate: Should be < 1%
- DLQ insertions: Should be 0

## Next Steps

1. Test the refactored code locally with docker-compose
2. Verify metrics are being collected
3. Monitor logs for any errors
4. Deploy to staging environment
5. Run integration tests
6. Deploy to production with monitoring

For more details, see `ARCHITECTURE_QUICK_REFERENCE.md`.
