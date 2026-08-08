# StreamGuard - Architecture Quick Reference

## Directory Structure

```
StreamGuard/
├── config/
│   └── index.js                 # Centralized configuration
├── lib/
│   ├── kafkaClient.js           # Kafka wrapper
│   └── mongodbClient.js         # MongoDB wrapper
├── models/
│   ├── Location.js              # Location data model
│   └── DeadLetter.js            # Dead-letter queue model
├── services/
│   ├── consumerService.js       # Consumer business logic
│   └── producerService.js       # Producer business logic
├── utils/
│   ├── logger.js                # Structured logging
│   ├── metrics.js               # Metrics collection
│   └── errors.js                # Custom error classes
├── server.js                     # Express server
├── consumer-app.js               # Consumer entry point
├── package.json                  # Dependencies
├── .env.example                  # Configuration template
└── docker-compose.yml            # Local development
```

## Data Flow

### Producer
```
HTTP POST /push/location
    ↓
ProducerService (validation)
    ↓
KafkaClient (send message)
    ↓
Kafka Topic
```

### Consumer
```
Kafka Topic
    ↓
ConsumerService (parse & buffer)
    ↓
[Check flush condition]
    ├─ Size: buffer >= MAX_DATA_LENGTH_BUFFER
    └─ Time: elapsed >= CONSUMER_FLUSH_INTERVAL_MS
    ↓
MongoDB Insert (with retries)
    ↓
[On failure] → DeadLetter Queue
```

## Key Configuration

```env
# Buffer & Flush
MAX_DATA_LENGTH_BUFFER=100              # Messages per batch
CONSUMER_FLUSH_INTERVAL_MS=5000         # Time-based flush (5 sec)

# Retry Strategy
CONSUMER_INSERT_MAX_RETRIES=3           # Max retry attempts
CONSUMER_INITIAL_RETRY_DELAY_MS=1000    # First retry delay (1 sec)

# Logging & Metrics
LOG_LEVEL=info                          # Verbosity: error/warn/info/debug/trace
LOG_FORMAT=json                         # Format: json or text
METRICS_ENABLED=true                    # Metrics collection
```

## API Endpoints

### Health Check
```bash
GET /health

Response:
{
  "status": "healthy",
  "timestamp": "2026-08-08T12:00:00.000Z",
  "checks": {
    "mongodb": "connected",
    "kafka": "connected"
  }
}
```

### Metrics
```bash
GET /metrics

Response:
{
  "counters": {
    "messagesReceived": 1000,
    "messagesProcessed": 998,
    "messagesErrored": 2,
    "batchesInserted": 10,
    "batchesFailed": 1,
    "dlqInserted": 5
  },
  "gauges": {
    "bufferSize": 42,
    "mongoConnected": true,
    "kafkaConnected": true
  },
  "histograms": {
    "insertLatencyMs": { "min": 50, "max": 850, "avg": 234, "p95": 750 },
    "processingLatencyMs": { "min": 1, "max": 12, "avg": 3, "p95": 8 }
  },
  "timestamp": "2026-08-08T12:00:00.000Z"
}
```

### Push Location
```bash
POST /push/location
Content-Type: application/json

Request:
{
  "latitude": 37.7749,
  "longitude": -122.4194,
  "metadata": {"source": "gps"}
}

Response (200):
{
  "success": true,
  "message": "Location data received",
  "data": {
    "success": true,
    "location": {
      "latitude": 37.7749,
      "longitude": -122.4194,
      "timestamp": "2026-08-08T12:00:00.000Z",
      "metadata": {"source": "gps"}
    }
  }
}

Response (400 - Validation Error):
{
  "success": false,
  "error": "Latitude must be between -90 and 90",
  "code": "VALIDATION_ERROR"
}
```

## Error Handling

### Error Response Format
```json
{
  "success": false,
  "error": "Error message",
  "code": "ERROR_CODE"
}
```

### HTTP Status Codes
- `200 OK` - Success
- `400 Bad Request` - Validation error (VALIDATION_ERROR)
- `500 Internal Error` - Server error (BATCH_INSERT_ERROR, MESSAGE_PARSE_ERROR, DEAD_LETTER_ERROR)
- `503 Service Unavailable` - Connection error (KAFKA_CONNECTION_ERROR, MONGODB_CONNECTION_ERROR)

## Monitoring Checklist

- [ ] `/health` returns `healthy` status
- [ ] `/metrics` shows `messagesReceived > 0`
- [ ] `bufferSize < MAX_DATA_LENGTH_BUFFER`
- [ ] `batchesInserted` incrementing
- [ ] `dlqInserted = 0` (no failures)
- [ ] `insertLatencyMs.p95 < 1000ms`
- [ ] `messagesErrored < 1% of received`
- [ ] Logs show `[info]` level messages
- [ ] No `[error]` messages in logs

## Quick Start

### Development (Docker Compose)
```bash
git clone https://github.com/xiaunknown116-del/StreamGuard.git
cd StreamGuard
docker-compose up --build

# In another terminal
node test/test.js

# Check health
curl http://localhost:3000/health

# Check metrics
curl http://localhost:3000/metrics
```

### Production (Local)
```bash
npm install
cp .env.example .env
# Edit .env with production values
node server.js       # Terminal 1
node consumer-app.js # Terminal 2
```

## Performance Tuning

| Scenario | Buffer Size | Flush Interval | Rationale |
|----------|-------------|----------------|----------|
| Low latency (< 1 sec) | 50 | 1000ms | Frequent inserts |
| Balanced (< 5 sec) | 100 | 5000ms | Default, good throughput |
| High throughput (< 30 sec) | 500 | 10000ms | Larger batches |
| Memory constrained | 25 | 2000ms | Smaller batches |

## Troubleshooting

### High Buffer Size
- Check MongoDB: `curl http://localhost:3000/health`
- Lower interval: `CONSUMER_FLUSH_INTERVAL_MS=2000`
- Reduce buffer: `MAX_DATA_LENGTH_BUFFER=50`

### High Error Rate
- Check logs: `LOG_LEVEL=debug`
- Check DLQ: `db.deadLetters.find()`
- Increase retries: `CONSUMER_INSERT_MAX_RETRIES=5`

### Kafka Lag Building
- Scale consumers or increase batch size
- Check MongoDB write performance
- Monitor Kafka broker health

## References

- [Full Architecture Documentation](./ARCHITECTURE.md)
- [README](./README.md)
- [KafkaJS Docs](https://kafka.js.org/)
- [Mongoose Docs](https://mongoosejs.com/)
- [Express Docs](https://expressjs.com/)
