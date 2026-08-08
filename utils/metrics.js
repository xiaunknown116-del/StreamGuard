// utils/metrics.js
// Prometheus-style metrics collection for monitoring

const config = require('../config');

class Metrics {
  constructor() {
    this.enabled = config.metrics.enabled;
    this.metrics = {
      // Counter metrics
      messagesReceived: 0,
      messagesProcessed: 0,
      messagesErrored: 0,
      batchesInserted: 0,
      batchesFailed: 0,
      dlqInserted: 0,

      // Gauge metrics (current state)
      bufferSize: 0,
      mongoConnected: false,
      kafkaConnected: false,

      // Histogram metrics (latencies, in ms)
      insertLatencies: [],
      processingLatencies: [],
    };
  }

  incrementCounter(name) {
    if (this.enabled && this.metrics.hasOwnProperty(name)) {
      this.metrics[name]++;
    }
  }

  setGauge(name, value) {
    if (this.enabled && this.metrics.hasOwnProperty(name)) {
      this.metrics[name] = value;
    }
  }

  recordLatency(histogramName, latencyMs) {
    if (this.enabled && this.metrics.hasOwnProperty(histogramName)) {
      this.metrics[histogramName].push(latencyMs);
      // Keep only last 1000 samples to avoid memory bloat
      if (this.metrics[histogramName].length > 1000) {
        this.metrics[histogramName].shift();
      }
    }
  }

  getMetrics() {
    if (!this.enabled) return {};

    const insertLatencies = this.metrics.insertLatencies;
    const processingLatencies = this.metrics.processingLatencies;

    const calculateStats = (arr) => {
      if (arr.length === 0) return { min: 0, max: 0, avg: 0, p95: 0 };
      const sorted = [...arr].sort((a, b) => a - b);
      const sum = sorted.reduce((a, b) => a + b, 0);
      return {
        min: sorted[0],
        max: sorted[sorted.length - 1],
        avg: Math.round(sum / sorted.length),
        p95: sorted[Math.floor(sorted.length * 0.95)],
      };
    };

    return {
      counters: {
        messagesReceived: this.metrics.messagesReceived,
        messagesProcessed: this.metrics.messagesProcessed,
        messagesErrored: this.metrics.messagesErrored,
        batchesInserted: this.metrics.batchesInserted,
        batchesFailed: this.metrics.batchesFailed,
        dlqInserted: this.metrics.dlqInserted,
      },
      gauges: {
        bufferSize: this.metrics.bufferSize,
        mongoConnected: this.metrics.mongoConnected,
        kafkaConnected: this.metrics.kafkaConnected,
      },
      histograms: {
        insertLatencyMs: calculateStats(insertLatencies),
        processingLatencyMs: calculateStats(processingLatencies),
      },
      timestamp: new Date().toISOString(),
    };
  }

  reset() {
    if (!this.enabled) return;
    this.metrics.messagesReceived = 0;
    this.metrics.messagesProcessed = 0;
    this.metrics.messagesErrored = 0;
    this.metrics.batchesInserted = 0;
    this.metrics.batchesFailed = 0;
    this.metrics.dlqInserted = 0;
    this.metrics.insertLatencies = [];
    this.metrics.processingLatencies = [];
  }
}

module.exports = new Metrics();
