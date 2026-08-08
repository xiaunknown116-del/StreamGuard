// utils/logger.js
// Structured logging utility with JSON and text formatting

const config = require('../config');

const LOG_LEVELS = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
  trace: 4,
};

class Logger {
  constructor() {
    this.currentLevel = LOG_LEVELS[config.logging.level] ?? LOG_LEVELS.info;
    this.format = config.logging.format;
  }

  shouldLog(level) {
    return LOG_LEVELS[level] <= this.currentLevel;
  }

  formatMessage(level, message, data = {}) {
    const timestamp = new Date().toISOString();

    if (this.format === 'json') {
      return JSON.stringify({
        timestamp,
        level,
        message,
        ...data,
      });
    }

    // Text format
    const dataStr = Object.keys(data).length > 0 ? ` ${JSON.stringify(data)}` : '';
    return `[${timestamp}] [${level.toUpperCase()}] ${message}${dataStr}`;
  }

  log(level, message, data = {}) {
    if (!this.shouldLog(level)) return;
    console.log(this.formatMessage(level, message, data));
  }

  error(message, error = null, context = {}) {
    const data = {
      ...context,
      ...(error && {
        error: {
          message: error.message,
          stack: error.stack,
          code: error.code,
        },
      }),
    };
    this.log('error', message, data);
  }

  warn(message, data = {}) {
    this.log('warn', message, data);
  }

  info(message, data = {}) {
    this.log('info', message, data);
  }

  debug(message, data = {}) {
    this.log('debug', message, data);
  }

  trace(message, data = {}) {
    this.log('trace', message, data);
  }
}

module.exports = new Logger();
