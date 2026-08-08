// utils/errors.js
// Custom error classes for structured error handling

class StreamGuardError extends Error {
  constructor(message, code, statusCode = 500, context = {}) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.statusCode = statusCode;
    this.context = context;
    Error.captureStackTrace(this, this.constructor);
  }
}

class KafkaConnectionError extends StreamGuardError {
  constructor(message, context = {}) {
    super(message, 'KAFKA_CONNECTION_ERROR', 503, context);
  }
}

class MongoDBConnectionError extends StreamGuardError {
  constructor(message, context = {}) {
    super(message, 'MONGODB_CONNECTION_ERROR', 503, context);
  }
}

class ValidationError extends StreamGuardError {
  constructor(message, context = {}) {
    super(message, 'VALIDATION_ERROR', 400, context);
  }
}

class BatchInsertError extends StreamGuardError {
  constructor(message, context = {}) {
    super(message, 'BATCH_INSERT_ERROR', 500, context);
  }
}

class MessageParseError extends StreamGuardError {
  constructor(message, context = {}) {
    super(message, 'MESSAGE_PARSE_ERROR', 400, context);
  }
}

class DeadLetterError extends StreamGuardError {
  constructor(message, context = {}) {
    super(message, 'DEAD_LETTER_ERROR', 500, context);
  }
}

module.exports = {
  StreamGuardError,
  KafkaConnectionError,
  MongoDBConnectionError,
  ValidationError,
  BatchInsertError,
  MessageParseError,
  DeadLetterError,
};
