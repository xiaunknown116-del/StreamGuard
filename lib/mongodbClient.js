// lib/mongodbClient.js
// MongoDB client wrapper with connection pooling and error handling

const mongoose = require('mongoose');
const config = require('../config');
const logger = require('../utils/logger');
const { MongoDBConnectionError } = require('../utils/errors');

class MongoDBClient {
  constructor() {
    this.connection = null;
    this.isConnected = false;
  }

  async connect() {
    if (this.isConnected) return this.connection;

    try {
      this.connection = await mongoose.connect(config.mongodb.url, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        connectTimeoutMS: config.mongodb.connectionTimeout,
        socketTimeoutMS: config.mongodb.socketTimeout,
        serverSelectionTimeoutMS: config.mongodb.connectionTimeout,
        maxPoolSize: 10,
        minPoolSize: 2,
      });

      this.isConnected = true;
      logger.info('Connected to MongoDB', {
        database: config.mongodb.url.split('/').pop(),
      });

      return this.connection;
    } catch (error) {
      logger.error('Failed to connect to MongoDB', error, {
        mongoUrl: config.mongodb.url,
      });
      throw new MongoDBConnectionError('Failed to connect to MongoDB', {
        mongoUrl: config.mongodb.url,
        originalError: error.message,
      });
    }
  }

  async disconnect() {
    if (this.isConnected) {
      try {
        await mongoose.disconnect();
        this.isConnected = false;
        this.connection = null;
        logger.info('Disconnected from MongoDB');
      } catch (error) {
        logger.warn('Error disconnecting from MongoDB', { error: error.message });
      }
    }
  }

  getConnection() {
    if (!this.isConnected) {
      throw new MongoDBConnectionError('MongoDB connection not established');
    }
    return this.connection;
  }

  isReady() {
    return this.isConnected && mongoose.connection.readyState === 1;
  }
}

module.exports = new MongoDBClient();
