// services/producerService.js
// Producer business logic with validation and error handling

const logger = require('../utils/logger');
const metrics = require('../utils/metrics');
const { ValidationError } = require('../utils/errors');
const kafkaClient = require('../lib/kafkaClient');
const config = require('../config');

class ProducerService {
  async sendLocation(latitude, longitude, metadata = {}) {
    // Validate input
    this.validateCoordinates(latitude, longitude);

    const location = {
      latitude,
      longitude,
      timestamp: new Date().toISOString(),
      ...(Object.keys(metadata).length > 0 && { metadata }),
    };

    try {
      await kafkaClient.sendMessage(config.kafka.topic, location);
      logger.debug('Location published to Kafka', { location });
      return { success: true, location };
    } catch (error) {
      logger.error('Failed to publish location', error, { location });
      throw error;
    }
  }

  validateCoordinates(latitude, longitude) {
    if (latitude === undefined || latitude === null) {
      throw new ValidationError('Latitude is required', { field: 'latitude' });
    }
    if (longitude === undefined || longitude === null) {
      throw new ValidationError('Longitude is required', { field: 'longitude' });
    }

    const lat = Number(latitude);
    const lon = Number(longitude);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      throw new ValidationError('Latitude and longitude must be valid numbers', {
        latitude,
        longitude,
      });
    }

    if (lat < -90 || lat > 90) {
      throw new ValidationError('Latitude must be between -90 and 90', {
        latitude: lat,
      });
    }

    if (lon < -180 || lon > 180) {
      throw new ValidationError('Longitude must be between -180 and 180', {
        longitude: lon,
      });
    }
  }
}

module.exports = new ProducerService();
