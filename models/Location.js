// models/Location.js
// MongoDB Location model with indexing and validation

const mongoose = require('mongoose');

const locationSchema = new mongoose.Schema(
  {
    latitude: {
      type: Number,
      required: [true, 'Latitude is required'],
      min: [-90, 'Latitude must be >= -90'],
      max: [90, 'Latitude must be <= 90'],
      index: true,
    },
    longitude: {
      type: Number,
      required: [true, 'Longitude is required'],
      min: [-180, 'Longitude must be >= -180'],
      max: [180, 'Longitude must be <= 180'],
      index: true,
    },
    timestamp: {
      type: Date,
      default: Date.now,
      index: true,
    },
    metadata: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
  },
  {
    timestamps: true,
    collection: 'locations',
  }
);

// Compound index for geospatial queries
locationSchema.index({ latitude: 1, longitude: 1 });

// TTL index for automatic document deletion after 30 days (optional)
// locationSchema.index({ createdAt: 1 }, { expireAfterSeconds: 2592000 });

const Location = mongoose.model('Location', locationSchema);

module.exports = Location;
