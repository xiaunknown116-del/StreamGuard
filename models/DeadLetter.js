// models/DeadLetter.js
// Dead Letter Queue model for failed batch processing

const mongoose = require('mongoose');

const deadLetterSchema = new mongoose.Schema(
  {
    payload: {
      type: mongoose.Schema.Types.Mixed,
      required: true,
    },
    error: {
      type: String,
      required: true,
    },
    errorCode: {
      type: String,
      default: null,
    },
    retryCount: {
      type: Number,
      default: 0,
    },
    lastAttempt: {
      type: Date,
      default: Date.now,
    },
    status: {
      type: String,
      enum: ['pending', 'resolved', 'abandoned'],
      default: 'pending',
    },
  },
  {
    timestamps: true,
    collection: 'deadLetters',
  }
);

// Index for status and lastAttempt to facilitate retry logic
deadLetterSchema.index({ status: 1, lastAttempt: 1 });

const DeadLetter = mongoose.model('DeadLetter', deadLetterSchema);

module.exports = DeadLetter;
