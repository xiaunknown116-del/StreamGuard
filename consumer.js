// consumer.js
const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');

require('dotenv').config();

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID,
  brokers: [process.env.KAFKA_BROKER]
});

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID });
// Read topic from env with fallback to 'location'
const topic = process.env.KAFKA_TOPIC || 'location';

// Mongoose setup
const mongoUrl = process.env.MONGO_URL;
mongoose.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB'))
  .catch(err => console.error('Failed to connect to MongoDB', err));

const locationSchema = new mongoose.Schema({
  latitude: Number,
  longitude: Number
});

const Location = mongoose.model('Location', locationSchema);

// Dead-letter schema to persist batches that fail to insert after retries
const deadLetterSchema = new mongoose.Schema({
  payload: mongoose.Schema.Types.Mixed,
  error: String,
  createdAt: { type: Date, default: Date.now }
});
const DeadLetter = mongoose.model('DeadLetter', deadLetterSchema);

// Buffer to store messages
const buffer = [];
// Parse buffer size from env and validate
let bufferSize = Number(process.env.MAX_DATA_LENGTH_BUFFER);
if (!Number.isFinite(bufferSize) || bufferSize <= 0) {
  console.warn('Invalid or missing MAX_DATA_LENGTH_BUFFER; falling back to 100');
  bufferSize = 100;
}

// Retry / DLQ settings
const MAX_RETRIES = Number(process.env.CONSUMER_INSERT_MAX_RETRIES) || 3;
const INITIAL_RETRY_DELAY_MS = Number(process.env.CONSUMER_INITIAL_RETRY_DELAY_MS) || 1000;

async function writeBatchWithRetries(batch) {
  let attempt = 0;
  while (attempt < MAX_RETRIES) {
    try {
      await Location.insertMany(batch);
      return true;
    } catch (err) {
      attempt += 1;
      console.error(`Insert attempt ${attempt} failed:`, err);

      if (attempt < MAX_RETRIES) {
        const delay = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt - 1);
        console.log(`Retrying insert in ${delay}ms...`);
        await new Promise(res => setTimeout(res, delay));
        continue;
      }

      // Final attempt failed — write to dead-letter collection
      try {
        const dlqEntries = batch.map(doc => ({ payload: doc, error: err.message }));
        await DeadLetter.insertMany(dlqEntries);
        console.log(`Wrote ${dlqEntries.length} documents to DeadLetter collection`);
        return true; // treat DLQ write as success so offsets can be committed
      } catch (dlqErr) {
        console.error('Failed to write to DeadLetter collection', dlqErr);
        return false; // neither insert nor DLQ write succeeded
      }
    }
  }
  return false;
}

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const location = JSON.parse(message.value.toString());
        console.log(`Received message: ${JSON.stringify(location)}`);

        buffer.push(location);

        // Check if the buffer has reached the desired size
        if (buffer.length >= bufferSize) {
          // Take a snapshot of the current buffer and clear it to avoid reusing the same array
          const batch = buffer.splice(0, buffer.length);

          const success = await writeBatchWithRetries(batch);

          if (success) {
            try {
              // Commit offsets AFTER successful insert or DLQ write
              await consumer.commitOffsets([
                { topic, partition, offset: (parseInt(message.offset, 10) + 1).toString() }
              ]);
              console.log('Offsets committed');
            } catch (commitErr) {
              console.error('Failed to commit offsets', commitErr);
            }
          } else {
            console.error('Batch failed and could not be written to DLQ; leaving offsets uncommitted for retry');
            // If desired, you could push the batch back into buffer for re-processing, but that may lead to tight retry loops.
          }
        }
      } catch (err) {
        console.error('Error processing message', err);
      }
    },
  });
};

const disconnectConsumer = async () => {
  // Insert any remaining messages in the buffer before disconnecting
  if (buffer.length > 0) {
    const remaining = buffer.splice(0, buffer.length);
    try {
      const success = await writeBatchWithRetries(remaining);
      if (success) console.log(`Inserted remaining ${remaining.length} documents (or wrote to DLQ)`);
      else console.error('Failed to flush remaining documents and DLQ write also failed');
    } catch (err) {
      console.error('Failed to insert remaining documents into MongoDB', err);
    }
  }

  await consumer.disconnect();
  await mongoose.disconnect();
};

module.exports = { runConsumer, disconnectConsumer };
