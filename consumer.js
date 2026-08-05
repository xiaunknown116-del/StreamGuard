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

// Buffer to store messages
const buffer = [];
// Parse buffer size from env and validate
let bufferSize = Number(process.env.MAX_DATA_LENGTH_BUFFER);
if (!Number.isFinite(bufferSize) || bufferSize <= 0) {
  console.warn('Invalid or missing MAX_DATA_LENGTH_BUFFER; falling back to 100');
  bufferSize = 100;
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

          try {
            await Location.insertMany(batch);
            console.log(`Inserted ${batch.length} documents into MongoDB`);

            // Commit offsets AFTER successful insert
            await consumer.commitOffsets([
              { topic, partition, offset: (parseInt(message.offset, 10) + 1).toString() }
            ]);
            console.log('Offsets committed');
          } catch (err) {
            console.error('Failed to insert documents into MongoDB', err);
            // Optionally: implement retry, DLQ, or re-queueing logic here
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
      await Location.insertMany(remaining);
      console.log(`Inserted remaining ${remaining.length} documents into MongoDB`);
    } catch (err) {
      console.error('Failed to insert remaining documents into MongoDB', err);
    }
  }

  await consumer.disconnect();
  await mongoose.disconnect();
};

module.exports = { runConsumer, disconnectConsumer };
