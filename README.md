# StreamGuard

**StreamGuard** is a powerful data management NodeJS Application designed to efficiently handle high-velocity data streams and reduce the load on MongoDB. It leverages Kafka to streamline the processing pipeline and performs bulk inserts into MongoDB to improve write throughput.

## Features

- **Real-Time Data Processing**: Handles fast-moving data streams efficiently.
- **Reduced MongoDB Overload**: Uses Kafka to alleviate pressure on MongoDB.
- **Bulk Insertion**: Performs bulk data operations to optimize performance.

## Installation

Clone the repository and install the dependencies:

```bash
git clone https://github.com/codeterrayt/StreamGuard.git
cd StreamGuard
npm install
```

## Running the Application

1. **Start the Required Services**:

   - MongoDB:
     ```bash
     docker run -p 27017:27017 mongo
     ```
   - Zookeeper:
     ```bash
     docker run -p 2181:2181 zookeeper
     ```
   - Kafka:
     ```bash
     docker run -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=<IPv4-Address>:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://<IPv4-Address>:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:latest
     ```

2. **Run the Server and Consumer Applications**:

   ```bash
   node server.js
   node consumer-app.js
   ```

## Environment Variables

- **`MAX_DATA_LENGTH_BUFFER`**: Defines the maximum number of data entries to buffer before performing a bulk insertion. Set this to `10` to trigger bulk operations after accumulating 10 data entries (example below).

   Example configuration in `.env` file:

   ```env
   MAX_DATA_LENGTH_BUFFER=100
   ```

## Usage

- **Server**: Manages data flow and interacts with Kafka.
- **Consumer**: Processes incoming data and performs bulk insertions based on the configured buffer length.

## Testing

To test the data ingestion and processing, you can use the provided test script:

1. **Run the Test Script**:

   ```bash
   node test/test.js
   ```

   This script sends 3 requests per second with incrementing latitude and longitude values to simulate data streaming.


## Contributing

Feel free to contribute by submitting issues or pull requests. For any questions or feedback, open an issue on the [GitHub repository](https://github.com/codeterrayt/StreamGuard).

---

## Example .env and Local-run checklist

Add a `.env` file in the project root with the values below (adjust to your environment):

```env
# Kafka settings
KAFKA_BROKER=kafka:9092            # broker address; when using docker-compose this can be 'kafka:9092'
KAFKA_CLIENT_ID=streamguard-client  # client id for kafkajs
KAFKA_TOPIC=location                # topic the server sends to (consumer subscribes to 'location' by default)
KAFKA_GROUP_ID=streamguard-group    # consumer group id

# MongoDB
MONGO_URL=mongodb://mongo:27017/streamguard

# Consumer buffering
MAX_DATA_LENGTH_BUFFER=100          # number of messages to buffer before bulk insert (must be a number)
```

Minimal local-run checklist (quick steps):

1. Ensure Docker is running (if using docker-compose).
2. From project root, bring up services with docker-compose (recommended):

```bash
# builds images for server and consumer and starts kafka, zookeeper, mongo
docker-compose up --build
```

Wait for Kafka, Zookeeper and Mongo to be healthy in the compose logs before starting traffic.

3. If running locally (without compose), start required services separately and then in project root:

```bash
npm install
# in one terminal
node server.js
# in another terminal
node consumer-app.js
# in another terminal to generate test data
node test/test.js
```

4. Verify behavior:
- Check server logs (port 3000) for "Location data received".
- Check consumer logs for "Received message" and "Documents inserted into MongoDB".
- Connect to MongoDB (e.g., mongosh) and confirm documents in the `locations` or `locations` collection (model name `Location` -> collection `locations`).

5. Tearing down (if using docker-compose):

```bash
docker-compose down
```

Notes & tips
- consumer.js currently hardcodes `const topic = 'location'`. Make sure `KAFKA_TOPIC` in your `.env` matches this value or modify consumer.js to read the env var.
- MAX_DATA_LENGTH_BUFFER is read from process.env; ensure it's a numeric value. For production workloads tune this value based on message size and ingestion latency.
- When running in containers, ensure network hostnames (kafka, zookeeper, mongo) are resolvable by the Node processes (docker-compose handles this by default).
