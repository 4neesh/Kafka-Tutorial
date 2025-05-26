# Kafka Spring Boot Application with EOS Support

This is a Spring Boot application that demonstrates producing and consuming messages with Apache Kafka, featuring Exactly Once Semantics (EOS) support. It includes REST endpoints to send plain text and JSON messages, and a consumer that logs the received data, ensuring that messages are processed exactly once.

## Prerequisites

Before running the project, ensure you have the following installed:

- Java 17+
- Maven 3.6+
- Apache Kafka (locally or via Docker)

## Kafka Setup

1. **Download Kafka:**  
   https://kafka.apache.org/downloads  
   Extract the archive and navigate into the Kafka directory.

2. **Start Kafka Server (in a new terminal):**

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-server-start.sh config/server.properties
```

3.**View Topic messages (each in a new terminal):**
```bash
bin/kafka-console-consumer.sh --topic MyTopic --from-beginning --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic MyJsonTopic --from-beginning --bootstrap-server localhost:9092
```

## Build the Project
Navigate to the application directory and run:

```bash
./mvnw clean install
```
Run the Spring Boot application using:
```bash
./mvnw spring-boot:run
```

## API Endpoints
- POST http://localhost:8080/publish?message=hello — Sends a plain text message to Kafka
- POST http://localhost:8080/publishJson — Sends a JSON payload (User object) to Kafka

### JSON Payload Example for /publishJson
When calling the /publishJson endpoint, the body must include a JSON representation of a user, such as:

```json
{
"id": 1,
"firstName": "John",
"lastName": "Smith"
}
```

## Notes
Make sure Kafka is running and accessible at localhost:9092. You can configure topic names and bootstrap servers in src/main/resources/application.properties.

## Exactly Once Semantics (EOS) Support

This application has been configured to support Exactly Once Semantics (EOS) for Kafka message processing:

### Producer Configuration
- Transaction support is enabled with a transaction ID prefix
- Idempotence is enabled to prevent duplicate message production
- All acknowledgments (acks=all) are required to ensure messages are properly committed

### Consumer Configuration
- Isolation level is set to `read_committed` to ensure consumers only read committed messages
- This prevents the consumption of messages that are part of aborted transactions

### Implementation Details
- Producers use the `@Transactional` annotation and `executeInTransaction` method to ensure messages are sent within transactions
- This guarantees that either all messages in a transaction are successfully delivered or none are
