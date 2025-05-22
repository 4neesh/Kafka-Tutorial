# Kafka Spring Boot Application

This is a basic Spring Boot application that demonstrates producing and consuming messages with Apache Kafka. It includes REST endpoints to send plain text and JSON messages, and a consumer that logs the received data.

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

3.**View Topic messages (in a new terminal):**
```bash
bin/kafka-console-consumer.sh --topic MyTopic --from-beginning --bootstrap-server localhost:9092
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
- POST https://localhost:8080/publish?message=hello — Sends a plain text message to Kafka
- POST https://localhost:8080/publishJson — Sends a JSON payload (User object) to Kafka

## Notes
Make sure Kafka is running and accessible at localhost:9092. You can configure topic names and bootstrap servers in src/main/resources/application.properties.