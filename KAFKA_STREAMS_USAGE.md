# Kafka Streams User Message Counter

This project now includes real-time message counting per user ID using Kafka Streams. The system processes messages from `MyJsonTopic` and maintains a running count of messages sent by each user.

## What Was Added

### 1. Dependencies
- Added `kafka-streams` dependency to `pom.xml`

### 2. Configuration
- **KafkaStreamsConfig.java**: Configures Kafka Streams with exactly-once processing
- **Updated KafkaTopicConfig.java**: Added `UserCountTopic` for output
- **Updated application.properties**: Added Kafka Streams configuration

### 3. Stream Processing
- **UserMessageCountProcessor.java**: Main stream processor that:
  - Reads from `MyJsonTopic`
  - Extracts user ID from JSON messages
  - Maintains running count per user ID
  - Outputs results to `UserCountTopic`

### 4. Consumer Script
- **kafka-stream-consumer.sh**: Shell script to consume and display user counts

## How It Works

1. **Input**: Messages are consumed from `MyJsonTopic` containing User JSON objects with `id`, `firstName`, and `lastName`
2. **Processing**: 
   - Extracts the `id` field from each JSON message
   - Groups messages by user ID
   - Maintains a running count for each user
3. **Output**: Sends count updates to `UserCountTopic` in format:
   ```json
   {"userId":"123","messageCount":5,"timestamp":1640995200000}
   ```

## Usage Instructions

### 1. Start Your Application
```bash
mvn spring-boot:run
```

### 2. Send Messages to MyJsonTopic
Use your existing producer or send messages like:
```json
{"id":1,"firstName":"John","lastName":"Doe"}
{"id":2,"firstName":"Jane","lastName":"Smith"}
{"id":1,"firstName":"John","lastName":"Doe"}
```

### 3. Monitor User Counts
Run the consumer script to see real-time updates:
```bash
./kafka-stream-consumer.sh
```

Or use the standard Kafka console consumer:
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic UserCountTopic --from-beginning
```

## Expected Output

When you send messages, you'll see output like:
```
1 => {"userId":"1","messageCount":1,"timestamp":1640995200000}
2 => {"userId":"2","messageCount":1,"timestamp":1640995201000}
1 => {"userId":"1","messageCount":2,"timestamp":1640995202000}
```

## Key Features

- **Real-time Processing**: Counts are updated immediately as messages arrive
- **Exactly-Once Processing**: Ensures accurate counts even with failures
- **Stateful Processing**: Maintains counts across application restarts
- **JSON Output**: Easy to parse and integrate with other systems

## Troubleshooting

1. **Ensure Kafka is running** on localhost:9092
2. **Check topics exist**: `MyJsonTopic` and `UserCountTopic` should be created automatically
3. **Verify JSON format**: Messages must contain an `id` field
4. **Check logs**: Application logs will show processing information

## Customization

- **Change input topic**: Modify `MyJsonTopic` in `UserMessageCountProcessor.java`
- **Change output topic**: Modify `UserCountTopic` in the processor and topic config
- **Adjust JSON parsing**: Update the JSON extraction logic for different message formats
- **Add windowing**: Implement time-based windows for periodic counts
