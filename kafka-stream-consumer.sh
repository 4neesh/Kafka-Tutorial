#!/bin/bash

# Kafka Stream Consumer Shell Script
# This script helps you consume messages from the UserCountTopic to see real-time user message counts

KAFKA_HOME=${KAFKA_HOME:-"/opt/kafka"}
BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-"localhost:9092"}
TOPIC="UserCountTopic"

echo "Starting Kafka consumer for UserCountTopic..."
echo "This will show real-time user message counts from MyJsonTopic"
echo "Press Ctrl+C to stop"
echo "----------------------------------------"

# Check if kafka-console-consumer.sh exists in common locations
if [ -f "$KAFKA_HOME/bin/kafka-console-consumer.sh" ]; then
    CONSUMER_SCRIPT="$KAFKA_HOME/bin/kafka-console-consumer.sh"
elif [ -f "/usr/local/bin/kafka-console-consumer.sh" ]; then
    CONSUMER_SCRIPT="/usr/local/bin/kafka-console-consumer.sh"
elif command -v kafka-console-consumer.sh &> /dev/null; then
    CONSUMER_SCRIPT="kafka-console-consumer.sh"
else
    echo "Error: kafka-console-consumer.sh not found!"
    echo "Please set KAFKA_HOME environment variable or ensure Kafka is in your PATH"
    echo "Example: export KAFKA_HOME=/path/to/kafka"
    exit 1
fi

# Start the consumer
$CONSUMER_SCRIPT \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic $TOPIC \
    --from-beginning \
    --property print.key=true \
    --property key.separator=" => " \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.timestamp=true
