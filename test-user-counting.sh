#!/bin/bash

# Test script for Kafka Streams User Message Counting
# This script sends test messages to MyJsonTopic to demonstrate the counting functionality

KAFKA_HOME=${KAFKA_HOME:-"/opt/kafka"}
BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-"localhost:9092"}
TOPIC="MyJsonTopic"

echo "Testing Kafka Streams User Message Counting"
echo "============================================"

# Check if kafka-console-producer.sh exists
if [ -f "$KAFKA_HOME/bin/kafka-console-producer.sh" ]; then
    PRODUCER_SCRIPT="$KAFKA_HOME/bin/kafka-console-producer.sh"
elif [ -f "/usr/local/bin/kafka-console-producer.sh" ]; then
    PRODUCER_SCRIPT="/usr/local/bin/kafka-console-producer.sh"
elif command -v kafka-console-producer.sh &> /dev/null; then
    PRODUCER_SCRIPT="kafka-console-producer.sh"
else
    echo "Error: kafka-console-producer.sh not found!"
    echo "Please set KAFKA_HOME environment variable or ensure Kafka is in your PATH"
    exit 1
fi

echo "Sending test messages to $TOPIC..."
echo "These messages will be processed by Kafka Streams to count messages per user ID"
echo ""

# Send test messages
echo '{"id":1,"firstName":"John","lastName":"Doe"}' | $PRODUCER_SCRIPT --bootstrap-server $BOOTSTRAP_SERVERS --topic $TOPIC
echo "Sent message for user ID 1 (John Doe)"

echo '{"id":2,"firstName":"Jane","lastName":"Smith"}' | $PRODUCER_SCRIPT --bootstrap-server $BOOTSTRAP_SERVERS --topic $TOPIC
echo "Sent message for user ID 2 (Jane Smith)"

echo '{"id":1,"firstName":"John","lastName":"Doe"}' | $PRODUCER_SCRIPT --bootstrap-server $BOOTSTRAP_SERVERS --topic $TOPIC
echo "Sent another message for user ID 1 (John Doe)"

echo '{"id":3,"firstName":"Bob","lastName":"Johnson"}' | $PRODUCER_SCRIPT --bootstrap-server $BOOTSTRAP_SERVERS --topic $TOPIC
echo "Sent message for user ID 3 (Bob Johnson)"

echo '{"id":2,"firstName":"Jane","lastName":"Smith"}' | $PRODUCER_SCRIPT --bootstrap-server $BOOTSTRAP_SERVERS --topic $TOPIC
echo "Sent another message for user ID 2 (Jane Smith)"

echo '{"id":1,"firstName":"John","lastName":"Doe"}' | $PRODUCER_SCRIPT --bootstrap-server $BOOTSTRAP_SERVERS --topic $TOPIC
echo "Sent third message for user ID 1 (John Doe)"

echo ""
echo "Test messages sent successfully!"
echo "Expected counts:"
echo "- User ID 1: 3 messages"
echo "- User ID 2: 2 messages" 
echo "- User ID 3: 1 message"
echo ""
echo "Run './kafka-stream-consumer.sh' to see the real-time counts!"
