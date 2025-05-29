package com.kafka.application.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserMessageCountProcessor {

    private static final Logger logger = LoggerFactory.getLogger(UserMessageCountProcessor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        // Read from MyJsonTopic
        KStream<String, String> userStream = streamsBuilder
                .stream("MyJsonTopic", Consumed.with(Serdes.String(), Serdes.String()));

        // Extract user ID from JSON message and count occurrences
        KTable<String, Long> userCounts = userStream
                .filter((key, value) -> {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(value);
                        return jsonNode.has("id");
                    } catch (Exception e) {
                        logger.warn("Failed to parse JSON message: {}", value, e);
                        return false;
                    }
                })
                .selectKey((key, value) -> {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(value);
                        return String.valueOf(jsonNode.get("id").asInt());
                    } catch (Exception e) {
                        logger.error("Failed to extract user ID from message: {}", value, e);
                        return "unknown";
                    }
                })
                .groupByKey()
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Convert counts to string and send to output topic
        userCounts.toStream()
                .mapValues((userId, count) -> {
                    String result = String.format("{\"userId\":\"%s\",\"messageCount\":%d,\"timestamp\":%d}", 
                            userId, count, System.currentTimeMillis());
                    logger.info("User {} has sent {} messages", userId, count);
                    return result;
                })
                .to("UserCountTopic", Produced.with(Serdes.String(), Serdes.String()));

        logger.info("User message count processing pipeline initialized");
    }
}
