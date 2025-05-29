package com.kafka.application.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.application.payload.User;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaStreamsConfig {

    private static final String INPUT_TOPIC = "MyJsonTopic";
    private static final String OUTPUT_TOPIC = "user-message-counts";
    private static final String STORE_NAME = "user-counts-store";

    @Bean
    public KafkaStreams kafkaStreams() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-counts-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Custom Serde for User
        ObjectMapper objectMapper = new ObjectMapper();
        Serde<User> userSerde = Serdes.serdeFrom(
                (topic, data) -> {
                    try {
                        return objectMapper.writeValueAsBytes(data);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                (topic, bytes) -> {
                    try {
                        return objectMapper.readValue(bytes, User.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        // Read the input stream with User value
        KStream<String, User> inputStream = builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), userSerde));

        // Group by user id and count
        KTable<String, Long> counts = inputStream
                .groupBy((key, user) -> String.valueOf(user.getId()),
                        Grouped.with(Serdes.String(), userSerde))
                .count(Materialized.as(STORE_NAME));

        // To see the updates, write to output topic
        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        return streams;
    }
}
