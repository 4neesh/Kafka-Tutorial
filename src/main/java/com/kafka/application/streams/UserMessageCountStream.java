package com.kafka.application.streams;

import com.kafka.application.payload.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import org.apache.kafka.streams.kstream.TimeWindows;  // Added for windowing
import org.apache.kafka.streams.kstream.Windowed;  // Added for windowed keys
import org.apache.kafka.streams.kstream.Suppressed;  // Added for suppressing until window closes
import java.time.Duration;  // Added for window duration
import java.time.Instant;  // Added for timestamp handling
import java.time.ZoneId;  // Added for UTC zone
import java.time.format.DateTimeFormatter;  // Added for ISO-8601 formatting

@Component
public class UserMessageCountStream {
    private final Logger LOGGER = LoggerFactory.getLogger(UserMessageCountStream.class);

    @Bean
    public KStream<String, User> kStream(StreamsBuilder builder) {
        // 1️⃣ Use JsonSerde for User class
        JsonSerde<User> userSerde = new JsonSerde<>(User.class);

        // 2️⃣ Read from input topic with User value type
        KStream<String, User> userStream = builder.stream(
                "MyJsonTopic",
                Consumed.with(Serdes.String(), userSerde)
        );

        // 3️⃣ Re-key the stream using the user id (unchanged)
        KStream<String, User> rekeyedStream = userStream.selectKey((key, user) -> String.valueOf(user.getId()));

        // Updated: Perform windowed aggregation for 1-minute tumbling windows
        // Aggregate into a String "count|distinctNames" (e.g., "5|Alice,Bob") to track both without custom classes
        TimeWindows window = TimeWindows.of(Duration.ofMinutes(1));
        KTable<Windowed<String>, String> windowedAggregates = rekeyedStream
                .groupByKey()
                .windowedBy(window)
                .aggregate(
                        () -> "0|",  // Initializer: "count|distinctNames"
                        (key, value, aggregate) -> {
                            String[] parts = aggregate.split("\\|", 2);
                            int count = Integer.parseInt(parts[0]) + 1;
                            String names = parts[1];
                            String newName = value.getFirstName();
                            // Ensure distinctness by checking if name is already present
                            if (!names.contains(newName)) {
                                names += (names.isEmpty() ? "" : ",") + newName;
                            }
                            return count + "|" + names;
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        // Updated: Map to required key/value format and send to new topic
        // Original count, peek, and output to "UserMessageCount" replaced with windowed output
        windowedAggregates.toStream().map((windowedKey, value) -> {
            String userId = windowedKey.key();
            long startMs = windowedKey.window().start();
            long endMs = windowedKey.window().end();
            Instant startInstant = Instant.ofEpochMilli(startMs);
            Instant endInstant = Instant.ofEpochMilli(endMs);
            DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC"));
            String windowStart = formatter.format(startInstant);
            String windowEnd = formatter.format(endInstant);
            String[] parts = value.split("\\|", 2);
            int count = Integer.parseInt(parts[0]);
            String distinctNames = parts[1];
            String newKey = userId + "@" + windowStart;
            String newValue = "count=" + count + ",distinctNames=[" + distinctNames + "],start=" + windowStart + ",end=" + windowEnd;
            return KeyValue.pair(newKey, newValue);
        }).to("UserMessageWindowedCount", Produced.with(Serdes.String(), Serdes.String()));

        // Return unchanged
        return rekeyedStream;
    }
}
