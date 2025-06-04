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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

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

        // 3️⃣ Re-key the stream using the user id
        KStream<String, User> rekeyedStream = userStream.selectKey((key, user) -> String.valueOf(user.getId()));

        // 4️⃣ Define 1-minute tumbling windows aligned to the minute
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
                .advanceBy(Duration.ofMinutes(1));

        // 5️⃣ Process windowed data - count messages and collect distinct first names
        rekeyedStream
                .groupByKey()
                .windowedBy(timeWindows)
                .aggregate(
                        () -> new UserWindowStats(0L, new HashSet<>()),
                        (key, user, aggregate) -> {
                            aggregate.count++;
                            aggregate.distinctNames.add(user.getFirstName());
                            return aggregate;
                        },
                        Materialized.with(Serdes.String(), new JsonSerde<>(UserWindowStats.class))
                )
                .toStream()
                .map((windowedKey, stats) -> {
                    // Format key as "userId@windowStart" in ISO-8601
                    String formattedWindowStart = Instant.ofEpochMilli(windowedKey.window().start())
                            .atZone(ZoneOffset.UTC)
                            .format(DateTimeFormatter.ISO_INSTANT);
                    String newKey = windowedKey.key() + "@" + formattedWindowStart;

                    // Format value as "count=<N>,distinctNames=[A,B,C],start=<windowStart>,end=<windowEnd>"
                    String formattedWindowEnd = Instant.ofEpochMilli(windowedKey.window().end())
                            .atZone(ZoneOffset.UTC)
                            .format(DateTimeFormatter.ISO_INSTANT);

                    String namesStr = stats.distinctNames.stream()
                            .sorted()
                            .collect(Collectors.joining(","));

                    String value = String.format("count=%d,distinctNames=[%s],start=%s,end=%s",
                            stats.count, namesStr, formattedWindowStart, formattedWindowEnd);

                    LOGGER.info("Window result - Key: {}, Value: {}", newKey, value);
                    return KeyValue.pair(newKey, value);
                })
                .to("UserMessageWindowedCount", Produced.with(Serdes.String(), Serdes.String()));

        return rekeyedStream;
    }

    // Simple class to hold aggregation state
    private static class UserWindowStats {
        private Long count;
        private Set<String> distinctNames;

        public UserWindowStats() {
            // Required for serialization
        }

        public UserWindowStats(Long count, Set<String> distinctNames) {
            this.count = count;
            this.distinctNames = distinctNames;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public Set<String> getDistinctNames() {
            return distinctNames;
        }

        public void setDistinctNames(Set<String> distinctNames) {
            this.distinctNames = distinctNames;
        }
    }
}