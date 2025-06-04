package com.kafka.application.streams;

import com.kafka.application.payload.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
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

@Component
public class UserMessageCountStream {
    private final Logger LOGGER = LoggerFactory.getLogger(UserMessageCountStream.class);

    @Bean
    public KStream<String, User> kStream(StreamsBuilder builder) {
        JsonSerde<User> userSerde = new JsonSerde<>(User.class);

        KStream<String, User> userStream = builder.stream(
                "MyJsonTopic",
                Consumed.with(Serdes.String(), userSerde)
        );

        KStream<String, User> rekeyedStream = userStream.selectKey((key, user) -> String.valueOf(user.getId()));

        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        KTable<Windowed<String>, String> aggregated = rekeyedStream
                .groupByKey()
                .windowedBy(tumblingWindow)
                .aggregate(
                        () -> "0|", // initial value
                        (key, user, agg) -> {
                            String[] parts = agg.split("\\|", 2);
                            long count = Long.parseLong(parts[0]) + 1;

                            Set<String> names = new HashSet<>();
                            if (!parts[1].isEmpty()) {
                                for (String name : parts[1].split(",")) {
                                    names.add(name);
                                }
                            }
                            names.add(user.getFirstName());

                            return count + "|" + String.join(",", names);
                        },
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("user-agg-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));


        aggregated
                .toStream()
                .map((windowedKey, aggValue) -> {
                    String userId = windowedKey.key();
                    Instant start = windowedKey.window().startTime();
                    Instant end = windowedKey.window().endTime();

                    String windowStart = DateTimeFormatter.ISO_INSTANT.format(start.atOffset(ZoneOffset.UTC));
                    String windowEnd = DateTimeFormatter.ISO_INSTANT.format(end.atOffset(ZoneOffset.UTC));

                    String key = userId + "@" + windowStart;

                    String[] parts = aggValue.split("\\|", 2);
                    String count = parts[0];
                    String distinctNames = "[" + parts[1] + "]";

                    String value = String.format(
                            "count=%s,distinctNames=%s,start=%s,end=%s",
                            count,
                            distinctNames,
                            windowStart,
                            windowEnd
                    );

                    LOGGER.info("Windowed Result: {} -> {}", key, value);
                    return KeyValue.pair(key, value);
                })
                .to("UserMessageWindowedCount", Produced.with(Serdes.String(), Serdes.String()));

        return rekeyedStream;
    }
}
