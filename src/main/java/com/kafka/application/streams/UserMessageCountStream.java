package com.kafka.application.streams;

import com.kafka.application.payload.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.processor.Processor;
import java.util.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class UserMessageCountStream {
    private final Logger LOGGER = LoggerFactory.getLogger(UserMessageCountStream.class);

    // Helper to get letter range based on firstName (A-F, G-L, M-R, S-Z)
    private String getLetterRange(User user) {
        if (user.getFirstName() == null || user.getFirstName().isEmpty()) {
            return "UNKNOWN";
        }
        char first = Character.toUpperCase(user.getFirstName().charAt(0));
        if (first >= 'A' && first <= 'F') return "A-F";
        else if (first >= 'G' && first <= 'L') return "G-L";
        else if (first >= 'M' && first <= 'R') return "M-R";
        else return "S-Z";
    }

    @Bean
    public KStream<String, User> kStream(StreamsBuilder builder) {
        // 1️⃣ Use JsonSerde for User class
        JsonSerde<User> userSerde = new JsonSerde<>(User.class);

        // 2️⃣ Read from input topic with User value type
        KStream<String, User> userStream = builder.stream(
                "MyJsonTopic3",
                Consumed.with(Serdes.String(), userSerde)
        );

        // Add basic logging for messages per partition (for real-time tracking visibility)
        // Note: Full per-partition counting requires state stores or custom processors beyond current constraints
        userStream.process(() -> new Processor<String, User>() {
            private ProcessorContext context;
            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public void process(String key, User user) {
                // Log message receipt with partition for basic real-time tracking
                LOGGER.info("Partition {} received message for user ID {}", context.partition(), user.getId());
            }
            @Override
            public void close() {}
        });

        // 3️⃣ Re-key the stream using the user id
        KStream<String, User> rekeyedStream = userStream.selectKey((key, user) -> String.valueOf(user.getId()));

        // 4️⃣ Count messages per user id (existing logic)
        KTable<String, Long> userMessageCounts = rekeyedStream
                .groupByKey()
                .count();

        // 5️⃣ Log the counts (existing logic)
        userMessageCounts
                .toStream()
                .peek((key, count) -> LOGGER.info("User " + key + " count: " + count))
                .to("UserMessageCount", Produced.with(Serdes.String(), Serdes.Long()));

        // New: Compute top 3 most active users per letter range
        userStream.groupBy((key, user) -> getLetterRange(user), Grouped.with(Serdes.String(), userSerde))
                .aggregate(
                        HashMap<String, Long>::new,  // Initializer: Map of user ID to count
                        (range, user, map) -> {
                            String userId = String.valueOf(user.getId());
                            map.put(userId, map.getOrDefault(userId, 0L) + 1);
                            return map;
                        },
                        (range, map1, map2) -> {
                            // Merge aggregator
                            for (Map.Entry<String, Long> entry : map2.entrySet()) {
                                String userId = entry.getKey();
                                map1.put(userId, map1.getOrDefault(userId, 0L) + entry.getValue());
                            }
                            return map1;
                        }
                )
                .toStream()
                .peek((range, map) -> {
                    // Compute top 3 users by sorting the map (user ID with highest counts first)
                    List<Map.Entry<String, Long>> sortedUsers = new ArrayList<>(map.entrySet());
                    sortedUsers.sort((a, b) -> b.getValue().compareTo(a.getValue()));  // Descending order
                    List<Map.Entry<String, Long>> top3 = sortedUsers.subList(0, Math.min(3, sortedUsers.size()));
                    LOGGER.info("Top 3 users for range {}: {}", range, top3);
                });

        return rekeyedStream;
    }
}
