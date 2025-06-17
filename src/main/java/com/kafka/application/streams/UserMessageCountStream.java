package com.kafka.application.streams;

import com.kafka.application.payload.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class UserMessageCountStream {
    private final Logger LOGGER = LoggerFactory.getLogger(UserMessageCountStream.class);

    // Track partition message counts
    private final Map<Integer, Long> partitionCounts = new HashMap<>();
    // Track user counts per letter range
    private final Map<String, Map<String, Long>> rangeUserCounts = new HashMap<>();

    @Bean
    public KStream<String, User> kStream(StreamsBuilder builder) {
        // 1️⃣ Use JsonSerde for User class
        JsonSerde<User> userSerde = new JsonSerde<>(User.class);

        // 2️⃣ Read from input topic with User value type
        KStream<String, User> userStream = builder.stream(
                "MyJsonTopic4",
                Consumed.with(Serdes.String(), userSerde)
        );

        // 3️⃣ Process each message to track partition and range statistics
        KStream<String, User> processedStream = userStream.peek((key, user) -> {
            // Determine letter range and partition
            String letterRange = getLetterRange(user.getFirstName());
            int partition = getPartitionForFirstName(user.getFirstName());

            // Update partition counts
            partitionCounts.merge(partition, 1L, Long::sum);

            // Update user counts per range
            rangeUserCounts.computeIfAbsent(letterRange, k -> new HashMap<>())
                    .merge(String.valueOf(user.getId()), 1L, Long::sum);

            // Log partition statistics
            LOGGER.info("Partition {} received message from user {} (firstName: {}, range: {}). Total messages in partition: {}",
                    partition, user.getId(), user.getFirstName(), letterRange, partitionCounts.get(partition));

            // Log top 3 users in this range
            logTop3UsersInRange(letterRange);
        });

        // 4️⃣ Re-key the stream using the user id (already done by producer)
        KStream<String, User> rekeyedStream = processedStream.selectKey((key, user) -> String.valueOf(user.getId()));

        // 5️⃣ Count messages per user id
        KTable<String, Long> userMessageCounts = rekeyedStream
                .groupByKey(Grouped.with(Serdes.String(), userSerde))
                .count(Materialized.as("user-message-counts"));

        // 6️⃣ Output user counts and partition statistics
        userMessageCounts
                .toStream()
                .peek((userId, count) -> LOGGER.info("User {} total message count: {}", userId, count))
                .to("UserMessageCount", Produced.with(Serdes.String(), Serdes.Long()));

        return rekeyedStream;
    }

    private String getLetterRange(String firstName) {
        if (firstName == null || firstName.isEmpty()) {
            return "OTHER";
        }

        char firstLetter = Character.toLowerCase(firstName.charAt(0));

        if (firstLetter >= 'a' && firstLetter <= 'f') {
            return "A-F";
        } else if (firstLetter >= 'g' && firstLetter <= 'l') {
            return "G-L";
        } else if (firstLetter >= 'm' && firstLetter <= 'r') {
            return "M-R";
        } else if (firstLetter >= 's' && firstLetter <= 'z') {
            return "S-Z";
        } else {
            return "OTHER";
        }
    }

    private int getPartitionForFirstName(String firstName) {
        if (firstName == null || firstName.isEmpty()) {
            return 0;
        }

        char firstLetter = Character.toLowerCase(firstName.charAt(0));

        if (firstLetter >= 'a' && firstLetter <= 'f') {
            return 0; // A-F
        } else if (firstLetter >= 'g' && firstLetter <= 'l') {
            return 1; // G-L
        } else if (firstLetter >= 'm' && firstLetter <= 'r') {
            return 2; // M-R
        } else if (firstLetter >= 's' && firstLetter <= 'z') {
            return 3; // S-Z
        } else {
            return 0; // Non-alphabetic characters
        }
    }

    private void logTop3UsersInRange(String range) {
        Map<String, Long> usersInRange = rangeUserCounts.get(range);
        if (usersInRange != null && !usersInRange.isEmpty()) {
            List<Map.Entry<String, Long>> top3 = usersInRange.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(3)
                    .collect(Collectors.toList());

            LOGGER.info("Top 3 active users in range {}: {}", range,
                    top3.stream()
                            .map(entry -> "User " + entry.getKey() + " (" + entry.getValue() + " msgs)")
                            .collect(Collectors.joining(", ")));
        }
    }
}