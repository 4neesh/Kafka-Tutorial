package com.kafka.application.streams;

import com.kafka.application.payload.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class UserMessageCountStream {
    private final Logger LOGGER = LoggerFactory.getLogger(UserMessageCountStream.class);

    @Bean
    public KStream<String, User> kStream(StreamsBuilder builder) {
        // 1️⃣ Use JsonSerde for User class
        JsonSerde<User> userSerde = new JsonSerde<>(User.class);

        // 2️⃣ Read from input topic with User value type
        KStream<String, User> userStream = builder.stream(
                "MyJsonTopic2",
                Consumed.with(Serdes.String(), userSerde)
        );

        // 3️⃣ Re-key the stream using the user id
        KStream<String, User> rekeyedStream = userStream.selectKey((key, user) -> String.valueOf(user.getId()));

        // 4️⃣ Count messages per user id
        KTable<String, Long> userMessageCounts = rekeyedStream
                .groupByKey()
                .count();

        // 5️⃣ Log the counts
        userMessageCounts
                .toStream()
                .peek((key, count) -> LOGGER.info("User " + key + " count: " + count))
                .to("UserMessageCount", Produced.with(Serdes.String(), Serdes.Long()));

        return rekeyedStream;
    }
}
