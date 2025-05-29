package com.kafka.application.kafka;

import com.kafka.application.protobuf.UserProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerProto {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerProto.class);
    private final KafkaTemplate<String, UserProto.User> kafkaTemplate;

    public KafkaProducerProto(KafkaTemplate<String, UserProto.User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(UserProto.User user) {
        LOGGER.info("Sending Protobuf message to MyProtobufTopic: {}", user);

        Message<UserProto.User> message = MessageBuilder
                .withPayload(user)
                .setHeader(KafkaHeaders.TOPIC, "MyProtobufTopic")
                .build();

        kafkaTemplate.executeInTransaction(operations -> {

            operations.send(message);
            return true;
        });
    }
}
