package com.kafka.application.kafka;

import com.kafka.application.protobuf.UserProto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerProto {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerProto.class);

    @KafkaListener(
            topics = "MyProtobufTopic",
            groupId = "myGroup",
            containerFactory = "kafkaListenerContainerFactoryProto"
    )
    public void consume(ConsumerRecord<String, UserProto.User> record) {
        UserProto.User user = record.value();
        LOGGER.info("Consumed Protobuf message: id={}, firstName={}, lastName={}",
                user.getId(), user.getFirstName(), user.getLastName());
    }
}