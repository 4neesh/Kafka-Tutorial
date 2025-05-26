package com.kafka.application.kafka;

import com.kafka.application.payload.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class KafkaProducerJson {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerJson.class);
    private KafkaTemplate<String, User> kafkaTemplate;

    public KafkaProducerJson(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(User user){
        LOGGER.info(String.format("Json message sent: %s", user));

        Message<User> message = MessageBuilder
                .withPayload(user)
                .setHeader(KafkaHeaders.TOPIC, "MyJsonTopic")
                .build();
        
        // Execute within a transaction
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send(message);
            return true;
        });
        
        LOGGER.info("Transaction completed successfully");
    }
}
