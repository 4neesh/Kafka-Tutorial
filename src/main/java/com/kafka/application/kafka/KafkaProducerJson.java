package com.kafka.application.kafka;

import com.kafka.application.payload.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.kafka.support.KafkaHeaders;

@Service
public class KafkaProducerJson {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerJson.class);
    private final KafkaTemplate<String, User> kafkaTemplate;

    public KafkaProducerJson(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(User user) {
        LOGGER.info("Json message sent: {}", user);

        // Use user ID as the message key for routing
        String messageKey = String.valueOf(user.getId());

        // Determine partition based on firstName starting letter
        int partition = getPartitionForFirstName(user.getFirstName());

        Message<User> message = MessageBuilder
                .withPayload(user)
                .setHeader(KafkaHeaders.TOPIC, "MyJsonTopic4")
                .setHeader(KafkaHeaders.KEY, messageKey)
                .setHeader(KafkaHeaders.PARTITION, partition)
                .build();

        // Execute within a transaction
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send(message);
            return true;
        });

        LOGGER.info("Transaction completed successfully - User {} routed to partition {} (firstName: {})",
                user.getId(), partition, user.getFirstName());
    }

    private int getPartitionForFirstName(String firstName) {
        if (firstName == null || firstName.isEmpty()) {
            return 0; // Default partition for null/empty names
        }

        char firstLetter = Character.toLowerCase(firstName.charAt(0));

        // Fixed letter ranges for even distribution across 4 partitions
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
}