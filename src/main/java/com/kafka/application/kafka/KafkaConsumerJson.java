package com.kafka.application.kafka;

import com.kafka.application.payload.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerJson {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerJson.class);

    @KafkaListener(topics = "MyJsonTopic3", groupId = "myGroup")
    public void consume(User user){
        LOGGER.info(String.format("JSON message received -> %s", user));
    }
}