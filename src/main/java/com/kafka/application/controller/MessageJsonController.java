package com.kafka.application.controller;

import com.kafka.application.kafka.KafkaProducerJson;
import com.kafka.application.payload.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kafka")
public class MessageJsonController {

    private final Logger LOGGER = LoggerFactory.getLogger(MessageJsonController.class);
    private KafkaProducerJson kafkaProducer;

    public MessageJsonController(KafkaProducerJson kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestBody User user){
        kafkaProducer.sendMessage(user);
        LOGGER.info(String.format("Sending JSON Message: %s", user));
        return ResponseEntity.ok(String.format("Message sent: %s", user));
    }
}
