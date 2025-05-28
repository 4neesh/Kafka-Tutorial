package com.kafka.application.controller;

import com.kafka.application.kafka.KafkaProducerProto;
import com.kafka.application.protobuf.UserProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kafka/proto")
public class MessageProtoController {

    private final Logger LOGGER = LoggerFactory.getLogger(MessageProtoController.class);
    private final KafkaProducerProto kafkaProducer;

    public MessageProtoController(KafkaProducerProto kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping(value = "/publish", consumes = "application/x-protobuf")
    public ResponseEntity<String> publish(@RequestBody UserProto.User user){

        kafkaProducer.sendMessage(user);
        return ResponseEntity.ok("Protobuf message received: " + user);
    }
}
