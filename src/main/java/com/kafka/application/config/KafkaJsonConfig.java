package com.kafka.application.config;

import com.kafka.application.payload.User;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaJsonConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String producerBootstrapServers;

    @Value("${spring.kafka.producer.transaction-id-prefix}")
    private String transactionIdPrefix;

    @Bean
    @Primary
    public ProducerFactory<String, User> producerFactoryJson() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerBootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.springframework.kafka.support.serializer.JsonSerializer.class);
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "proto-" + transactionIdPrefix);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, User> kafkaTemplateJson() {
        return new KafkaTemplate<>(producerFactoryJson());
    }
}