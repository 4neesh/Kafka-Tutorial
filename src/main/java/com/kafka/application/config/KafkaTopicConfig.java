package com.kafka.application.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic demoTopic(){
        return TopicBuilder.name("MyTopic").build();
    }

    @Bean
    public NewTopic jsonTopic(){
        return TopicBuilder.name("MyJsonTopic3").build();
    }
}
