package com.example.simple_kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@EnableKafka
@Configuration
public class MessageListener {

    @Value("${bootstrap.servers}")
    private String kafkaServer;

    @Value("${schema.registry.url}")
    private String schemaUrl;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @KafkaListener(topics = {"msg", "CustomerCountry", "justTopic", "kafka-config-topic"})
    public void msgListener(String msg) {
        writeMessage(msg);
    }

    @KafkaListener(topics = "customerContacts")
    public void bookMessageWithoutGeneratedSchemas(ConsumerRecord<String, String> record) {
        writeMessage(record.value());
    }

    private void writeMessage(String msg) {
        System.out.println("Start message");
        System.out.println();
        System.out.println(msg);
        System.out.println();
        System.out.println("End message");
    }

}
