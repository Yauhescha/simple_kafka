package com.example.simple_kafka.listener;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@EnableKafka
@Configuration
public class MessageListener {

    @KafkaListener(topics = "msg")
    public void msgListener(String msg) {
        writeMessage(msg);
    }

    @KafkaListener(topics = "CustomerCountry")
    public void bookStaticListener(String msg) {
        writeMessage(msg);
    }

    @KafkaListener(topics = "justTopic")
    public void bookMessageWithPattern(String msg) {
        writeMessage(msg);
    }

    private void writeMessage(String msg) {
        System.out.println("Start message");
        System.out.println(msg);
        System.out.println("End message");
    }

}
