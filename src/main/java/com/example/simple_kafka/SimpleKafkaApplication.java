package com.example.simple_kafka;

import com.example.simple_kafka.listener.RunnableCustomSerializerListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SimpleKafkaApplication {

    public static void main(String[] args) {
        new Thread(new RunnableCustomSerializerListener()).start();
        SpringApplication.run(SimpleKafkaApplication.class, args);
    }

}
