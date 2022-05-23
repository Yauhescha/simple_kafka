package com.example.simple_kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SimpleKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimpleKafkaApplication.class, args);
    }

}
