package com.example.simple_kafka.listener;

import com.example.simple_kafka.dto.Customer;
import com.example.simple_kafka.serializator.CustomerDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunnableCustomSerializerListener implements Runnable {

    // bookMessageWithCustomSerializer
    @Override
    public void run() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(props);
        try (kafkaConsumer) {
            kafkaConsumer.subscribe(List.of("customerDTO"));
            while (true) {
                ConsumerRecords<String, Customer> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, Customer> customerRecord : records) {
                    Customer customer = customerRecord.value();
                    writeMessage(customer.getId() + " " + customer.getName());
                }
            }
        }
    }

    private void writeMessage(String msg) {
        System.out.println("Start message");
        System.out.println();
        System.out.println(msg);
        System.out.println();
        System.out.println("End message");
    }
}
