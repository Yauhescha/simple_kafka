package com.example.simple_kafka.listener;

import com.example.simple_kafka.dto.Customer;
import com.example.simple_kafka.dto.Message;
import com.example.simple_kafka.serializator.CustomerDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@EnableKafka
@Configuration
public class MessageListener {
    private KafkaConsumer<String, Customer> kafkaConsumer;
    private static boolean startedSimpleListen = false;
    private static boolean startedAvroListen = false;

    {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app.1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        kafkaConsumer = new KafkaConsumer<>(props);

        kafkaConsumer.subscribe(List.of("customerDTO"));
    }

    @KafkaListener(topics = "msg")
    public void msgListener(String msg) {
        writeMessage(msg);
    }

    @KafkaListener(topics = "CustomerCountry")
    public void bookStaticListener(String msg) {
        writeMessage(msg);
        bookStaticWithAvroSchemasListener();
    }

    @KafkaListener(topics = "justTopic")
    public void bookMessageWithProps(String msg) {
        writeMessage(msg);
    }

    @Scheduled(cron = "10 * * * * *")
    public void bookMessageWithCustomSerializer() {
        if (startedSimpleListen) return;
        startedSimpleListen = true;
        while (true) {
            ConsumerRecords<String, Customer> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, Customer> customerRecord : records) {
                Customer customer = customerRecord.value();
                System.out.println("Start message");
                System.out.println(customer.getId() + " " + customer.getName());
                System.out.println("End message");
            }
        }
    }

    @KafkaListener(topics = "customerContacts")
    public void bookMessageWithoutGeneratedSchemas(ConsumerRecord<String, String> record) {
        writeMessage(record.value());
    }

    @Scheduled(cron = "15,45 * * * * *")
    public void bookStaticWithAvroSchemasListener() {
        if (startedAvroListen) return;
        startedAvroListen = true;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "mygroup");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        try (KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("testopic"));

            while (true) {
                ConsumerRecords records = consumer.poll(100);
                Iterator<ConsumerRecord> iterator = records.iterator();
                while (iterator.hasNext()) {
                    System.out.println("We got message!");
                    System.out.println(iterator.next().value());
                    System.out.println("End message/");
                }
            }
        }
    }

    private void writeMessage(String msg) {
        System.out.println("Start message");
        System.out.println(msg);
        System.out.println("End message");
    }

}
