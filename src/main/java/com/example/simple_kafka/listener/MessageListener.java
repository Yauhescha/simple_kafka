package com.example.simple_kafka.listener;

import com.example.simple_kafka.dto.Customer;
import com.example.simple_kafka.dto.Message;
import com.example.simple_kafka.serializator.CustomerDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@EnableKafka
@Configuration
public class MessageListener {

    @Value("${bootstrap.servers}")
    private String kafkaServer;

    @Value("${schema.registry.url}")
    private String schemaUrl;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    private KafkaConsumer<String, Customer> kafkaConsumer;
    private static boolean startedSimpleListen = false;
    private static boolean startedAvroListen = false;

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

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app.1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of("customerDTO"));


        while (true) {
            ConsumerRecords<String, Customer> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, Customer> customerRecord : records) {
                Customer customer = customerRecord.value();
                writeMessage(customer.getId() + " " + customer.getName());
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
        props.put("bootstrap.servers", kafkaServer);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", schemaUrl);

        try (KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("testopic"));

            while (true) {
                ConsumerRecords records = consumer.poll(100);
                for (ConsumerRecord record : (Iterable<ConsumerRecord>) records) {
                    writeMessage(record.value().toString());
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
