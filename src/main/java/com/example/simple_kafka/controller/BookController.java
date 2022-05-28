package com.example.simple_kafka.controller;

import com.example.simple_kafka.dto.Message;
import io.swagger.annotations.ApiOperation;
import com.example.simple_kafka.dto.Customer;
import com.example.simple_kafka.serializator.CustomerSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Properties;
import java.util.concurrent.Future;

@RestController
@RequestMapping("book")
public class BookController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/messageStatic")
    void messageStatic() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
        ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
        producer.send(record);
        producer.close();
    }

    @PostMapping("/messageSimple")
    void messageSimple(String key, String value) {
        kafkaTemplate.send("justTopic", key, value);
    }

    @PostMapping("/messageDTO")
    void messageDTO(@RequestBody Customer customer) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", CustomerSerializer.class);

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(kafkaProps);

        ProducerRecord<String, Customer> record = new ProducerRecord<>("customerDTO", customer.getId() + "", customer);
        producer.send(record);
        producer.close();
    }

    @PostMapping("/messageDTOWithAnswer")
    void messageDTOWithAnswer(@RequestBody Customer customer) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", CustomerSerializer.class);

        try (KafkaProducer<String, Customer> producer = new KafkaProducer<>(kafkaProps)) {

            ProducerRecord<String, Customer> record = new ProducerRecord<>("customerDTO", customer.getId() + "", customer);
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    System.out.println("Answer contains EXCEPTION!!!!");
                    e.printStackTrace();
                } else {
                    System.out.println("SENT MESSAGE WITHOUT EXCEPTION!");
                }
            });
        }
    }

    @PostMapping("/messageStaticAvro")
    @ApiOperation(value = "Trying to send message with avro schemas. Without 'schema.registry.urr' on 'http://localhost:8081' will be thrown exception")
    public void messageStaticAvro() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");


        try (KafkaProducer<String, Message> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, Message> record = new ProducerRecord<>("testopic", new Message("Message-" + i, i, "extra"));
                producer.send(record);
            }
        }
    }
}
