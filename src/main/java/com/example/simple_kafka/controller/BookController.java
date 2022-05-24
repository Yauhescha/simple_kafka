package com.example.simple_kafka.controller;

import io.swagger.annotations.ApiOperation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import com.example.simple_kafka.dto.Customer;
import com.example.simple_kafka.serializator.CustomerSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Properties;

@RestController
@RequestMapping("book")
public class BookController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/messageStatic")
    void postStaticMessage() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
        ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
        producer.send(record);
    }

    @PostMapping("/messageSimple")
    void postMessageWithPattern(String key, String value) {
        kafkaTemplate.send("justTopic", key, value);
    }

    @PostMapping("/messageDTO")
    void postMessageWithPattern(@RequestBody Customer customer) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", CustomerSerializer.class);

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(kafkaProps);

        ProducerRecord<String, Customer> record = new ProducerRecord<>("customerDTO", customer.getId()+"", customer);
        producer.send(record);
    }

    @PostMapping("/messageStaticAvro")
    @ApiOperation(value = "Trying to send message with avro chemas. Withoout 'schema.registry.urr' on 'http://localhost:8081' will be thrown exception")
    public void postStaticMessageWithAvroSerializer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");
        String schemaString = "{\"namespace\": \"customerManagement.avro\","+
                                "\"type\": \"record\", " +
                                "\"name\": \"Customer\"," +
                                "\"fields\": [" +
                                    "{\"name\": \"id\", \"type\": \"int\"}," +
                                    "{\"name\": \"name\", \"type\": \"string\"}" +
                                "]}";
        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);
        for (int nCustomers = 0; nCustomers < 5; nCustomers++) {
            String name = "exampleCustomer" + nCustomers;
            String email = "example " + nCustomers + "@example.com";
            GenericRecord customer = new GenericData.Record(schema);
            customer.put("id", nCustomers);
            customer.put("name", email);
            ProducerRecord<String, GenericRecord> data = new ProducerRecord<>("customerContacts", name, customer);
            producer.send(data);
        }
    }
}
