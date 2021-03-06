package com.example.simple_kafka.controller;

import com.example.simple_kafka.dto.Message;
import io.swagger.annotations.ApiOperation;
import com.example.simple_kafka.dto.Customer;
import com.example.simple_kafka.serializator.CustomerSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
@RequestMapping("book")
public class BookController {

    @Value("${bootstrap.servers}")
    private String kafkaServer;
    
    @Value("${schema.registry.url}")
    private String schemaUrl;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/messageStatic")
    void messageStatic() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", kafkaServer);
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
        kafkaProps.put("bootstrap.servers", kafkaServer);
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
        kafkaProps.put("bootstrap.servers", kafkaServer);
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
        props.put("bootstrap.servers", kafkaServer);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaUrl);


        try (KafkaProducer<String, Message> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, Message> record = new ProducerRecord<>("testopic", new Message("Message-" + i, i, "extra"));
                producer.send(record);
            }
        }
    }

    @PostMapping("/messageStaticAvroWithoutGeneratedSchema")
    public void messageStaticAvroWithoutGeneratedSchema() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaUrl);

        String schemaString = "{\"namespace\": \"customerManagement.avro\", " +
                                 "\"type\": \"record\", " +
                                "\"name\": \"Customer\"," +
                                "\"fields\": [" +
                                    "{\"name\": \"id\", \"type\": \"int\"}," +
                                    "{\"name\": \"name\", \"type\": \"string\"}," +
                                    "{\"name\": \"email\", \"type\": [\"null\", \"string\"], \"default\":null }" +
                              "]}";
        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);
        for (int nCustomers = 0; nCustomers < 5; nCustomers++) {
            String name = "exampleCustomer" + nCustomers;
            String email = "example " + nCustomers + "@example.com";
            GenericRecord customer = new GenericData.Record(schema);
            customer.put("id", nCustomers);
            customer.put("name", name);
            customer.put("email", email);
            ProducerRecord<String, GenericRecord> data = new ProducerRecord<>("customerContacts", name, customer);
            producer.send(data);
        }
        producer.close();
    }
}
