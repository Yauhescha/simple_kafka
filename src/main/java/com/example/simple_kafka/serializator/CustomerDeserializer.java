package com.example.simple_kafka.serializator;

import com.example.simple_kafka.dto.Customer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {
    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Customer deserialize(String topic, byte[] data) {
        int cid;
        int nameSize;
        String cname;
        try {
            if (data == null) {
                return null;
            }
            if (data.length < 8) {
                throw new SerializationException("Size of data received by IntegerDeserializer is shorter than expected!");
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            cid = buffer.getInt();
            nameSize = buffer.getInt();
            byte[] nameBytes = new byte[nameSize];
            buffer.get(nameBytes);
            cname = new String(nameBytes, "UTF-8");
            return new Customer(cid, cname);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Customer: " + e.getMessage());
        }
    }

}
