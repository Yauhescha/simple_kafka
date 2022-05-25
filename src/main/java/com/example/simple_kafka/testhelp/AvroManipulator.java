package com.example.simple_kafka.testhelp;
import java.io.File;
import java.io.IOException;

import com.example.simple_kafka.dto.Message;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroManipulator {

    public static void main(String... args) {

        Logger logger = LoggerFactory.getLogger(AvroManipulator.class);

        Message message1 = new Message("Test1", 1, "This");
        Message message2 = new Message("Test2", 1, "is");
        Message message3 = new Message("Test3", 1, "super");
        Message message4 = new Message("Test4", 1, "dope");

        try {
            File file = new File("messages.avro");

            // Serialization
            DatumWriter datumWriter = new SpecificDatumWriter<>(Message.class);
            try (DataFileWriter dfWriter = new DataFileWriter<>(datumWriter)) {
                dfWriter.create(message1.getSchema(), file);
                dfWriter.append(message1);
                dfWriter.append(message2);
                dfWriter.append(message3);
                dfWriter.append(message4);
            }

            // Deserialization
            DatumReader messageDatumReader = new SpecificDatumReader<>(Message.class);
            DataFileReader dfReader = new DataFileReader<>(file, messageDatumReader);
            dfReader.forEach(readMessage -> logger.info(readMessage.toString()));

        } catch (IOException e) {
            logger.error("Something obviously went wrong !", e);
        }
    }
}