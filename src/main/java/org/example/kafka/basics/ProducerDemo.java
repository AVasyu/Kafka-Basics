package org.example.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // What type of data are we sending to kafka for serialization and deserialization of it
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties); //Want the key to be string and value to be string

        // Create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "First Java Message");

        // Send Data - Asynchronous
        producer.send(producerRecord);

        // Without these the program will close and data will never be sent to the topic
        producer.flush();
        producer.close();
    }
}
