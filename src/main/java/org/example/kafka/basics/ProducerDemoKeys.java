package org.example.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // What type of data are we sending to kafka for serialization into bytes and deserialization from bytes.
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties); //Want the key to be string and value to be string

        // Create a Producer Record
        for(int i=0;i<10;i++) {
            String topic = "first_topic";
            String value = "Java Message With Key " + Integer.toString(i);
            String key = "id_ " + Integer.toString(i);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key,value);

            // Send Data - Asynchronous
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // This function is executed every time a record is sent successfully or we get an exception
                    if (e == null) {
                        // The record was successfully sent
                        logger.info("Received New Metadata: " + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        // Without these the program will close and data will never be sent to the topic
        producer.flush();
        producer.close();
    }
}
