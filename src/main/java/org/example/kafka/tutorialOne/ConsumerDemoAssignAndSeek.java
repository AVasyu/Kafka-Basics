package org.example.kafka.tutorialOne;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

        // Create Consumer Configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Deserialization of data from bytes
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read from beginning of topic, other values are latest or none

        // Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // Assign And Seek are mostly used to replay data or fetch a specific message
        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
        kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));
        // Seek
        kafkaConsumer.seek(partitionToReadFrom, 5L); // Start reading messages from partition 0, offset 5 of topic my_first_topic

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;

        // Poll For New Data
        while(keepOnReading){
            ConsumerRecords<String, String>records = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset" + record.offset());
                numberOfMessagesToRead--;
                if (numberOfMessagesToRead == 0) {
                    keepOnReading = false;
                    break;
                }
            }
            logger.info("Exiting the application");
        }
    }
}
