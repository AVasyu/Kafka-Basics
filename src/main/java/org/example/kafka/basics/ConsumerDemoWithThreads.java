package org.example.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {}

    private void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        logger.info("Creating Kafka Consumer Thread");
        CountDownLatch latch = new CountDownLatch(1);

        // Create Consumer Runnable
        Runnable myConsumerThread = new ConsumerThreads(latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // Add Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught Shutdown Hook");
            ((ConsumerThreads) myConsumerThread).shutdown();
            try{
                latch.await();
            } catch (Exception e){
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application Got Interrupted ", e);
        } finally {
            logger.info("Application Is Closing");
        }
    }

    public class ConsumerThreads implements Runnable{

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> kafkaConsumer;
        final Logger logger = LoggerFactory.getLogger(ConsumerThreads.class);

        public ConsumerThreads(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;

            // Create Consumer Configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Deserialization of data from bytes
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fifth-application");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create Consumer
            this.kafkaConsumer = new KafkaConsumer<String, String>(properties);

            // Subscribe consumer to out topic(s)
            kafkaConsumer.subscribe(Collections.singleton("first_topic"));
        }

        @Override
        public void run() {
            // Poll For New Data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset" + record.offset());
                    }
                }
            } catch(WakeupException e){
                logger.info("Received ShutDown Signal");
            } finally {
                kafkaConsumer.close();
                countDownLatch.countDown(); // Tell our main code that we are done with our consumer
            }
        }

        public void shutdown() {
            kafkaConsumer.wakeup(); // wakeup() is a special method to interrupt kafkaConsumer.poll(). It will throw exception WakeUpException
        }
    }
}
