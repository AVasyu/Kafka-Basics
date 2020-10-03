# Kafka-Basics
Repository containing Java code to perform basic operations on Apache Kafka

Download Apache Kafka, which will contain Apache Zookeeper and use the following command to start the services on your local machine:- 
For Windows:- 
1. zookeeper-server-start.bat <PATH_TO_ZOOKEEPER_CONFIG_FOLDER>\zookeeper.properties -> Will start zookeeper required by kafka
2. kafka-server-start.bat <PATH_TO_ZOOKEEPER_CONFIG_FOLDER>\server.properties -> Will start Apache Kafka on local machine

For Linux:-
1. ./zookeeper-server-start.sh <PATH_TO_ZOOKEEPER_CONFIG_FOLDER>\zookeeper.properties -> Will start zookeeper required by kafka
2. ./kafka-server-start.sh <PATH_TO_ZOOKEEPER_CONFIG_FOLDER>\server.properties -> Will start Apache Kafka on local machine

By default Zookeeper will run on 2181 port and kafka bootstrap server will run on 9092 port. You can change the port in the properties file.

This is a maven project. To import the required dependency on your local machine, update the project using command "mvn update" or if the projected is imported in Eclipse IDE, right click on the project -> Select Maven -> Update Project. This will download the dependencies required to connect and interact with Kafka and the underlying zookeeper.

Under the basic folder of this repository, we have the following files:-
1. ProducerDemo.java -> This contains the code to connect to the local kafka instance, create a new topic or get an existing one and asynchronously publish new messages or records to the specified topic.
2. ConsumerDemo.java -> This containts the code to connect to local kafka instance, subscribe to a topic and read messages from that topic either from the starting or after a paricular set of messages based on the consumer group offsets.
3. ProducerDemoKeys.java -> Illustrates an example to send messages to a kafka topic in form of key-value pairs. In a kafka topic, the keys have to be unique across all the records but the values can be same for two or more different keys.
4. ProducerDemoWithCallback.java -> Contains an example where a callback function is called every time a producer publishes a message to a kafka topic. The callback function will list down various properties of the newly published message including the topic name, partition where the record is published, its offset, the timestamp at which it was published etc.
5. ConsumerDemoAssignAndSeek.java -> This contains the code which is used when a user wants to read the messaged from a particular offset of a specified partition. In this we are reading the entire data starting from offset 5 of partition 0.
6. ConsumerDemoGroups.java -> This illustrates the case where a consumer is part of a consumer groups and wants to read messages from the subscribe kafka topic.
7. ConsumerDemoWithThreads.java -> Contains the code where consumer subscribes to a kafka topic and reads data from it using Java Threads.
