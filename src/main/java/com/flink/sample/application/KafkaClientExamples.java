package com.flink.sample.application;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * This utility class can be used to perform basic kafka operations
 * to list, create, produce and consume the messages
 * in same way as kafka command line utility on standalone
 * local cluster or using docker container
 */
public class KafkaClientExamples {
    public static void main(String[] args) {

        String currentDir = System.getProperty("user.dir");
        System.out.println("Current working directory: " + currentDir + File.separator);

        if (args.length < 1) {
            System.out.println("Usage: java -jar flink-sample-app.jar <command>" +
                    "Please use list/create/produce/consume");
            System.exit(1);
        }

        Properties properties = buildLocalProperties();

        String command = args[0].toLowerCase();
        String topic = "";
        switch (command) {
            case "create":
                if (args.length != 2) {
                    System.out.println("Please specify the topic to create");
                    System.exit(1);
                }
                topic = args[1].toLowerCase();
                System.out.println("Creating the topic: " + topic);
                createTopic(properties, topic, 1, (short) 1);
                break;
            case "list":
                System.out.println("Listing the topics");
                listTopics(properties);
                break;
            case "produce":
                if (args.length != 3) {
                    System.out.println("Please specify the topic and message to produce");
                    System.exit(1);
                }
                topic = args[1].toLowerCase();
                String message = args[2];
                System.out.println("Produce the message to topic: " + topic);
                producer(properties, topic, message);
                break;
            case "consume":
                if (args.length != 2) {
                    System.out.println("Please specify the topic to consume");
                    System.exit(1);
                }
                topic = args[1].toLowerCase();
                System.out.println("Consume the message from topic: " + topic);
                consumer(properties, topic);
                break;
            default:
                System.out.println("Unknown command: " + command + " \nPlease use valid commands");
                break;
        }
    }

    private static Properties buildLocalProperties() {
        // Kafka broker address
        String bootstrapServers = "localhost:9092";
        // Set the properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);

        //Producer properties
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Consumer properties
        properties.put("group.id", "kafka-client-app");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }

    public static void listTopics(Properties properties) {
        // Create an AdminClient
        try (AdminClient adminClient = KafkaAdminClient.create(properties)) {
            // List topics
            ListTopicsResult topics = adminClient.listTopics();
            System.out.println("Topics in Kafka:");
            topics.names().get().forEach(System.out::println);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Failed to list Kafka topics: " + e.getMessage());
        }
    }

    public static void createTopic(Properties properties, String topicName, int partition, short replication) {
        try (AdminClient adminClient = KafkaAdminClient.create(properties)) {
            // Create a new topic
            NewTopic newTopic = new NewTopic(topicName, partition, replication);

            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            // Check if the topic was created successfully
            result.all().get();
            System.out.println("Topic created successfully: " + topicName);
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Failed to create Kafka topic: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static void producer(Properties properties, String topicName, String message) {
        // Create a producer

        // Create a producer record without a key

        // Send the record to the Kafka topic
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println("Message sent successfully to topic " + metadata.topic() +
                    " to partition " + metadata.partition() +
                    " with offset " + metadata.offset());
        } catch (Exception e) {
            System.err.println("Failed to send message to Kafka: " + e.getMessage());
        }
        // Close the producer
    }

    public static void consumer(Properties properties, String topicName) {
        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topicName));

        // Poll for new messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: key = %s, value = %s, partition = %d, offset = %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the consumer
            consumer.close();
        }
    }
}
