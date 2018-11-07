package com.quark.datastream.runtime.engine.flink.connectors.kafka.common;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnector.class);
    private static Gson gson = new Gson();
    private static final OffsetManager offsetManager = new OffsetManager("kafkaOffsetManager");

    /**
     * @param servers
     * @param groupId
     * @param qos     0：最多消费一次 1：最少消费一次 2：只消费一次
     * @return
     */
    public static KafkaConsumer<String, String> getConsumer(String servers, String groupId, int qos) {
        Properties props = new Properties();
        KafkaConsumer<String, String> consumer = null;
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try {
            switch (qos) {
                case 0:
                    props.put("enable.auto.commit", "true");
                    props.put("auto.commit.interval.ms", "1000");
                    consumer = new KafkaConsumer<>(props);
                    LOGGER.info("[KAFKA] connected");
                    break;
                case 1:
                case 2:
                    props.put("enable.auto.commit", "false");
                    consumer = new KafkaConsumer<>(props);
                    LOGGER.info("[KAFKA] sconnected");
                    break;
            }
        } catch (KafkaException e) {
            e.printStackTrace();
            LOGGER.info("[KAFKA] failed to connect");
        }
        return consumer;
    }

    /**
     * produce massage
     *
     * @param servers
     * @param topic
     */
    public static void produce(String servers, String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(properties);
            for (int i = 0; i < 5; i++) {
                Map<String, String> message = new HashMap<>();
                message.put("key" + i, "value" + i);
                producer.send(new ProducerRecord<>(topic, gson.toJson(message)));// data's format must be JsonStr
                System.out.println("Sent:" + gson.toJson(message));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    // consume at most once
    public void mostOnce(String servers, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }

    // consume at least once
    public void leastOnce(String servers, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
            consumer.commitAsync();
        }
    }

    //only consume once
    public void exactlyOnce(String servers, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("heartbeat.interval.ms", "2000");
        props.put("session.timeout.ms", "6001");
        // Control maximum data on each poll, make sure this value is bigger than the maximum single record size
        props.put("max.partition.fetch.bytes", "140");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic), new RebalanceListener(consumer, offsetManager));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                offsetManager.saveOffsetInExternalStore(record.topic(), record.partition(), record.offset());
            }
        }
    }

    public static void main(String[] args) {
        //produce
        KafkaConnector.produce("192.168.2.106:9092", "dual");

        KafkaConnector kafkaUtil = new KafkaConnector();
//        kafkaUtil.exactlyOnce("192.168.2.106:9092", "test-consumer-group", "test");
//        kafkaUtil.mostOnce("192.168.2.106:9092", "test-consumer-group", "test");
//        kafkaUtil.leastOnce("192.168.2.106:9092", "test-consumer-group", "test");

    }
}
