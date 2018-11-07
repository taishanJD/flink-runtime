package com.quark.datastream.runtime.engine.flink.connectors.kafka;

import com.quark.datastream.runtime.engine.flink.connectors.kafka.common.KafkaConnector;
import com.quark.datastream.runtime.engine.flink.connectors.kafka.common.RebalanceListener;
import com.quark.datastream.runtime.engine.flink.connectors.kafka.common.OffsetManager;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class KafkaSource extends RichSourceFunction<DataSet> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
    private final String servers;
    private final String groupId;
    private final String topic;
    private final int qos;
    private transient KafkaConsumer<String, String> consumer = null;
    private transient volatile boolean running;

    public KafkaSource(String servers, String groupId, String topic, int qos) {
        this.servers = servers;
        this.groupId = groupId;
        this.topic = topic;
        this.qos = qos;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOGGER.info("[KAFKA] connecting to kafka broker: {}", servers);
        this.consumer = KafkaConnector.getConsumer(servers, groupId, qos);
        if (this.consumer == null) {
            LOGGER.info("[KAFKA] source node failed to open");
            return;
        }
        this.running = true;
    }

    @Override
    public void run(SourceContext<DataSet> ctx) throws Exception {

        OffsetManager offsetManager = null;
        switch (qos) {
            case 0:
            case 1:
                consumer.subscribe(Arrays.asList(topic));
                break;
            case 2:
                offsetManager = new OffsetManager("kafkaOffsetManager");
                consumer.subscribe(Arrays.asList(topic), new RebalanceListener(consumer, offsetManager));
                break;
        }

        while (this.running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                DataSet out = DataSet.create();
                out.addRecord(record.value());
                if (qos == 2) {
                    offsetManager.saveOffsetInExternalStore(record.topic(), record.partition(), record.offset());
                }
                ctx.collect(out);
                LOGGER.info("[KAFKA] out===>{}", out);
                System.out.println(out);
            }
            if (qos == 1) {
                consumer.commitAsync();
            }
        }
    }

    @Override
    public void cancel() {
        if (this.consumer != null) {
            this.consumer.close();
        }
        this.running = false;
    }
}
