package com.quark.datastream.runtime.engine.flink.connectors.kafka.common;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

//
public class RebalanceListener implements ConsumerRebalanceListener {

    private OffsetManager offsetManager;
    private Consumer<String, String> consumer;

    public RebalanceListener(Consumer<String, String> consumer, OffsetManager offsetManager) {
        this.consumer = consumer;
        this.offsetManager = offsetManager;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(), consumer.position(partition));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
        }
    }

}