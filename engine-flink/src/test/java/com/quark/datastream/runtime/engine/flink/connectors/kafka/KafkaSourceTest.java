package com.quark.datastream.runtime.engine.flink.connectors.kafka;

import com.quark.datastream.runtime.engine.flink.connectors.kafka.common.KafkaConnector;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class KafkaSourceTest {
    private String servers = "192.168.2.106:9092,192.168.2.106:9093";
    private String groupId = "test-consumer-group";
    private String topic = "dual";
    private int qos = 1;

    @Test
    public void produceTest() {
        KafkaConnector.produce(servers, topic);
    }

    @Test(timeout = 20000L)
//    @Test
    public void sourceTest() throws Exception {
        KafkaSource kafkaSource = new KafkaSource(servers, groupId, topic, qos);
        kafkaSource.open(null);
        SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
        kafkaSource.run(sourceContext);
        kafkaSource.cancel();
    }
}
