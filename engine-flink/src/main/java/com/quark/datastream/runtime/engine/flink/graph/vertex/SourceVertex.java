package com.quark.datastream.runtime.engine.flink.graph.vertex;

import java.util.Map;

import com.google.gson.Gson;
import com.quark.datastream.runtime.common.workflow.WorkflowSource;
import com.quark.datastream.runtime.engine.flink.connectors.cassandra.CassandraSource;
import com.quark.datastream.runtime.engine.flink.connectors.file.FileInputSource;
import com.quark.datastream.runtime.engine.flink.connectors.hive.HiveSource;
import com.quark.datastream.runtime.engine.flink.connectors.kafka.KafkaSource;
import com.quark.datastream.runtime.engine.flink.connectors.mqtt.MqttSource;
import com.quark.datastream.runtime.engine.flink.connectors.mysql.MysqlSource;
import com.quark.datastream.runtime.engine.flink.connectors.oracle.OracleSource;
import com.quark.datastream.runtime.engine.flink.connectors.zmq.ZmqSource;
import com.quark.datastream.runtime.engine.flink.connectors.zmq.common.ZmqConnectionConfig;
import com.quark.datastream.runtime.engine.flink.graph.Vertex;
import com.quark.datastream.runtime.engine.flink.schema.DataSetSchema;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceVertex implements Vertex {

    private StreamExecutionEnvironment env;
    private WorkflowSource config;

    public SourceVertex(StreamExecutionEnvironment env, WorkflowSource config) {
        this.env = env;
        this.config = config;
    }

    @Override
    public Long getId() {
        return this.config.getId();
    }

    @Override
    public DataStream<DataSet> serve() throws Exception {
        Map<String, Object> properties = this.config.getConfig().getProperties();
        if (!properties.containsKey("dataType") || !properties.containsKey("dataSource")) {
            throw new NullPointerException("dataType and dataSource must be specified");
        }
        String type = ((String) properties.get("dataType"));
        String source = ((String) properties.get("dataSource"));

        if (type == null || source == null) {
            throw new NullPointerException("Null sink error");
        }

        if (type.equals("") || source.equals("")) {
            throw new NullPointerException("Empty sink error");
        }

        type = type.toLowerCase();
        Gson gson = new Gson();

        switch (type) {
            case "zmq":
                String[] dataSource_zmq = source.split(":");
                ZmqConnectionConfig zmqConnectionConfig = new ZmqConnectionConfig.Builder()
                        .setHost(dataSource_zmq[0].trim())
                        .setPort(Integer.parseInt(dataSource_zmq[1].trim()))
                        .setIoThreads(1)
                        .build();
                return env.addSource(new ZmqSource<>(zmqConnectionConfig,
                        dataSource_zmq[2], new DataSetSchema())).setParallelism(1);
            case "ezmq":
                String[] dataSource_ezmq = source.split(":");
                String host = dataSource_ezmq[0].trim();
                int port = Integer.parseInt(dataSource_ezmq[1].trim());
     /* if (dataSource.length == 3) {
        String topic = dataSource[2].trim();
        return env.addSource(new EzmqSource(host, port, topic)).setParallelism(1);
      } else {
        return env.addSource(new EzmqSource(host, port)).setParallelism(1);
      }*/
                break;
            case "f":
                String meta = ((String) properties.get("name"));
                return env.addSource(new FileInputSource(source, meta)).setParallelism(1);
            case "mqttsource":
                Map mqttConfig = gson.fromJson(source, Map.class);
                String ip_mqtt = (String) mqttConfig.get("ip");
                Integer port_mqtt = Integer.parseInt((String) mqttConfig.get("port"));
                String clientId = (String) mqttConfig.get("clientId");
                Boolean isNewSession = (Boolean) mqttConfig.get("isNewSession");
                String username_mqtt = (String) mqttConfig.get("username");
                String password_mqtt = (String) mqttConfig.get("password");
                String topic_mqtt = (String) mqttConfig.get("topic");
                Integer qos_mqtt = Integer.parseInt((String) mqttConfig.get("qos"));
                MqttSource mqttSource = new MqttSource(ip_mqtt, port_mqtt, clientId, isNewSession, username_mqtt, password_mqtt, topic_mqtt, qos_mqtt);
                return env.addSource(mqttSource, "mqtt source");
            case "mysqlsource":
                Map mysqlSourceConfig = gson.fromJson(source, Map.class);
                MysqlSource mysqlSource = new MysqlSource(mysqlSourceConfig);
                return env.addSource(mysqlSource, "mysql source");
            case "oraclesource":
                Map oracleSourceConfig = gson.fromJson(source, Map.class);
                OracleSource oracleSource = new OracleSource(oracleSourceConfig);
                return env.addSource(oracleSource, "oracle source");
            case "hivesource":
                Map hiveSourceConfig = gson.fromJson(source, Map.class);
                HiveSource hiveSource = new HiveSource(hiveSourceConfig);
                return env.addSource(hiveSource, "hive source");
            case "cassandrasource":
                Map cassandraSourceConfig = gson.fromJson(source, Map.class);
                CassandraSource cassandraSource = new CassandraSource(cassandraSourceConfig);
                return env.addSource(cassandraSource, "cassandra source");
            case "kafkasource":
                Map kafkaSourceCpnfig = gson.fromJson(source, Map.class);
                String servers = (String) kafkaSourceCpnfig.get("servers");
                String groupId = (String) kafkaSourceCpnfig.get("groupId");
                String topic = (String) kafkaSourceCpnfig.get("topic");
                int qos = Integer.parseInt((String) kafkaSourceCpnfig.get("qos"));
                KafkaSource kafkaSource = new KafkaSource(servers, groupId, topic, qos);
                return env.addSource(kafkaSource, "kafka source");
            default:
                throw new UnsupportedOperationException("Unsupported input data type: " + type);
        }

//    if (type.equals("zmq")) {
//
//    } else if (type.equals("ezmq")) {
//
//    } else if (type.equals("f")) {
//
//    } else {
//
//    }
        return null;
    }

    @Override
    public void setInflux(DataStream<DataSet> influx) {
        return;
    }

}
