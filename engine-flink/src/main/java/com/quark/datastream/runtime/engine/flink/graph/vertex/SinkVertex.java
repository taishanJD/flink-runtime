package com.quark.datastream.runtime.engine.flink.graph.vertex;

import java.util.Map;

import com.google.gson.Gson;
import com.quark.datastream.runtime.common.workflow.WorkflowSink;
import com.quark.datastream.runtime.engine.flink.connectors.cassandra.CassandraSink;
import com.quark.datastream.runtime.engine.flink.connectors.email_sink.EmailSinkNode;
import com.quark.datastream.runtime.engine.flink.connectors.file.FileOutputSink;
import com.quark.datastream.runtime.engine.flink.connectors.hive.HiveSink;
import com.quark.datastream.runtime.engine.flink.connectors.mongodb.MongoDbSink;
import com.quark.datastream.runtime.engine.flink.connectors.mysql.MysqlSink;
import com.quark.datastream.runtime.engine.flink.connectors.oracle.OracleSink;
import com.quark.datastream.runtime.engine.flink.connectors.websocket.WebSocketServerSink;
import com.quark.datastream.runtime.engine.flink.connectors.zmq.ZmqSink;
import com.quark.datastream.runtime.engine.flink.connectors.zmq.common.ZmqConnectionConfig;
import com.quark.datastream.runtime.engine.flink.graph.Vertex;
import com.quark.datastream.runtime.engine.flink.schema.DataSetSchema;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;

public class SinkVertex implements Vertex {

    private WorkflowSink config;
    private DataStream<DataSet> influx = null;

    public SinkVertex(WorkflowSink config) {
        this.config = config;
    }

    @Override
    public Long getId() {
        return this.config.getId();
    }

    @Override
    public DataStream<DataSet> serve() throws Exception {
        Map<String, Object> properties = this.config.getConfig().getProperties();
        if (!properties.containsKey("dataType") || !properties.containsKey("dataSink")) {
            throw new NullPointerException("dataType and dataSink must be specified");
        }
        String type = ((String) properties.get("dataType"));
        String sink = ((String) properties.get("dataSink"));

        if (type == null || sink == null) {
            throw new NullPointerException("Null sink error");
        }

        if (type.equals("") || sink.equals("")) {
            throw new NullPointerException("Empty sink error");
        }

        type = type.toLowerCase();
        Gson gson = new Gson();

        switch (type) {
            case "zmq":
                String[] dataSink_zmq = sink.split(":");
                ZmqConnectionConfig zmqConnectionConfig = new ZmqConnectionConfig.Builder()
                        .setHost(dataSink_zmq[0].trim())
                        .setPort(Integer.parseInt(dataSink_zmq[1].trim()))
                        .setIoThreads(1)
                        .build();

                influx.addSink(new ZmqSink<>(zmqConnectionConfig, dataSink_zmq[2], new DataSetSchema()))
                        .setParallelism(1);
                break;
            case "ws":
                String[] dataSink_ws = sink.split(":");
                influx.addSink(new WebSocketServerSink(Integer.parseInt(dataSink_ws[1])))
                        .setParallelism(1);
                break;
            case "ezmq":
                String[] dataSink_ezmq = sink.split(":");
                int port = Integer.parseInt(dataSink_ezmq[1].trim());
//      influx.addSink(new EzmqSink(port)).setParallelism(1);
                break;
            case "f":
                String outputFilePath = sink;
                if (!outputFilePath.endsWith(".txt")) {
                    outputFilePath += ".txt";
                }
                influx.addSink(new FileOutputSink(outputFilePath));
                break;
            case "mongodb":
                String[] dataSink_mongodb = sink.split(":");
                String[] name = ((String) properties.get("name")).split(":", 2);
                influx.addSink(new MongoDbSink(dataSink_mongodb[0], Integer.parseInt(dataSink_mongodb[1]), name[0], name[1]))
                        .setParallelism(1);
                break;
            case "mysqlsink":
                Map mysqlConfig = gson.fromJson(sink, Map.class);
//        String ip_mysql = (String) mysqlConfig.get("ip");
//        Integer port_mysql = Integer.parseInt((String) mysqlConfig.get("port"));
//        String db_mysql = (String) mysqlConfig.get("database");
//        String username_mysql = (String) mysqlConfig.get("username");
//        String password_mysql = (String) mysqlConfig.get("password");
//        String table_mysql = (String) mysqlConfig.get("table");
//        MysqlSink mysqlSink = new MysqlSink(ip_mysql,port_mysql,db_mysql,username_mysql,password_mysql,table_mysql);
                MysqlSink mysqlSink = new MysqlSink(mysqlConfig);
                influx.addSink(mysqlSink);
                break;
            case "oraclesink":
                Map oracleConfig = gson.fromJson(sink, Map.class);
//        String ip_oracle = (String) oracleConfig.get("ip");
//        Integer port_oracle = Integer.parseInt((String) oracleConfig.get("port"));
//        String db_oracle = (String) oracleConfig.get("database");
//        String username_oracle = (String) oracleConfig.get("username");
//        String password_oracle = (String) oracleConfig.get("password");
//        String table_oracle = (String) oracleConfig.get("table");
//        OracleSink oracleSink = new OracleSink(ip_oracle,port_oracle,db_oracle,username_oracle,password_oracle,table_oracle);
                OracleSink oracleSink = new OracleSink(oracleConfig);
                influx.addSink(oracleSink);
                break;
            case "hivesink":
                Map hiveConfig = gson.fromJson(sink, Map.class);
                HiveSink hiveSink = new HiveSink(hiveConfig);
                influx.addSink(hiveSink);
                break;
            case "emailsink":
                Map emailSinkConfig = gson.fromJson(sink, Map.class);
                String server = (String) emailSinkConfig.get("server");
                String port_email = (String) emailSinkConfig.get("port");
                String username_email = (String) emailSinkConfig.get("username");
                String password_email = (String) emailSinkConfig.get("password");
                String receivers = (String) emailSinkConfig.get("receivers");
                String customerReceivers = (String) emailSinkConfig.get("customerReceivers");
                String subject = (String) emailSinkConfig.get("title");
                String contentKey = (String) emailSinkConfig.get("contentKey");
                EmailSinkNode emailSink = new EmailSinkNode(server, port_email, username_email, password_email, receivers, customerReceivers, subject, contentKey);
                influx.addSink(emailSink);
                break;
            case "cassandrasink":
                Map cassandraSinkConfig = gson.fromJson(sink, Map.class);
                CassandraSink cassandraSink = new CassandraSink(cassandraSinkConfig);
                influx.addSink(cassandraSink);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported output data type: " + type);
        }

//    if (type.equals("zmq")) {
//
//    } else if (type.equals("ws")) {
//
//    } else if (type.equals("ezmq")) {
//
//    } else if (type.equals("f")) {
//
//    } else if (type.equals("mongodb")) {
//
//    } else {
//
//    }

        return null;
    }

    @Override
    public void setInflux(DataStream<DataSet> influx) {
        if (this.influx == null) {
            this.influx = influx;
        } else {
            this.influx = this.influx.union(influx).flatMap((dataSet, collector) ->
                    collector.collect(dataSet)
            );
        }
    }
}
