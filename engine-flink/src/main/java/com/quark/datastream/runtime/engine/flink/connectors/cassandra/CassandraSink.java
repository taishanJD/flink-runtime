package com.quark.datastream.runtime.engine.flink.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import com.quark.datastream.runtime.engine.flink.connectors.cassandra.common.CassandraConnect;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CassandraSink extends RichSinkFunction<DataSet> {

    private final static Logger LOGGER = LoggerFactory.getLogger(CassandraSource.class);
    private Map dbConfig;
    private String host;
    private String port;
    private String userName;
    private String password;
    private String dbName;
    private String tableName;
    private CassandraConnect connect;
    private Cluster cluster;

    public CassandraSink(Map dbConfig) {
        this.dbConfig = dbConfig;
        this.host = (String) dbConfig.get("ip");
        this.port = (String) dbConfig.get("port");
        this.userName = (String) dbConfig.get("username");
        this.password = (String) dbConfig.get("password");
        this.dbName = (String) dbConfig.get("database");
        this.tableName = (String) dbConfig.get("table");
    }

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            this.connect = new CassandraConnect();
            this.cluster = connect.createCluster(host, port, userName, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(DataSet dataSet) {

        LOGGER.info("[cassandra sink] =======> dataSet in ---> {}", dataSet);

        // 获取表中的字段名及字段类型
        List<Map<String, Object>> tableStructures = connect.getTableStructure(cluster, dbName, tableName);

        List<DataSet.Record> records = dataSet.getRecords();
        for (DataSet.Record record : records) {
            Set<String> keys = record.keySet();
            // 获取插入的Column Names
            String columnNames = getColumnNames(keys, tableStructures);
            // 获取插入的Values
            String values = getValues(columnNames, tableStructures, record);
            String cql = "INSERT INTO " + tableName + "(" + columnNames + ") VALUES (" + values + ")";
            LOGGER.info("[cassandra sink] =======> execute cql ---> {}", cql);
            connect.executeInsert(cluster, dbName, cql);
        }
    }

    // 获取插入的Column Names
    private String getColumnNames(Set<String> keySet, List<Map<String, Object>> tableStructures) {
        StringBuffer sb = new StringBuffer();
        for (Map<String, Object> structure : tableStructures) {
            if (keySet.contains(structure.get("columnName"))) {
                sb.append(structure.get("columnName") + ", ");
            }
        }
        return sb.substring(0, sb.length() - 2);
    }

    // 获取插入的Values
    private String getValues(String columnNames, List<Map<String, Object>> tableStructures, DataSet.Record record) {
        StringBuffer sb = new StringBuffer();
        List<String> columnName = Arrays.asList(columnNames.split(", "));
        for (Map<String, Object> structure : tableStructures) {
            if (columnName.contains(structure.get("columnName"))) {
                String value = record.get(structure.get("columnName")).toString();
                String columnType = structure.get("columnType").toString();
                if (columnType.equals("text") || columnType.equals("timestamp")) {
                    sb.append("'" + value + "', ");
                } else {
                    sb.append(value + ", ");
                }
            }
        }
        return sb.substring(0, sb.length() - 2);
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connect.closeCluster(cluster);
        }
        LOGGER.info("[cassandra sink] =======> cassandra sink closed!");
    }
}
