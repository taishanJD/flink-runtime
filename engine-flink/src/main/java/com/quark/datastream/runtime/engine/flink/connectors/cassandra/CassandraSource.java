package com.quark.datastream.runtime.engine.flink.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.quark.datastream.runtime.engine.flink.connectors.cassandra.common.CassandraConnect;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBCommonUtils;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class CassandraSource extends RichSourceFunction<DataSet> {

    private final static Logger LOGGER = LoggerFactory.getLogger(CassandraSource.class);
    private Map dbConfig;
    private String host;
    private String port;
    private String userName;
    private String password;
    private transient volatile boolean running;
    private CassandraConnect connect;
    private Cluster cluster;

    public CassandraSource(Map dbConfig) {
        this.dbConfig = dbConfig;
        this.host = (String) dbConfig.get("ip");
        this.port = (String) dbConfig.get("port");
        this.userName = (String) dbConfig.get("username");
        this.password = (String) dbConfig.get("password");
    }

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            this.connect = new CassandraConnect();
            this.cluster = connect.createCluster(host, port, userName, password);
            this.running = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(SourceContext<DataSet> sourceContext) throws Exception {
        String cron = dbConfig.get("period") == null ? null : dbConfig.get("period").toString();
        String cql = (String) dbConfig.get("sql");
        String dbName = (String) dbConfig.get("database");
        if (!connect.isDb(cluster, dbName)) {
            LOGGER.info("[cassandra source] ======> dbName not exists");
            return;
        }

        Gson gson = new Gson();
        while (this.running) {
            if (null != cron && "" != cron.trim()) { // 设置定时周期
                Map<String, Integer> periodMap = gson.fromJson(cron, new TypeToken<TreeMap<String, Integer>>() {
                }.getType());
                int hour = periodMap.get("hour");
                int min = periodMap.get("min");
                int sec = periodMap.get("sec");
                int period = hour * 60 * 60 + min * 60 + sec;

                Thread.sleep(period * 1000);

                DataSet out = DataSet.create();
                List<HashMap<String, Object>> resultMap = null;
                String parsedCql = parseCql(cql);
                resultMap = connect.executeQuery(cluster, dbName, parsedCql);
                for (int i = 0; i < resultMap.size(); i++) {
                    DataSet.Record record = DataSet.Record.create(resultMap.get(i));
                    out.addRecord(record);
                }
                System.out.println("[cassandra source] ======> CassandraSource out dataset--->" + out);
                LOGGER.info("[cassandra source] ======> CassandraSource out time--->{}, out dataset--->{}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()), out);
                sourceContext.collect(out);

            } else { // 未设置定时周期
                String parsedSql = parseCql(cql);
                DataSet out = DataSet.create();
                List<HashMap<String, Object>> resultMap = null;
                resultMap = connect.executeQuery(cluster, dbName, parsedSql);
                for (int i = 0; i < resultMap.size(); i++) {
                    DataSet.Record record = DataSet.Record.create(resultMap.get(i));
                    out.addRecord(record);
                }
                LOGGER.info("[cassandra source] ======> CassandraSource out--->{}", out);
                sourceContext.collect(out);
                this.running = false;
            }
        }
    }

    /**
     * 解析cql中的${}
     *
     * @param cql cql语句
     * @return
     */
    private String parseCql(String cql) {

        //查找cql中的${systime}条件
        List<String> conditions = DBCommonUtils.getDateParams(cql);
        if (0 < conditions.size()) {
            for (String condition : conditions) { // 计算日期
                String systime = DBCommonUtils.parseSelectDate(condition);
                cql = cql.replace("${" + condition + "}", "'" + systime + "'");
            }
            return cql;
        }
        return cql;
    }


    @Override
    public void cancel() {
        try {
            super.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.running = false;
            connect.closeCluster(cluster);
        }
        LOGGER.info("[cassandra source] =======> cassandra source closed!");
    }
}
