package com.quark.datastream.runtime.engine.flink.connectors.cassandra.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.combinator.testing.Str;

public class CassandraConnect {

    private final static Logger LOGGER = LoggerFactory.getLogger(CassandraConnect.class);
    private static PoolingOptions poolingOptions = new PoolingOptions();

    static {
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 32)
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 2)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 4)
                .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, 4)
                .setHeartbeatIntervalSeconds(60);
    }

    /**
     * cassandra数据库连接池信息组
     */
    private HashMap<String, Cluster> cassandraPools = new HashMap<>();

    /**
     *  创建Cluster实例
     *
     * @param host
     * 		   主机地址
     * @param port
     * 		   端口，可选参数
     * @param userName
     * 		   用户名
     * @param password
     * 		   密码
     * @return
     * 		   Session
     */
    public Cluster createCluster(String host, String port, String userName, String password) {
        Cluster cluster = null;
        if (!cassandraPools.containsKey(host)) {
            cluster = null == port ?
                    Cluster.builder().addContactPoints(host)
                            .withCredentials(userName, password)
                            .withPoolingOptions(poolingOptions)
                            .build() :
                    Cluster.builder().addContactPoints(host)
                            .withPort(Integer.parseInt(port))
                            .withCredentials(userName, password)
                            .withPoolingOptions(poolingOptions)
                            .build();
            try {
                LOGGER.info("获取cassandra连接...");
                cluster.connect();
                cassandraPools.put(host, cluster);
            } catch (Exception e) {
                LOGGER.error("连接cassandra数据库失败， host: [" + host + "]");
            }
        } else {
            cluster = cassandraPools.get(host);
        }
        return cluster;
    }


    /**
     *
     * 根据库名 判断库是否存在
     *
     * @param dbName
     * @return
     */
    public boolean isDb(Cluster cluster, String dbName) {
        Session session = cluster.connect("system_schema");
        String cql = "select keyspace_name from keyspaces where keyspace_name ='" + dbName + "';";
        if (session.execute(cql).all().size() == 1) {
            session.close();
            return true;
        }
        closeSession(session);
        return false;
    }

    /**
     * 获取表结构信息,包括字段名,字段类型等
     *
     * @param tableName
     * @return
     */
    public List<Map<String, Object>> getTableStructure(Cluster cluster, String dbName, String tableName) {
        LOGGER.info("开始查询表 " + tableName + " 的结构");
        List<Map<String, Object>> tableStructures = new ArrayList<>();

        List<Row> result;
        Session session = null;
        try {
            session = cluster.connect("system_schema");
            String cql = "select column_name,type from columns where keyspace_name = '" + dbName
                    + "' and table_name = '" + tableName + "';";
            ResultSet execute = session.execute(cql);
            result = execute.all();
            if (result.size() > 0) {
                for (int i = 1; i <= result.size(); i++) {
                    String columnName = result.get(i-1).get(0, String.class);
                    String columnType = result.get(i-1).get(1, String.class);
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("index",i);
                    resultMap.put("columnName", columnName);
                    resultMap.put("columnType", columnType);
                    tableStructures.add(resultMap);
                }
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            closeSession(session);
        }
        return tableStructures;
    }

    /**
     * 执行数据查询cql
     *
     * @param cluster
     * @param dbName
     * @param cql
     * @return map key : 字段名 value：字段值
     */
    public List<HashMap<String, Object>> executeQuery(Cluster cluster, String dbName, String cql) {

        List<HashMap<String, Object>> datas = new ArrayList<>();
        Session session = null;
        try {
            session = cluster.connect(dbName);
            LOGGER.info("[cassandra source] =======>  execute cql--->{}", cql);
            com.datastax.driver.core.ResultSet resultSet = session.execute(cql);

            //列信息，包含列名称、类型等
            ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();

            for (Row row : resultSet) {
                HashMap<String, Object> oneRow = new HashMap<>();
                for (int i = 0; i < columnDefinitions.size(); i++) {
                    oneRow.put(columnDefinitions.getName(i), row.getObject(i));
                }
                datas.add(oneRow);
            }
        } catch (Exception e1) {
            e1.printStackTrace();
            System.err.println("执行cql异常");
        } finally {
            closeSession(session);
        }
        return datas;
    }

    /**
     * 执行数据插入cql
     * @param cluster
     * @param dbName
     * @param cql
     */
    public void executeInsert(Cluster cluster, String dbName, String cql) {
        Session session = null;
        try {
            session = cluster.connect(dbName);
            session.execute(cql);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("执行cql异常");
        } finally {
            closeSession(session);
        }
    }

    /**
     * 关闭session
     *
     * @param session
     */
    public void closeSession(Session session) {
        if (null != session) {
            session.close();
        }
    }

    /**
     * 关闭与Cassandra集群的连接
     */
    public void closeCluster(Cluster cluster) {
        if (null != cluster) {
            cluster.close();
        }
    }
}
