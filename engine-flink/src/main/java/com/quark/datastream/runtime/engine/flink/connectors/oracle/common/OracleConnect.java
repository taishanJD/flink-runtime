package com.quark.datastream.runtime.engine.flink.connectors.oracle.common;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DruidDataSourceProperties;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBConnect;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBOperate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class OracleConnect extends DBConnect implements DBOperate {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnect.class);

    public OracleConnect(Map oracleConfig) throws Exception {
        Properties properties = new Properties();
        String ip = (String) oracleConfig.get("ip");
        Integer port = Integer.parseInt((String) oracleConfig.get("port"));
        String database = (String) oracleConfig.get("database");
        String username = (String) oracleConfig.get("username");
        String password = (String) oracleConfig.get("password");

        String url = "jdbc:oracle:thin:@//" + ip + ":" + port + "/" + database;
        properties.setProperty(DruidDataSourceProperties.PROP_URL, url);
        properties.setProperty(DruidDataSourceProperties.PROP_USERNAME, username);
        properties.setProperty(DruidDataSourceProperties.PROP_PASSWORD, password);
        properties.setProperty(DruidDataSourceProperties.PROP_DRIVERCLASSNAME, "oracle.jdbc.driver.OracleDriver");
        properties.setProperty(DruidDataSourceProperties.PROP_MAXACTIVE, String.valueOf(10));
        properties.setProperty(DruidDataSourceProperties.PROP_MINIDLE, String.valueOf(0));
        properties.setProperty(DruidDataSourceProperties.PROP_INITIALSIZE, String.valueOf(10));
        properties.setProperty(DruidDataSourceProperties.NUPROP_MAXWAIT, String.valueOf(10000));
        properties.setProperty(DruidDataSourceProperties.PROP_CONNECTIONPROPERTIES, "oracle.jdbc.ReadTimeout=5000;oracle.net.CONNECT_TIMEOUT=10000;useUnicode=true;characterEncoding=UTF8");
//        properties.setProperty("validationQuery", "SELECT 1 FROM DUAL");
//        properties.setProperty("validationQueryTimeout", String.valueOf(2));
        dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
    }

    @Override
    public List<Map<String, Object>> getTableStructure(String tableName) {
        LOGGER.info("[oracle operate] ======>  get table structure, table name =={}", tableName);
        List<Map<String, Object>> tableStructures = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
            try {
                tableStructures = new ArrayList<>();
                connection = getConnection();
                StringBuffer sql = new StringBuffer();
                sql.append("select * from user_tab_columns where table_name ='");
                sql.append(tableName.toUpperCase());
                sql.append("'");
                pstmt = connection.prepareStatement(sql.toString());
                resultSet = pstmt.executeQuery();

                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String columnName = resultSet.getString("COLUMN_NAME");//获得指定列的列名
//                    String dataType = resultSet.getString("DATA_TYPE");//获取数据类型
//                    String dataLength = resultSet.getString("DATA_LENGTH");//获取数据长度
//                    String dataScale = resultSet.getString("DATA_SCALE");//获取数据精度
//                    String nullable = resultSet.getString("NULLABLE");//获取是否为空
                    map.put("columnName", columnName);
                    tableStructures.add(map);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                release(resultSet, pstmt, connection);
            }
        return tableStructures;
    }

    public static void main(String[] a) throws Exception {

        String url = "jdbc:oracle:thin:@//192.168.2.114:8081/orcl";
        Properties properties = new Properties();
        properties.setProperty(DruidDataSourceProperties.PROP_URL, url);
        properties.setProperty(DruidDataSourceProperties.PROP_USERNAME, "test");
        properties.setProperty(DruidDataSourceProperties.PROP_PASSWORD, "test123456");
        properties.setProperty(DruidDataSourceProperties.PROP_DRIVERCLASSNAME, "oracle.jdbc.driver.OracleDriver");
        properties.setProperty("maxActive", String.valueOf(10));
        properties.setProperty(DruidDataSourceProperties.PROP_MINIDLE, String.valueOf(0));
        properties.setProperty(DruidDataSourceProperties.PROP_INITIALSIZE, String.valueOf(10));
        properties.setProperty(DruidDataSourceProperties.NUPROP_MAXWAIT, String.valueOf(2000));
        properties.setProperty("phyTimeoutMillis", String.valueOf(2000));
        properties.setProperty("validationQuery", "SELECT 1 FROM DUAL");
        properties.setProperty("validationQueryTimeout", String.valueOf(5));
        properties.setProperty(DruidDataSourceProperties.PROP_CONNECTIONPROPERTIES, "oracle.jdbc.ReadTimeout=5000;oracle.net.CONNECT_TIMEOUT=10000;useUnicode=true;characterEncoding=UTF8");

        OracleConnect connect = new OracleConnect(properties);
        List<Map<String, Object>> list =  connect.getTableStructure("bolier");
        System.out.println(list);
    }
}
