package com.quark.datastream.runtime.engine.flink.connectors.mysql.common;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DruidDataSourceProperties;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBConnect;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBOperate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class MysqlConnect extends DBConnect implements DBOperate {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlConnect.class);

    public MysqlConnect(Map mysqlConfig) throws Exception {

        Properties properties = new Properties();
        String ip = (String) mysqlConfig.get("ip");
        Integer port = Integer.parseInt((String) mysqlConfig.get("port"));
        String database = (String) mysqlConfig.get("database");
        String username = (String) mysqlConfig.get("username");
        String password = (String) mysqlConfig.get("password");

        String url = "jdbc:mysql://" + ip + ":" + port + "/" + database;
        properties.setProperty(DruidDataSourceProperties.PROP_URL, url);
        properties.setProperty(DruidDataSourceProperties.PROP_USERNAME, username);
        properties.setProperty(DruidDataSourceProperties.PROP_PASSWORD, password);
        properties.setProperty(DruidDataSourceProperties.PROP_DRIVERCLASSNAME, "com.mysql.jdbc.Driver");
        properties.setProperty(DruidDataSourceProperties.PROP_MAXACTIVE, String.valueOf(10));
        properties.setProperty(DruidDataSourceProperties.PROP_MINIDLE, String.valueOf(0));
        properties.setProperty(DruidDataSourceProperties.PROP_INITIALSIZE, String.valueOf(10));
        properties.setProperty(DruidDataSourceProperties.NUPROP_MAXWAIT, String.valueOf(10000));

        //JDBC驱动建立连接时附带的连接属性,格式必须为这样：[属性名=property;]，不需要包含username和password两个属性
        //添加connectTimeout和socketTimeout以判断连接池连接数据库是否成功，这两个属性与oracle不同
        properties.setProperty(DruidDataSourceProperties.PROP_CONNECTIONPROPERTIES, "connectTimeout=10000;socketTimeout=5000;useUnicode=true;characterEncoding=UTF8");
//        properties.setProperty("validationQuery", "SELECT 1");
//        properties.setProperty("validationQueryTimeout", String.valueOf(2));
        dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
    }

    @Override
    public List<Map<String, Object>> getTableStructure(String tableName) {
        LOGGER.info("[mysql operate] ======>  get table structure, table name =={}", tableName);
        List<Map<String, Object>> tableStructures = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
            try {
                tableStructures = new ArrayList<>();
                connection = getConnection();
                StringBuffer sql = new StringBuffer();
                sql.append("select * from ");
                sql.append(tableName);
                pstmt = connection.prepareStatement(sql.toString());
                resultSet = pstmt.executeQuery();

                ResultSetMetaData data = resultSet.getMetaData();
                for (int i = 1; i <= data.getColumnCount(); i++) {
                    Map<String, Object> map = new HashMap<>();

                    String columnName = data.getColumnName(i);//获得指定列的列名

                    //                String columnValue = resultSet.getString(i);//获得指定列的列值

                    //            int columnType = data.getColumnType(i);//获得指定列的数据类型编号

                    //            String columnTypeName = data.getColumnTypeName(i);//获得指定列的数据类型名
                    //
                    //                String catalogName = data.getCatalogName(i);//所在的Catalog名字
                    //
                    //                String columnClassName = data.getColumnClassName(i);//对应数据类型的类
                    //
                    //                int columnDisplaySize = data.getColumnDisplaySize(i);//在数据库中类型的最大字符个数
                    //
                    //                String columnLabel = data.getColumnLabel(i);//默认的列的标题
                    //
                    //                String schemaName = data.getSchemaName(i);//获得列的模式

                    //                  int precision = data.getPrecision(i);//某列类型的精确度(类型的长度)

                    //                  int scale = data.getScale(i);//小数点后的位数

                    //                String tableNamea = data.getTableName(i);//获取某列对应的表名

                    //                boolean isAutoInctement = data.isAutoIncrement(i);// 是否自动递增

                    //                boolean isCurrency = data.isCurrency(i);//在数据库中是否为货币型

                    //                int isNullable = data.isNullable(i);//是否为空

                    //                boolean isReadOnly = data.isReadOnly(i);//是否为只读

                    //                boolean isSearchable = data.isSearchable(i);//能否出现在where中


                    //                System.out.println("获得列" + i + "的字段名称:" + columnName);
                    //                System.out.println("获得列" + i + "的字段值:" + columnValue);
                    //                System.out.println("获得列" + i + "的类型,返回SqlType中的编号:" + columnType);
                    //                System.out.println("获得列" + i + "的数据类型名:" + columnTypeName);
                    //                System.out.println("获得列" + i + "所在的Catalog名字:" + catalogName);
                    //                System.out.println("获得列" + i + "对应数据类型的类:" + columnClassName);
                    //                System.out.println("获得列" + i + "在数据库中类型的最大字符个数:" + columnDisplaySize);
                    //                System.out.println("获得列" + i + "的默认的列的标题:" + columnLabel);
                    //                System.out.println("获得列" + i + "的模式:" + schemaName);
                    //                System.out.println("获得列" + i + "类型的精确度(类型的长度):" + precision);
                    //                System.out.println("获得列" + i + "小数点后的位数:" + scale);
                    //                System.out.println("获得列" + i + "对应的表名:" + tableNamea);
                    //                System.out.println("获得列" + i + "是否自动递增:" + isAutoInctement);
                    //                System.out.println("获得列" + i + "在数据库中是否为货币型:" + isCurrency);
                    //                System.out.println("获得列" + i + "是否为空:" + isNullable);
                    //                System.out.println("获得列" + i + "是否为只读:" + isReadOnly);
                    //                System.out.println("获得列" + i + "能否出现在where中:" + isSearchable);
                    //                }
                    //                System.out.println();
                    map.put("columnName", columnName);
                    tableStructures.add(map);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                release(resultSet, pstmt,connection);
            }
        return tableStructures;
    }

    public static void main(String[] a) throws Exception {

//        String url = "jdbc:mysql://192.168.2.114:8081/jd_iot_data_test";
//        Properties properties = new Properties();
//        properties.setProperty(DruidDataSourceProperties.PROP_URL, url);
//        properties.setProperty(DruidDataSourceProperties.PROP_USERNAME, "root");
//        properties.setProperty(DruidDataSourceProperties.PROP_PASSWORD, "root");
//        properties.setProperty(DruidDataSourceProperties.PROP_DRIVERCLASSNAME, "com.mysql.jdbc.Driver");
//        properties.setProperty("maxActive", String.valueOf(10));
//        properties.setProperty(DruidDataSourceProperties.PROP_MINIDLE, String.valueOf(0));
//        properties.setProperty(DruidDataSourceProperties.PROP_INITIALSIZE, String.valueOf(10));
//        properties.setProperty(DruidDataSourceProperties.NUPROP_MAXWAIT, String.valueOf(2000));
//        properties.setProperty("phyTimeoutMillis", String.valueOf(2000));
//        properties.setProperty("validationQuery", "SELECT 1");
//        properties.setProperty("validationQueryTimeout", String.valueOf(2));
//        properties.setProperty(DruidDataSourceProperties.PROP_CONNECTIONPROPERTIES, "connectTimeout=3000;socketTimeout=3000;useUnicode=true;characterEncoding=UTF8");
//
//        MysqlConnect connect = new MysqlConnect(properties);
//        List<Map<String, Object>> list =  connect.getTableStructure("flink_test");
//        System.out.println(list);
    }
}
