package com.quark.datastream.runtime.engine.flink.connectors.commom.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBConnect {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBConnect.class);


    protected DruidDataSource dataSource = null;

    public DBConnect() {
    }

    DBConnect(Properties properties) throws Exception {
        dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
    }

    public Connection getConnection() throws SQLException {
        Connection con = null;
        if (null != dataSource){
            con = dataSource.getConnection();
        }
        return con;
    }

    public void release(ResultSet resultSet, Statement statement, Connection connection) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (statement != null) {
            try {
                //关闭负责执行SQL命令的Statement对象
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                //将Connection连接对象还给数据库连接池
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        if (null != dataSource){
            dataSource.close();
        }
    }
}
