package com.quark.datastream.runtime.engine.flink.connectors.hive.common;

import com.alibaba.druid.pool.DruidDataSource;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBConnect;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBOperate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

public class HiveConnect extends DBConnect implements DBOperate {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    /**
     * Hive operation JdbcTemplate
     */
    private JdbcTemplate hiveJdbcTemplate;
    /**
     * Hive metadata operation JdbcTemplate
     */
    private JdbcTemplate metadataJdbcTemplate;
    /**
     * Hive JDBC driver
     */
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    /**
     * Hive metadata JDBC driver
     */
    private static final String METADATA_MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    /**
     * Metadata datasource
     */
    private DruidDataSource metadataDataSource = null;
    /**
     * The hive database currently in use
     */
    private String currentDataBase;


    public HiveConnect(Map hiveConfig) {
        // Init hive connection
        initHiveJdbcTemplate(hiveConfig);

        // Init hive metadata connection
        initMetadataJdbcTemplate(hiveConfig);
    }

    public JdbcTemplate getHiveJdbcTemplate() {
        return hiveJdbcTemplate;
    }

    @Override
    public List<Map<String, Object>> getTableStructure(String tableName) {
        LOGGER.info("[HIVE OPERATE INFO] ======>  Get table structure, table name : {}", tableName);

        StringBuilder sql = new StringBuilder()
                .append("SELECT COLUMN_NAME, TYPE_NAME FROM COLUMNS_V2 WHERE CD_ID = (SELECT CD_ID FROM SDS WHERE SD_ID = ")
                .append("(SELECT SD_ID FROM TBLS WHERE TBL_NAME = '").append(tableName).append("' ")
                .append("AND DB_ID = (SELECT DB_ID FROM DBS WHERE NAME = '").append(currentDataBase).append("')))");

        return  metadataJdbcTemplate.queryForList(sql.toString());
    }

    @Override
    public void close() {
        super.close();
        if (null != metadataDataSource) {
            metadataDataSource.close();
        }
    }

    private void initHiveJdbcTemplate(Map hiveProperties) {
        // Hive connection information
        String ip = (String) hiveProperties.get(HiveConfig.HIVE_IP);
        Integer port = Integer.parseInt((String) hiveProperties.get(HiveConfig.HIVE_PORT));
        String database = (String) hiveProperties.get(HiveConfig.HIVE_DATABASE);
        String url = "jdbc:hive2://" + ip + ":" + port + "/" + database;
        String username = (String) hiveProperties.get(HiveConfig.HIVE_USERNAME);
        String password = (String) hiveProperties.get(HiveConfig.HIVE_PASSWORD);

        // Hive datasource
        DruidDataSource hiveDataSource = new DruidDataSource();
        hiveDataSource.setUrl(url);
        hiveDataSource.setDriverClassName(HIVE_DRIVER);
        hiveDataSource.setUsername(username);
        hiveDataSource.setPassword(password);
        hiveDataSource.setTestWhileIdle(true);
        hiveDataSource.setValidationQuery("SELECT 1");
        hiveDataSource.setMaxActive(5);
        hiveDataSource.setInitialSize(3);
        hiveDataSource.setRemoveAbandoned(true);
        hiveDataSource.setRemoveAbandonedTimeout(180);

        currentDataBase = database;
        super.dataSource = hiveDataSource;
        hiveJdbcTemplate = new JdbcTemplate(super.dataSource);
    }

    private void initMetadataJdbcTemplate(Map metadataProperties) {
        // Hive metadata MySQL connection information
        String metadataIp = (String) metadataProperties.get((HiveConfig.HIVE_METADATA_IP));
        Integer metadataPort = Integer.parseInt((String) metadataProperties.get(HiveConfig.HIVE_METADATA_PORT));
        String metadataDatabase = (String) metadataProperties.get(HiveConfig.HIVE_METADATA_DATABASE);
        String metadataUrl ="jdbc:mysql://" + metadataIp + ":" + metadataPort + "/" + metadataDatabase;
        String metadataUsername = (String) metadataProperties.get(HiveConfig.HIVE_METADATA_USERNAME);
        String metadataPassword = (String) metadataProperties.get(HiveConfig.HIVE_METADATA_PASSWORD);

        Properties properties = new Properties();
        properties.setProperty("connectTimeout", "10000");
        properties.setProperty("socketTimeout", "5000");
        properties.setProperty("useUnicode", "true");
        properties.setProperty("characterEncoding", "UTF8");

        // Hive metadata datasource
        DruidDataSource metadataDataSource = new DruidDataSource();
        metadataDataSource.setUrl(metadataUrl);
        metadataDataSource.setUsername(metadataUsername);
        metadataDataSource.setPassword(metadataPassword);
        metadataDataSource.setDriverClassName(METADATA_MYSQL_DRIVER);
        metadataDataSource.setMaxActive(5);
        metadataDataSource.setMinIdle(0);
        metadataDataSource.setInitialSize(1);
        metadataDataSource.setMaxWait(10000);
        metadataDataSource.setConnectProperties(properties);

        this.metadataDataSource = metadataDataSource;
        metadataJdbcTemplate = new JdbcTemplate(this.metadataDataSource);
    }
}
