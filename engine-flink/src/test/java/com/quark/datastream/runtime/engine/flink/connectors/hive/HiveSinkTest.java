package com.quark.datastream.runtime.engine.flink.connectors.hive;

import com.quark.datastream.runtime.task.DataSet;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * 建表语句：
 *  CREATE TABLE sink (studentid INT, studentname STRING, description STRING)
 *  ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
 *  STORED AS TEXTFILE;
 *
 */
public class HiveSinkTest {
    @Test(timeout = 60000L)
    public void runTest() throws Exception {

        Map<String, String> hiveConfig = new HashMap<>();
        hiveConfig.put("ip", "192.168.2.28");
        hiveConfig.put("port", "10000");
        hiveConfig.put("username","hadoop");
        hiveConfig.put("password", "hadoop");
        hiveConfig.put("database", "test_db");
        hiveConfig.put("table", "sink");

        hiveConfig.put("metadataIp", "192.168.2.28");
        hiveConfig.put("metadataPort", "3306");
        hiveConfig.put("metadataDatabase", "hive");
        hiveConfig.put("metadataUsername", "root");
        hiveConfig.put("metadataPassword", "QPsk29cn^0*");

        DataSet dataSet = DataSet.create();
        dataSet.addRecord("{\"studentid\":1,\"studentname\":\"Bob\",\"description\":\"asdfasddddddddff\"}");
        dataSet.addRecord("{\"studentid\":2,\"studentname\":\"John\",\"description\":\"asdfasadfddff\"}");
        dataSet.addRecord("{\"studentid\":3,\"studentname\":\"Beer\",\"description\":\"asdfasadfdsdfsddff\"}");
        dataSet.addRecord("{\"studentid\":4,\"studentname\":\"Alice\",\"description\":\"asdfasdsdfsddff\"}");
        dataSet.addRecord("{\"studentid\":5,\"studentname\":\"Mike\",\"description\":\"asdfasadfdsdff\"}");
        dataSet.addRecord("{\"studentid\":6,\"studentname\":\"Nike\",\"description\":\"asdfasadfdsdfs\"}");

        HiveSink hiveSink = new HiveSink(hiveConfig);
        hiveSink.open(null);
        hiveSink.invoke(dataSet);
        hiveSink.close();
    }
}
