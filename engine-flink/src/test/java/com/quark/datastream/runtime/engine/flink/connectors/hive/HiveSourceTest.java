package com.quark.datastream.runtime.engine.flink.connectors.hive;

import com.google.gson.Gson;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 *  建表语句：
 *  CREATE TABLE source (studentid INT, sutdentname STRING, description STRING)
 *  ROW FORMAT DELIMITED
 *  FIELDS TERMINATED BY '\t'
 *  LINES TERMINATED BY '\n'
 *  STORED AS textfile;
 *
 */
public class HiveSourceTest {

    @Test(timeout = 60000L)
    public void runTest() throws Exception {

        Map<String, String> hiveConfig = new HashMap<>();
        hiveConfig.put("ip", "192.168.2.28");
        hiveConfig.put("port", "10000");
        hiveConfig.put("username", "hadoop");
        hiveConfig.put("password", "hadoop");
        hiveConfig.put("database", "test_db");

        hiveConfig.put("metadataIp", "192.168.2.28");
        hiveConfig.put("metadataPort", "3306");
        hiveConfig.put("metadataDatabase", "hive");
        hiveConfig.put("metadataUsername", "root");
        hiveConfig.put("metadataPassword", "QPsk29cn^0*");

        Gson gson = new Gson();
        Map<String, Integer> periodMap = new HashMap<>();
        periodMap.put("hour", 0);
        periodMap.put("min", 0);
        periodMap.put("sec", 5);
        String cron = gson.toJson(periodMap);
        hiveConfig.put("period", cron);
        hiveConfig.put("sql", "select * from source");

        HiveSource hiveSource = new HiveSource(hiveConfig);
        hiveSource.open(null);
        SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
        hiveSource.run(sourceContext);
        hiveSource.cancel();
    }

}
