package com.quark.datastream.runtime.engine.flink.connectors.cassandra;

import com.google.gson.Gson;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class CassandraSourceTest {

    @Test(timeout = 20000L)
    public void runTest() throws Exception {

        Map dbConfig = new HashMap();
        dbConfig.put("ip", "192.168.2.106");
        dbConfig.put("port", "9042");
        dbConfig.put("username","cassandra");
        dbConfig.put("password", "cassandra");
        Gson gson = new Gson();
        Map<String, Integer> periodMap = new HashMap<>();
        periodMap.put("hour", 0);
        periodMap.put("min", 0);
        periodMap.put("sec", 3);
        String cron = gson.toJson(periodMap);
        dbConfig.put("period", cron);
        dbConfig.put("database","key");
        dbConfig.put("sql","select * from test where updatetime < '2018-10-10' allow filtering");
        CassandraSource cassandraSource = new CassandraSource(dbConfig);
        cassandraSource.open(null);
        SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
        cassandraSource.run(sourceContext);
        cassandraSource.cancel();
    }
}
