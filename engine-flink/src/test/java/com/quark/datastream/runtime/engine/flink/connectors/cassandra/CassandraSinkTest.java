package com.quark.datastream.runtime.engine.flink.connectors.cassandra;

import com.google.gson.Gson;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class CassandraSinkTest {

    @Test(timeout = 20000L)
//    @Test
    public void runTest() throws Exception {

        Map dbConfig = new HashMap();
        dbConfig.put("ip", "192.168.2.106");
        dbConfig.put("port", "9042");
        dbConfig.put("username","cassandra");
        dbConfig.put("password", "cassandra");
        DataSet dataSet = DataSet.create();
        dataSet.addRecord("{\"id\":2,\"name\":\"oppo\",\"phone\":\"123345\"}");
        dbConfig.put("table", "test");
        dbConfig.put("database","key");

        CassandraSink cassandraSink = new CassandraSink(dbConfig);
        cassandraSink.open(null);
        cassandraSink.invoke(dataSet);
        cassandraSink.close();
    }
}
