package com.quark.datastream.runtime.engine.flink.connectors.mysql;

import com.quark.datastream.runtime.task.DataSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MysqlSink.class})
public class MysqlSinkTest {

    @Test
    public void testOpen() throws Exception {
//        MysqlSink sink = new MysqlSink("localhost", 3306, "oneiot_tenant", "root", "root", "role");
//        sink.open(null);
//        DataSet dataSet = DataSet.create();
//        dataSet.addRecord("{\"role_code\":1,\"role_name\":\"name\",\"description\":\"test\"}");
//        sink.invoke(dataSet);
//        sink.close();
    }
}
