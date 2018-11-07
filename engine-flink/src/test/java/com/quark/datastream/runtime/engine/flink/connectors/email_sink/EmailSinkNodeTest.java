package com.quark.datastream.runtime.engine.flink.connectors.email_sink;

import com.quark.datastream.runtime.task.DataSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class EmailSinkNodeTest {

    @Test
    public void testEmailSinkInvoke() throws Exception {
        DataSet dataSet = DataSet.create();
        List<DataSet.Record> records = new ArrayList<>();
        DataSet.Record record = DataSet.Record.create("{\"nodeName\":\"SplitArray\",\"splitKey\":\"aaa\"}");
        records.add(record);
        dataSet.setValue("/records",records);

        String recipients = "jiadao0703@thundersoft.com, luohl0703@thundersoft.com";
        String custom = "";

        EmailSinkNode emailSinkNode = new EmailSinkNode("mail.thundersoft.com", "995",
                "luohl0703@thundersoft.com", "908breakingHH", recipients, custom, "From flink","key1.key2");
        emailSinkNode.invoke(dataSet);
    }

}
