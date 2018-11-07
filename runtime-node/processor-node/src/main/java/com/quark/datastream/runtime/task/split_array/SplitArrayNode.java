package com.quark.datastream.runtime.task.split_array;

import com.google.gson.Gson;
import com.quark.datastream.runtime.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitArrayNode extends AbstractTaskNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(SplitArrayNode.class);

    private static final String SPLITKET = "splitKey";
    private static final String TASK_NAME = "split_array";

    @TaskParam(uiName = "split array value", uiType = TaskParam.UiFieldType.STRING, key = "split_key")
    private String splitKey;

    /**
     * 设置任务节点参数
     *
     * @param param
     */
    @Override
    public void setParam(TaskNodeParam param) {
        if (param.containsKey(SPLITKET)) {
            splitKey = (String) param.get(SPLITKET);
        }
    }

    /**
     * 任务类型
     */
    @Override
    public TaskType getType() {
        return TaskType.SPLITARRAY;
    }

    /**
     * 任务名称
     *
     * @return
     */
    @Override
    public String getName() {
        return TASK_NAME;
    }

    /**
     * 节点计算逻辑
     *
     * @param in            输入数据
     * @param inRecordKeys  输入记录key
     * @param outRecordKeys 输出记录key
     * @return
     */
    @Override
    public DataSet calculate(DataSet in, List<String> inRecordKeys, List<String> outRecordKeys) {
        LOGGER.info("split node in:{}", in);
        StringBuffer sb = new StringBuffer();
        StringBuffer keyPrefix = new StringBuffer();
        StringBuffer keySuffix = new StringBuffer();
        String[] splitKeys = splitKey.split("\\.");
        for (String key : splitKeys) {
            sb.append("/" + key);
            keyPrefix.append("{\"" + key + "\":");
            keySuffix.append("}");
        }
        List<Object> data = in.getValue("/records" + sb, List.class);
        List<Map<String, Object>> records = (List<Map<String, Object>>) data.get(0); //找出待拆分的数组
        DataSet dataSet = DataSet.create();
        Gson gson = new Gson();
        for (Map<String, Object> record : records) {
            DataSet.Record record1 = DataSet.Record.create(keyPrefix.toString()+gson.toJson(record)+keySuffix.toString());
            dataSet.addRecord(record1);
        }
        LOGGER.info("split node out:{}", dataSet.toString());
        return dataSet;
    }

    /**
     * split_array_node测试
     *
     * @param args
     */
    public static void main(String[] args) {
        List<String> listValue = new ArrayList<>();
        listValue.add("/records");

        Map<String, Object> key22 = new HashMap<>();
        key22.put("data", "test2");

        Map<String, Object> key11 = new HashMap<>();
        key11.put("msg", "test1");

        Map<String, Object> key00 = new HashMap<>();
        key00.put("code", "test0");

        List<Map<String, Object>> maps = new ArrayList<>();
        maps.add(key00);
        maps.add(key11);
        maps.add(key22);

        Map<String, Object> key2 = new HashMap<>();
        key2.put("data", maps);

        Map<String, Object> key1 = new HashMap<>();
        key1.put("msg", key2);

        Map<String, Object> key0 = new HashMap<>();
        key0.put("code", key1);

        String aaa = "code.msg.data";
        String[] splitKeys = aaa.split("\\.");

        Map<String, Object> value0 = key0;

        for (int i = 0; i < splitKeys.length - 1; i++) {
            value0 = (Map<String, Object>) value0.get(splitKeys[i]);
        }

        List<Map<String, Object>> values = (List<Map<String, Object>>) value0.get(splitKeys[splitKeys.length - 1]);
        for (Map<String, Object> value : values) {
            System.out.println(value);
        }

        DataSet dataSet = DataSet.create();
        List<DataSet.Record> records = new ArrayList<>();
        Gson gson = new Gson();
        DataSet.Record record = DataSet.Record.create(gson.toJson(key0));
        records.add(record);
        dataSet.setValue("/records", records);

        Map<String, Object> param = new HashMap<>();
        param.put("split_key", aaa);
        TaskNodeParam taskNodeParam = TaskNodeParam.create(gson.toJson(param));
        SplitArrayNode splitArrayNode = new SplitArrayNode();
        splitArrayNode.setParam(taskNodeParam);
        DataSet outDataset = splitArrayNode.calculate(dataSet, listValue, listValue);
        System.out.println(outDataset);
    }
}
