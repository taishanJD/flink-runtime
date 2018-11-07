package com.quark.datastream.runtime.task.ml_svm;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.quark.datastream.runtime.task.DataSet;
import com.quark.datastream.runtime.task.TaskNodeParam;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FuzzyKMeansNodeTest {

    @Test
    public void FuzzyKMeansNodeTest() {
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("inputKeys", "test1,test2,test3,test4");
        paramMap.put("resultKey", "result");
        paramMap.put("k",3);
        paramMap.put("fuzziness", 2.0F);
        paramMap.put("convergenceDelta", 0.3D);
        paramMap.put("distanceMeasure", 0);
        paramMap.put("maxIterations", 10);
        Gson gson = new Gson();
        String paramStr = gson.toJson(paramMap);
        TaskNodeParam param = TaskNodeParam.create(paramStr);

        FuzzyKMeansNode fuzzyKMeansNode = new FuzzyKMeansNode();
        fuzzyKMeansNode.setParam(param);

        JsonObject data1 = makeData(5.1, 3.5, 1.4, 0.2);
        JsonObject data2 = makeData(5.4, 3.4, 1.7, 0.2);
        JsonObject data3 = makeData(4.8, 3.1, 1.6, 0.2);
        JsonObject data4 = makeData(5.0, 3.5, 1.3, 0.3);
        JsonObject data5 = makeData(7.0, 3.2, 4.7, 1.4);
        JsonObject data6 = makeData(5.0, 2.0, 3.5, 1.0);

        DataSet in = DataSet.create();
        in.addRecord(data1.toString());
        in.addRecord(data2.toString());
        in.addRecord(data3.toString());
        in.addRecord(data4.toString());
        in.addRecord(data5.toString());
        in.addRecord(data6.toString());

        System.out.println(in.toString());
        fuzzyKMeansNode.calculate(in, new ArrayList<>(), new ArrayList<>());
        DataSet dataSet = fuzzyKMeansNode.calculate(in, new ArrayList<>(), new ArrayList<>());
        List<String> value = dataSet.getValue("/records/result", List.class);
        System.out.println("result:");
        System.out.println(value);
        Assert.assertTrue(null != value.get(0));
    }

    private JsonObject makeData(double test1, double test2, double test3, double test4) {
        JsonObject sampleJson = new JsonObject();
        sampleJson.addProperty("test1", test1);
        sampleJson.addProperty("test2", test2);
        sampleJson.addProperty("test3", test3);
        sampleJson.addProperty("test4", test4);
        return sampleJson;
    }
}


