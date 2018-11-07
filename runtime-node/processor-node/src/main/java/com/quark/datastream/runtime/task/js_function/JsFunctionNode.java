package com.quark.datastream.runtime.task.js_function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonParser;
import com.quark.datastream.runtime.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.*;

/**
 * js function process node
 */
public class JsFunctionNode extends AbstractTaskNode {


    private static final Logger LOGGER = LoggerFactory.getLogger(JsFunctionNode.class);

    private static final String FUNCTION_NAME = "func";
    private static final String JSFUNCTION = "function";
    private static final String TASK_NAME_JSFUNCTION = "js function";

    @TaskParam(uiName = "js function value", uiType = TaskParam.UiFieldType.STRING, key = "function")
    private String functionBody;

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
        LOGGER.info("js function node in:{}",in);
        List<DataSet.Record> records = in.getRecords();
        List<DataSet.Record> resultRes = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            DataSet.Record record = records.get(i);
            ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine engine = manager.getEngineByName("javascript");
            String scriptText = "function "+FUNCTION_NAME+"(data){ "+ functionBody + "}";
            try {
                engine.eval(scriptText);
            } catch (ScriptException e) {
                e.printStackTrace();
            }
            Invocable invokeEngine = (Invocable)engine;
            Object jsFunctionRes = null;
            try {
                jsFunctionRes =  invokeEngine.invokeFunction(FUNCTION_NAME, record);
                JsonParser jsonParser = new JsonParser();
                if (!jsonParser.parse(jsFunctionRes.toString()).isJsonObject()){
                    throw new IllegalStateException("Not a JSON Object: " + jsFunctionRes.toString());
                }
                ObjectMapper objectMapper = new ObjectMapper();
                DataSet.Record resMap = objectMapper.readValue(jsFunctionRes.toString(),DataSet.Record.class);

                resultRes.add(resMap);

            } catch (ScriptException | NoSuchMethodException | IOException e) {
                e.printStackTrace();
            }
            in.setValue("/records", resultRes);
        }
        LOGGER.info("js function node out:{}",in);
        return in;
    }

    /**
     * 任务类型
     */
    @Override
    public TaskType getType() {
        return TaskType.JSFUNCTION;
    }

    /**
     * 任务名称
     *
     * @return
     */
    @Override
    public String getName() {
        return TASK_NAME_JSFUNCTION;
    }

    /**
     * 设置任务节点参数
     *
     * @param param
     */
    @Override
    public void setParam(TaskNodeParam param) {
        if(param.containsKey(JSFUNCTION)){
            functionBody = (String) param.get(JSFUNCTION);
        }
    }

    public static void main(String[] args) throws ScriptException, NoSuchMethodException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("javascript");
        String recode = "{\"data\":{\"bjzt\":false,\"chsll\":5.5,\"chswd\":77.84,\"chsyl\":1.1,\"device_id\":143,\"mbwd\":91.71,\"pywd\":152.2,\"qlwd\":68.96,\"rszt\":true,\"tlwd\":61.71,\"zdzt\":true}}";
        String s = "function reverse(data) { var json = JSON.parse(data); var s={}; s['chswd']=json.data.chswd;s['deviceid']=json.data.device_id; var ss = JSON.stringify(s);return ss;}";
        engine.eval(s);
        Invocable invokeEngine = (Invocable)engine;
        Object o = invokeEngine.invokeFunction("reverse", recode);
        DataSet dataSet = DataSet.create(String.valueOf(o));
        System.out.print(dataSet.toString());
    }
}
