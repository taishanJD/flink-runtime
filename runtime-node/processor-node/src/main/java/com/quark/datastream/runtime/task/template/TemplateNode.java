package com.quark.datastream.runtime.task.template;

import com.quark.datastream.runtime.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;

/**
 * template processor node
 */
public class TemplateNode extends AbstractTaskNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateNode.class);

    private static final String TEMPLATE = "function";
    private static final String TEMPLATEKEY = "contentKey";
    private static final String TASK_NAME_TEMPLATE = "template";

    @TaskParam(uiName = "template value", uiType = TaskParam.UiFieldType.STRING, key = "template")
    private String template;

    @TaskParam(uiName = "template content key", uiType = TaskParam.UiFieldType.STRING, key = "templateKey")
    private String templateKey;

    /**
     * 设置任务节点参数
     *
     * @param param
     */
    @Override
    public void setParam(TaskNodeParam param) {
        if (param.containsKey(TEMPLATE)) {
            template = (String) param.get(TEMPLATE);
        }
        if (param.containsKey(TEMPLATEKEY)) {
            templateKey = (String) param.get(TEMPLATEKEY);
        }
    }

    /**
     * 任务类型
     */
    @Override
    public TaskType getType() {
        return TaskType.TEMPLATE;
    }

    /**
     * 任务名称
     *
     * @return
     */
    @Override
    public String getName() {
        return TASK_NAME_TEMPLATE;
    }

    /**
     * 节点计算逻辑
     *
     * @param in 输入数据
     * @return
     */
    @Override
    public DataSet calculate(DataSet in, List<String> inRecordKeys, List<String> outRecordKeys) {
        LOGGER.info("template in: "+in.toString());
        List<DataSet.Record> records = in.getRecords();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            ST st = new ST(template);
            DataSet.Record record = records.get(i);
            for (String key : record.keySet()) {
                st.add(key, record.get(key));
            }
            String templateRes = st.render();
            values.add(templateRes);
        }
        String[] tokens = templateKey.split("\\.");
        StringBuffer sb = new StringBuffer();
        for (String token : tokens) {
            sb.append("/" + token);
        }
        in.setValue("/records" + sb, values);
        LOGGER.info("template out:{}",in);
        return in;
    }

    public static void main(String[] a){
        String json1 = "{\"bjzt\":false,\"chsll\":5.47,\"chswd\":81.03,\"chsyl\":1.26,\"device_id\":141,\"mbwd\":89.53,\"pywd\":159.73,\"qlwd\":63.08,\"rszt\":true,\"tlwd\":62.08,\"zdzt\":true}";
        String json2 = "{\"bjzt\":true,\"chsll\":5.47,\"chswd\":81.03,\"chsyl\":1.26,\"device_id\":141,\"mbwd\":89.53,\"pywd\":159.73,\"qlwd\":63.08,\"rszt\":true,\"tlwd\":62.08,\"zdzt\":true}";
        DataSet dataSet = DataSet.create(json1);
        dataSet.addRecord(json2);
        System.out.println("dataset == "+dataSet.toString());
        List<String> values = new ArrayList<>();
        List<DataSet.Record> records = dataSet.getRecords();
        for (int i = 0; i < records.size(); i++) {
             DataSet.Record record = records.get(i);
             System.out.println("record == "+record.toString());

            String content = "回水温度：<chswd> 高于报警温度,设备id:<device_id>";
            ST st = new ST(content);
            for (String key:record.keySet()) {
                st.add(key,record.get(key));
            }
            String result = st.render();
            values.add(result);
        }
        dataSet.setValue("/records/chswd", values);
        System.out.println(dataSet.toString());
    }

}
