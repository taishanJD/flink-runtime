package com.quark.datastream.runtime.task.ml_svm;

import com.quark.datastream.runtime.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * machine learning SVM scale node
 */
public class ScaleNode extends AbstractTaskNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScaleNode.class);

    private static final String TASK_NAME_SCALE = "scale";

    private double[] feature_max;  //feature最大值(特征向量的各个分量在训练数据或测试数据中的范围)
    private double[] feature_min;  //feature最小值
    private double label_max = -Double.MAX_VALUE; //label
    private double label_min = Double.MAX_VALUE;
    private boolean y_scaling = false; //是否对label缩放,默认不对label缩放

    @TaskParam(key = "inputKeys", uiName = "input keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "enter input keys")
    private List<String> inputKeys;

    @TaskParam(key = "resultKeys", uiName = "result keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "enter result keys")
    private List<String> resultKeys;

    @TaskParam(key = "lower", uiName = "Lower Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter lower value")
    protected Double lower = -1.0; // svm_scale.feature.lower

    @TaskParam(key = "upper", uiName = "Upper Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter upper value")
    protected Double upper = 1.0; // svm_scale.feature.upper

    @TaskParam(key = "y_lower", uiName = "Y_Lower Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter y_lower value")
    protected Double y_lower; // svm_scale.label.lower

    @TaskParam(key = "y_upper", uiName = "Y_Upper Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter y_upper value")
    protected Double y_upper; // svm_scale.label.upper

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
        LOGGER.info("[SVM] String calculation , in= {}");
        LOGGER.info("[SVM] String calculation , in= {}", in.toString());
        // get data
        List<List<Number>> vectors = new ArrayList<>();
        for (int index = 0; index < inputKeys.size(); index++) {
            List<Number> values = in.getValue("/records" + inputKeys.get(index), List.class);
            LOGGER.info("[SVM] records values = {}", values);
            vectors.add(values);
        }
        // 1.max index of feature
        int max_index = 0;

        LOGGER.info("[SVM] vectors= {}", vectors);
        if (vectors.size() == inputKeys.size()) {
            max_index = vectors.size() - 1;
            //将每列的最大值初始化为一个double一维数组
            try {
                feature_max = new double[(max_index + 1)];
                feature_min = new double[(max_index + 1)];
            } catch (OutOfMemoryError e) {
                System.err.println("can't allocate enough memory");
                System.exit(1);
            }
            // 初始化每一列的最大最小值
            for (int i = 0; i <= max_index; i++) {
                feature_max[i] = -Double.MAX_VALUE;
                feature_min[i] = Double.MAX_VALUE;
            }
            // 2.find out min/max value
            // 第几行
            for (int loop = 0; loop < vectors.get(0).size(); loop++) {
                // 第几列
                int next_index = 1;
                double value;
                double target;

                target = vectors.get(0).get(loop).doubleValue();
                label_max = Math.max(label_max, target);
                label_min = Math.min(label_min, target);

                for (int index = 1; index < vectors.size(); index++) {
                    value = vectors.get(index).get(loop).doubleValue();
                    for (int i = next_index; i < index; i++) {
                        feature_max[i] = Math.max(feature_max[i], 0);
                        feature_min[i] = Math.min(feature_min[i], 0);
                    }
                    feature_max[index] = Math.max(feature_max[index], value);
                    feature_min[index] = Math.min(feature_min[index], value);
                    next_index = index + 1;
                }

                for (int i = next_index; i <= max_index; i++) {
                    feature_max[i] = Math.max(feature_max[i], 0);
                    feature_min[i] = Math.min(feature_min[i], 0);
                }
            }
            // 3.scale
            // 第几列
            for (int index = 0; index < vectors.size(); index++) {

                int next_index = 1;
                double value;
                double target;
                //第几行
                for (int x = index; x < resultKeys.size(); x++) {
                    LOGGER.info("[SVM] resultKeys.get(x)==== {}",resultKeys.get(x));
                    LOGGER.info("[SVM] resultKeys.size()==== {}",resultKeys.size());
                    List<Double> values =  new ArrayList<>();
                    for (int loop = 0; loop < vectors.get(0).size(); loop++) {
                        if (index == 0) {
                            target = vectors.get(index).get(loop).doubleValue();
                            // scale label
                            double value_scale = output_target(target);
                            values.add(value_scale);
                            continue;
                        }
                        value = vectors.get(index).get(loop).doubleValue();

                        for (int i = next_index; i < index; i++)
                            output(i, 0);
                        // scale feature
                        if (feature_max[index] == feature_min[index]) {
                            values.add(0.0);
                        } else {
                            double feature_scale = output(index, value);
                            values.add(feature_scale);
                        }
                        next_index = index + 1;
                    }
                    LOGGER.info("[SVM] values.size() ==== {}",values.size());
                    if(values.size() != 0){
                        in.setValue("/records" + resultKeys.get(x),values);
                    }
                    break;
                }

                for (int i = next_index; i <= max_index; i++)
                    output(i, 0);
            }
        }
        return in;
    }

    /**
     * 重点做scale处理
     *
     * @param index
     * @param value
     */
    private double output(int index, double value) {

        if(value == feature_min[index]) {
            value = lower;
            LOGGER.info("[SVM] lower= {}", lower);
        }
        else if(value == feature_max[index]){
            value = upper;
            LOGGER.info("[SVM] upper= {}", upper);
        }
        else
            value = lower + (upper-lower) *
                    (value-feature_min[index])/
                    (feature_max[index]-feature_min[index]);

            return value;
    }

    private double output_target(double value) {
        if(y_scaling) {
            if(value == label_min)
                value = y_lower;
            else if(value == label_max)
                value = y_upper;
            else
                value = y_lower + (y_upper-y_lower) *
                        (value-label_min) / (label_max-label_min);
        }
        return value;
    }

    /**
     * 任务类型
     *
     * @return
     */
    @Override
    public TaskType getType() {
        return TaskType.SCALE;
    }

    /**
     * 任务名称
     *
     * @return
     */
    @Override
    public String getName() {
        return TASK_NAME_SCALE;
    }

    /**
     * 设置任务节点参数
     *
     * @param params
     */
    @Override
    public void setParam(TaskNodeParam params) {
        LOGGER.info("[SVM] Entering setParam Method");
        try {
            if (params.containsKey("inputKeys")) {
                inputKeys = new ArrayList<>();
                String[] keys = params.get("inputKeys").toString().split(",");
                for (String key : keys) {
                    String[] tokens = key.split("\\.");
                    StringBuffer sb = new StringBuffer();
                    for (String token : tokens) {
                        sb.append("/" + token);
                    }
                    inputKeys.add(sb.toString());
                }
            }
            if (params.containsKey("resultKey")) {
                resultKeys = new ArrayList<>();
                String[] keys = params.get("resultKey").toString().split(",");
                for (String key : keys) {
                    String[] tokens = key.split("\\.");
                    StringBuffer sb = new StringBuffer();
                    for (String token : tokens) {
                        sb.append("/" + token);
                    }
                    resultKeys.add(sb.toString());
                }
                LOGGER.info("[SVM] resultKeys.size()==== {}",resultKeys.size());
//                resultKey = params.get("resultKey").toString();
            }
            if(params.containsKey("lower")){
                lower = Double.parseDouble(params.get("lower").toString());
            }
            if(params.containsKey("upper")){
                upper = Double.parseDouble(params.get("upper").toString());
            }
            if(params.containsKey("y_lower")){
                y_lower = Double.parseDouble(params.get("y_lower").toString());
            }
            if(params.containsKey("y_upper")){
                y_upper = Double.parseDouble(params.get("y_upper").toString());
            }

        } catch (IllegalArgumentException e) {
            LOGGER.error("[SVM] " + e.toString());
        } catch (NullPointerException e) {
            LOGGER.error("[SVM] " + e.toString());
        }

        LOGGER.info("[SVM] Leaving setParam Method");
    }
}
