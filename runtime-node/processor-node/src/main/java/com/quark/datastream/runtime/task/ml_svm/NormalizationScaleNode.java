package com.quark.datastream.runtime.task.ml_svm;

import com.quark.datastream.runtime.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Normalization
 */
public class NormalizationScaleNode extends AbstractTaskNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationScaleNode.class);

    private static final String TASK_NAME_SCALE = "normalization";

    private static double[] feature_p; //特征范数

    @TaskParam(key = "inputKeys", uiName = "input keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "enter input keys")
    private List<String> inputKeys;

    @TaskParam(key = "resultKeys", uiName = "result keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "enter result keys")
    private List<String> resultKeys;

    @TaskParam(key = "p", uiName = "P Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter p value")
    protected int p = 2; // svm_scale.feature.p



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
            max_index = vectors.get(0).size() - 1;
            // 按行处理
            try {
                feature_p = new double[(max_index + 1)];
            } catch (OutOfMemoryError e) {
                System.err.println("can't allocate enough memory");
                System.exit(1);
            }

            // 2.find out Normalization value
            // 每行
            for (int i = 0; i < vectors.get(0).size(); i++) {
                double sum = 0;
                // 每列
                for (int loop = 0; loop < vectors.size(); loop++) {
                    double value = vectors.get(loop).get(i).doubleValue();
                    sum += getPow(value);
                }
                double normal = openPow(sum);
                feature_p[i] = normal;
            }

            // 存储到一个集合中
            List<List<Double>> results = new ArrayList<>();

            // normalization
            for (int loop = 0; loop < vectors.get(0).size(); loop++) {

                double value;

                List<Double> result = new ArrayList<>();
                // 第几行

                for (int index = 0; index < vectors.size(); index++) {
                    value = vectors.get(index).get(loop).doubleValue();

                    double feature_normal = normal(loop, value);
                    result.add(feature_normal);
                }
                results.add(result);
            }

            for (int loop = 0; loop < results.size(); loop++) {

                double resultValue;
                for (int x = loop; x < resultKeys.size(); x++) {
                    LOGGER.info("[SVM] resultKeys.get(x)==== {}", resultKeys.get(x));
                    LOGGER.info("[SVM] resultKeys.size()==== {}", resultKeys.size());
                    List<Double> resultValues = new ArrayList<>();

                    for (int i = 0; i < results.get(0).size(); i++) {
                        resultValue = results.get(i).get(loop).doubleValue();
                        resultValues.add(resultValue);
                    }
                    if (resultValues.size() != 0) {
                        in.setValue("/records" + resultKeys.get(x), resultValues);
                    }
                    break;
                }
            }
        }
        return in;
    }

    /**
     * 将每个数进行P次方处理,默认进行二次方处理
     * @param value
     * @return
     */
    private double getPow(double value){
        return Math.pow(value,p);
    }

    /**
     * 对每一行的数据处理和进行p次幂计算
     * @param value
     * @return
     */
    private double openPow(double value){
        return Math.pow(value,1.0/p);
    }

    /**
     * 对特征向量的每个属性进行处理
     * @param loop
     * @param value
     * @return
     */
    private double normal(int loop,double value){
        double resultValue = 0;
        double result = value/feature_p[loop];
        if(!Double.isNaN(result)){
            resultValue = new BigDecimal(result).setScale(2,BigDecimal.ROUND_DOWN).doubleValue();
        }
        return resultValue;
    }

    @Override
    public TaskType getType() {
        return TaskType.Normalization;
    }

    @Override
    public String getName() {
        return TASK_NAME_SCALE;
    }

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
                LOGGER.info("[SVM] resultKeys.size()==== {}", resultKeys.size());
//                resultKey = params.get("resultKey").toString();
            }
            if(params.containsKey("p")){
                p = Integer.parseInt(params.get("p").toString());
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("[SVM] " + e.toString());
        } catch (NullPointerException e) {
            LOGGER.error("[SVM] " + e.toString());
        }

        LOGGER.info("[SVM] Leaving setParam Method");
    }
}
