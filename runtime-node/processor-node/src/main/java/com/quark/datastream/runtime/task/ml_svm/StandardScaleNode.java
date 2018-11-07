package com.quark.datastream.runtime.task.ml_svm;

import com.quark.datastream.runtime.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StandardScaleNode extends AbstractTaskNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardScaleNode.class);

    private static final String TASK_NAME_SCALE = "scale";

    private double[] feature_ave;                //feature平均值
    private double[] feature_standardDiviation;  //feature标准差

    @TaskParam(key = "inputKeys", uiName = "input keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "enter input keys")
    private List<String> inputKeys;

    @TaskParam(key = "resultKeys", uiName = "result keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "enter result keys")
    private List<String> resultKeys;


    /**
     * 任务类型
     *
     * @return
     */
    @Override
    public TaskType getType() {
        return TaskType.STANDARDSCALE;
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
            //初始化为一个double一维数组
            try {
                feature_ave = new double[(max_index + 1)];
                feature_standardDiviation = new double[(max_index + 1)];
            } catch (OutOfMemoryError e) {
                System.err.println("can't allocate enough memory");
                System.exit(1);
            }

            // 2.find out ave/standardDiviation value
            for(int i = 0;i < vectors.size();i++) {
                List<Number> list = vectors.get(i);
                double[] x = new double[list.size()];
                for(int j = 0;j < list.size();j++){
                    double num =  list.get(j).doubleValue();
                    x[j] = num;
                }
                double ave = getAve(x);
//                System.out.println("平均数="+new BigDecimal(ave).setScale(2,BigDecimal.ROUND_DOWN));
                feature_ave[i] = ave;
                double sd = StandardDiviation(x);
//                System.out.println("方差="+new BigDecimal(sd).setScale(2,BigDecimal.ROUND_DOWN));
                feature_standardDiviation[i] = sd;
            }

            // standardScale
            for (int index = 0; index < vectors.size(); index++) {
                double value;

                for (int x = index; x < resultKeys.size(); x++) {
                    LOGGER.info("[SVM] resultKeys.get(x)==== {}",resultKeys.get(x));
                    LOGGER.info("[SVM] resultKeys.size()==== {}",resultKeys.size());
                    List<Double> values =  new ArrayList<>();
                    for (int loop = 0; loop < vectors.get(0).size(); loop++) {

                        value = vectors.get(index).get(loop).doubleValue();

                        // scale feature
                        if (isEqual(vectors.get(index)) == 1) {
                            break;
                        } else {
                            double feature_scale = output(index, value);
                            values.add(feature_scale);
                        }
                    }
                    LOGGER.info("[SVM] values.size() ==== {}",values.size());
                    if(values.size() != 0){
                        in.setValue("/records" + resultKeys.get(x),values);
                    }
                    break;
                }
            }
        }

        return in;
    }

    /**
     * 判断方差是否为0
     *
     * @param list
     * @return
     */
    public int isEqual(List<Number> list) {
        Set set = new HashSet();
        for(int i = 0;i < list.size();i++){
            set.add(list.get(i));
        }
        return set.size();
    }

    /**
     * 平均值
     *
     * @param x double数组
     * @return
     */
    public double getAve(double[] x) {
        int m=x.length;
        double sum=0;
        for(int i=0;i<m;i++){//求和
            sum+=x[i];
        }
        double dAve=sum/m;//求平均值
        return dAve;
    }

    /**
     * 标准差σ=sqrt(s^2)
     *
     * @param x double数组
     * @return
     */
    public double StandardDiviation(double[] x) {
        int m=x.length;
        double sum=0;
        for(int i=0;i<m;i++){//求和
            sum+=x[i];
        }
        double dAve=sum/m;//求平均值
        double dVar=0;
        for(int i=0;i<m;i++){//求方差
            dVar+=(x[i]-dAve)*(x[i]-dAve);
        }
        return Math.sqrt(dVar/m);
    }

    /**
     * 重点缩放
     *
     * @param index
     * @param value
     * @return
     */
    private double output(int index, double value) {

        double result = (value - feature_ave[index])/feature_standardDiviation[index];
        return new BigDecimal(result).setScale(2,BigDecimal.ROUND_DOWN).doubleValue();
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
                LOGGER.info("[SVM] resultKeys.size()==== {}", resultKeys.size());
//                resultKey = params.get("resultKey").toString();
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("[SVM] " + e.toString());
        } catch (NullPointerException e) {
            LOGGER.error("[SVM] " + e.toString());
        }

        LOGGER.info("[SVM] Leaving setParam Method");
    }
}
