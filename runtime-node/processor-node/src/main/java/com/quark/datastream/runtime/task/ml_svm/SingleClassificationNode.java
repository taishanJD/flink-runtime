package com.quark.datastream.runtime.task.ml_svm;

import com.quark.datastream.runtime.task.*;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * machine learning SVM node
 */
public class SingleClassificationNode extends AbstractTaskNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleClassificationNode.class);

    private static final String TASK_NAME_SVM = "svm";

    private String[] mSupportVectors = null;
    private svm_model model = null;

    @TaskParam(key = "inputKeys", uiName = "input keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "enter input keys")
    private List<String> inputKeys;

    @TaskParam(key = "resultKey", uiName = "result key", uiType = TaskParam.UiFieldType.STRING, tooltip = "enter result key")
    private String resultKey;

    @TaskParam(key = "svmType", uiName = "SVM type", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "C_SVC(0), NU_SVC(1), ONE_CLASS(2), EPSILON_SVR(3), NU_SVR(4)")
    protected Integer algoType; // model.param.svm_type

    @TaskParam(key = "kernelType", uiName = "Kernel type", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "linear(0), polynomial(1), radial(2), sigmoid(3)")
    protected Integer kernelType; // model.param.kernel_type

    @TaskParam(key = "gamma", uiName = "Gamma Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter gamma value")
    protected Double gamma; // model.param.gamma

    @TaskParam(key = "sVectors", uiName = "Lists of supporting vectors for each classes", uiType = TaskParam.UiFieldType.STRING, tooltip = "Enter list of  S.Vectors")
    protected String sVectors; // model.SV

    @TaskParam(key = "rho", uiName = "rho", uiType = TaskParam.UiFieldType.ARRAYNUMBER, tooltip = "Enter RHO values for each classes")
    protected List<Number> rho; // model.rho

    public SingleClassificationNode() {
        model = new svm_model();
        model.param = new svm_parameter();
        model.SV = null;
        model.rho = null;
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
        LOGGER.info("[SVM] String calculation , in= {}", in.toString());
        List<List<Number>> vectors = new ArrayList<>();
        for (int index = 0; index < inputKeys.size(); index++) {
            List<Number> values = in.getValue("/records" + inputKeys.get(index), List.class);
            vectors.add(values);
        }
        LOGGER.info("[SVM] vectors= {}", vectors);
        if (vectors.size() == inputKeys.size()) {
            List<Double> resultValue = new ArrayList<>();
            for (int loop = 0; loop < vectors.get(0).size(); loop++) {

                // 获取每行支持向量的个数
                int svm_size = 0;
                for (int index = 0; index < vectors.size(); index++) {
                    if (null != vectors.get(index).get(loop) && "" != (String.valueOf(vectors.get(index).get(loop)))) {
                        svm_size++;
                    }
                }
                svm_node[] dataRecord = new svm_node[svm_size];

                // 给每个支持向量赋值
                int nodeIndex = 0;
                for (int index = 0; index < vectors.size(); index++) {
                    dataRecord[nodeIndex] = new svm_node();
                    while (null == vectors.get(index).get(loop) || "" == (String.valueOf(vectors.get(index).get(loop)))) {
                        index++;
                    }
                    dataRecord[nodeIndex].index = index + 1;
                    dataRecord[nodeIndex].value = vectors.get(index).get(loop).doubleValue();
                    nodeIndex++;
                }
                double result = svm.svm_predict(model, dataRecord);
                resultValue.add(result);
            }
            String[] tokens = resultKey.split("\\.");
            StringBuffer sb = new StringBuffer();
            for (String token : tokens) {
                sb.append("/" + token);
            }
            LOGGER.info("[SVM] resultValue==== {}", resultValue);
            in.setValue("/records" + sb.toString(), resultValue);
        } else {
            LOGGER.error("[SVM] Feature value extraction from given data failed~!!");
        }

        LOGGER.info("[SVM] Returning calculation result");
        return in;
    }

    /**
     * 任务类型
     */
    @Override
    public TaskType getType() {
        return TaskType.CLASSIFICATION;
    }

    /**
     * 任务名称
     *
     * @return
     */
    @Override
    public String getName() {
        return TASK_NAME_SVM;
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
                resultKey = params.get("resultKey").toString();
            }
            // ONE_CLASS(2)
            model.param.svm_type = Integer.parseInt(params.get("svmType").toString());

            // linear(0), polynomial(1), radial(2), sigmoid(3)
            model.param.kernel_type = Integer.parseInt(params.get("kernelType").toString());

           /* if (params.containsKey("degree")) {
                model.param.degree = Integer.parseInt(params.get("degree").toString());
            }*/
            if (params.containsKey("gamma")) {
                model.param.gamma = Double.parseDouble(params.get("gamma").toString());
            }

            model.nr_class = 2;
            if (params.containsKey("rho")) {
//                model.rho = TaskNodeParam.transformToNativeDouble1DArray((List<Number>) params.get("rho"));
                rho = new ArrayList<>();
                String[] rhos = params.get("rho").toString().split(" ");
                for (String nsv : rhos) {
                    rho.add(Double.parseDouble(nsv));
                }
                model.rho = TaskNodeParam.transformToNativeDouble1DArray(rho);
            }
            if (params.containsKey("sVectors")) {

                String svStr = (String) params.get("sVectors");
                String[] svStrArray = svStr.split("\n");
                // Set number of features.
                model.l = svStrArray.length;
                // Set support vectors for each classes
                model.SV = new svm_node[model.l][];
                model.sv_coef = new double[1][model.l];
                for (int i = 0; i < model.l; i++) {
                    String sv = svStrArray[i];
                    String[] svArray = sv.split(" ");
                    model.SV[i] = new svm_node[svArray.length - 1];
                    model.sv_coef[0][i] = Double.parseDouble(svArray[0]);

                    for (int j = 1; j < svArray.length; j++) {
                        model.SV[i][j - 1] = new svm_node();
                        model.SV[i][j - 1].index = Integer.parseInt(svArray[j].split(":")[0]);
                        model.SV[i][j - 1].value = Double.parseDouble(svArray[j].split(":")[1]);
                    }
                }
                LOGGER.info("svCoef==={}", model.sv_coef[0]);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("[SVM] " + e.toString());
        } catch (NullPointerException e) {
            LOGGER.error("[SVM] " + e.toString());
        }

        LOGGER.info("[SVM] Leaving setParam Method");
    }
}
