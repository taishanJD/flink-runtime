package com.quark.datastream.runtime.task.ml_svm;

import com.google.gson.Gson;
import com.quark.datastream.runtime.task.*;
import com.quark.datastream.runtime.task.util.JsonUtil;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.io.*;
import java.util.*;

/**
 * machine learning SVM node
 */
public class MultiClassificationNode extends AbstractTaskNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiClassificationNode.class);

    private static final String TASK_NAME_SVM = "svm";

    private String[] mSupportVectors = null;
    private svm_model model = null;

    @TaskParam(key = "inputKeys", uiName = "input keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "enter input keys")
    private List<String> inputKeys;

    @TaskParam(key = "resultKey", uiName = "result key", uiType = TaskParam.UiFieldType.STRING, tooltip = "enter result key")
    private String resultKey;

    @TaskParam(key = "labels", uiName = "Label for each classes", uiType = TaskParam.UiFieldType.ARRAYNUMBER, tooltip = "Enter Labels of each classes")
    private String[] mClasses = null;

    @TaskParam(key = "svmType", uiName = "SVM type", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "C_SVC(0), NU_SVC(1), ONE_CLASS(2), EPSILON_SVR(3), NU_SVR(4)")
    protected Integer algoType; // model.param.svm_type

    @TaskParam(key = "kernelType", uiName = "Kernel type", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "linear(0), polynomial(1), radial(2), sigmoid(3)")
    protected Integer kernelType; // model.param.kernel_type

    /*@TaskParam(key = "degree", uiName = "Degree Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter degree value")
    protected Double degree; // model.param.degree*/

    @TaskParam(key = "gamma", uiName = "Gamma Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter gamma value")
    protected Double gamma; // model.param.gamma

    /*@TaskParam(key = "coef0", uiName = "Coefficient0 Value", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter coef0 value")
    protected Double coef0; // model.param.coef0*/

    @TaskParam(key = "nSV", uiName = "# of supporting vectors for each classes", uiType = TaskParam.UiFieldType.ARRAYNUMBER, tooltip = "Enter # of S.Vectors")
    protected List<Number> nSV; // model.nSV

    @TaskParam(key = "sVectors", uiName = "Lists of supporting vectors for each classes", uiType = TaskParam.UiFieldType.STRING, tooltip = "Enter list of  S.Vectors")
    protected String sVectors; // model.SV

    /*@TaskParam(key = "svCoef", uiName = "Lists of coefficients of supporting vectors of each classes", uiType = TaskParam.UiFieldType.ARRAYOBJECT, tooltip = "Enter coefficients of S.Vectors")
    protected List<List<Number>> scCoef; // model.sv_coef*/

    @TaskParam(key = "rho", uiName = "rho", uiType = TaskParam.UiFieldType.ARRAYNUMBER, tooltip = "Enter RHO values for each classes")
    protected List<Number> rho; // model.rho

    public MultiClassificationNode() {
        model = new svm_model();
        model.param = new svm_parameter();
        model.SV = null;
        model.rho = null;
        model.label = null;
        model.nSV = null;
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
        List<List<Double>> vectors = new ArrayList<>();
        for (int index = 0; index < inputKeys.size(); index++) {
            List<Double> values = in.getValue("/records" + inputKeys.get(index), List.class);
            LOGGER.info("[SVM] records values = {}", values);
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
                LOGGER.info("[SVM] result==== {}", result);
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
            // C_SVC(0), NU_SVC(1), ONE_CLASS(2), EPSILON_SVR(3), NU_SVR(4)
            model.param.svm_type = Integer.parseInt(params.get("svmType").toString());

            // linear(0), polynomial(1), radial(2), sigmoid(3)
            model.param.kernel_type = Integer.parseInt(params.get("kernelType").toString());

           /* if (params.containsKey("degree")) {
                model.param.degree = Integer.parseInt(params.get("degree").toString());
            }*/
            if (params.containsKey("gamma")) {
                model.param.gamma = Double.parseDouble(params.get("gamma").toString());
            }
            /*if (params.containsKey("coef0")) {
                model.param.coef0 = Double.parseDouble(params.get("coef0").toString());
            }*/
            if (params.containsKey("labels")) {
//                List<String> lList = (List<String>) params.get("labels");
                List<String> lList = Arrays.asList(params.get("labels").toString().split(" "));
                mClasses = lList.toArray(new String[lList.size()]);
                // Set number of classes
                model.nr_class = mClasses.length;
                model.label = new int[mClasses.length];
                // Set label of classes (using numeric value by default)
                for (int index = 0; index < model.label.length; index++) {
                    model.label[index] = Integer.valueOf(lList.get(index));
                }
            }
            if (params.containsKey("nSV")) {
                // Set number of SVs for each classes.
//                model.nSV = TaskNodeParam.transformToNativeInt1DArray((List<Number>) params.get("nSV"));
                nSV = new ArrayList<>();
                String[] nSvs = params.get("nSV").toString().split(" ");
                for (String nsv : nSvs) {
                    nSV.add(Integer.parseInt(nsv));
                }
                model.nSV = TaskNodeParam.transformToNativeInt1DArray(nSV);
            }
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
                model.sv_coef = new double[model.label.length - 1][model.l];
                for (int coefIndex = 0; coefIndex < model.label.length - 1; coefIndex++) {
                    for (int i = 0; i < model.l; i++) {
                        String sv = svStrArray[i];
                        String[] svArray = sv.split(" ");
                        model.SV[i] = new svm_node[svArray.length - model.label.length + 1];
                        model.sv_coef[coefIndex][i] = Double.parseDouble(svArray[coefIndex]);

                        for (int j = model.label.length; j < svArray.length + 1; j++) {
                            model.SV[i][j - model.label.length] = new svm_node();
                            model.SV[i][j - model.label.length].index = Integer.parseInt(svArray[j - 1].split(":")[0]);
                            model.SV[i][j - model.label.length].value = Double.parseDouble(svArray[j - 1].split(":")[1]);
                        }
                    }
                    LOGGER.info("svCoef[{}]==={}", coefIndex, model.sv_coef[coefIndex]);
                }
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("[SVM] " + e.toString());
        } catch (NullPointerException e) {
            LOGGER.error("[SVM] " + e.toString());
        }

        LOGGER.info("[SVM] Leaving setParam Method");
    }
}
