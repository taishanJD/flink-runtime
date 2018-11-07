package com.quark.datastream.runtime.task.ml_svm.LibSvmTest;

import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.StringTokenizer;

public class svmTest {
    private String testFile = "runtime-node/processor-node/src/test/java/com/quark/datastream/runtime/task/ml_svm/LibSvmTest/heart_scale";
    private String outputFile = "runtime-node/processor-node/src/test/java/com/quark/datastream/runtime/task/ml_svm/LibSvmTest/heart_scale.output";
    //训练后得到的模型文件
    private String modelFile = "runtime-node/processor-node/src/test/java/com/quark/datastream/runtime/task/ml_svm/LibSvmTest/heart_scale.model";
    //svm训练分类模型参数
    public String[] trainArgs = {"-s", "3", testFile, modelFile};
    //svm预测结果参数
    private String[] predictArgs = {testFile, modelFile, outputFile};
    private static svmTest libSVM = null;
    PreparedStatement pstmt = null;
    /*
     * 私有化构造函数，并训练分类器，得到分类模型
     */

    Connection con = null;

    private svmTest() {

    }

    public static svmTest getInstance() {
        if (libSVM == null)
            libSVM = new svmTest();
        return libSVM;
    }

    /*
     * 训练模型
     */
    public void trainByLibSVM() {
        try {
            svm_train.main(trainArgs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 预测,并返回准确率
     */
    public double predictByLibSVM() {
        double accuracy = 0;
        try {
            svm_predict.main(predictArgs);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return accuracy;
    }

    private static double atof(String s) {
        return Double.valueOf(s).doubleValue();
    }

    private static Integer asof(String s) {
        return Integer.valueOf(s);
    }

    /**
     * 读取ML测试文件并写入数据库
     *
     * @param
     */
    public void readNWrite() throws IOException, ClassNotFoundException {
        BufferedReader input = new BufferedReader(new FileReader(testFile));
        Class.forName("com.mysql.jdbc.Driver");
        final String URL = "jdbc:mysql://192.168.2.106:3306/test?useUnicode=true&characterEncoding=utf-8";
        try {
            con = DriverManager.getConnection(URL, "root", "root");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("正在写入sql...");
        while (true) {
            StringBuffer key = new StringBuffer();
            StringBuffer value = new StringBuffer();
            String line = input.readLine();
            if (line == null) break;
            /*Double label = atof(line.substring(0, 2));
            sb.append("'" + label + "',");*/
            key.append("`label`,");
            StringTokenizer st = new StringTokenizer(line, " \t\n\r\f:");
            int m = st.countTokens();
            for (int j = 0; j < m; j++) {
                value.append("'" + atof(st.nextToken()) + "',");
                if (j < m - 1) {
                    key.append("`test" + asof(st.nextToken()) + "`,");
                    j++;
                }
            }
            String sql = "INSERT INTO `svm_test` (" + key.toString().substring(0, key.length() - 1) +
                    ") VALUES (" + value.toString().substring(0, value.length() - 1) + ");";
            try {
                this.pstmt = this.con.prepareStatement(sql);
                this.pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            this.con.close();
            System.out.println("sql执行完毕!");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 读取Cluster测试文件并写入数据库
     *
     * @param
     */
    public void readNWrite2() throws IOException, ClassNotFoundException {
        BufferedReader input = new BufferedReader(new FileReader("testdata"));
        Class.forName("com.mysql.jdbc.Driver");
        final String URL = "jdbc:mysql://192.168.2.106:3306/test?useUnicode=true&characterEncoding=utf-8";
        try {
            con = DriverManager.getConnection(URL, "root", "root");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("正在写入sql...");
        while (true) {
            StringBuffer key = new StringBuffer();
            StringBuffer value = new StringBuffer();
            String line = input.readLine();
            if (line == null) break;
            StringTokenizer st = new StringTokenizer(line, " \t\n\r\f:");
            int m = st.countTokens();
            for (int j = 0; j < m; j++) {
                value.append("'" + atof(st.nextToken()) + "',");
                key.append("`test" + (j + 1) + "`,");
            }
            String sql = "INSERT INTO `cluster_test` (" + key.toString().substring(0, key.length() - 1) +
                    ") VALUES (" + value.toString().substring(0, value.length() - 1) + ");";
            try {
                this.pstmt = this.con.prepareStatement(sql);
                this.pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            this.con.close();
            System.out.println("sql执行完毕!");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 预测一条数据结果测试
     *
     * @return
     */
    public void singlePredict() {
        svm_model model = new svm_model();
        model.param = new svm_parameter();
        model.SV = new svm_node[1][13];
        model.SV[0][0] = new svm_node();
        model.SV[0][0].index = 1;
        model.SV[0][0].value = 0.166667;
        model.SV[0][1] = new svm_node();
        model.SV[0][1].index = 2;
        model.SV[0][1].value = 1;
        model.SV[0][2] = new svm_node();
        model.SV[0][2].index = 3;
        model.SV[0][2].value = -0.333333;
        model.SV[0][3] = new svm_node();
        model.SV[0][3].index = 4;
        model.SV[0][3].value = -0.433962;
        model.SV[0][4] = new svm_node();
        model.SV[0][4].index = 5;
        model.SV[0][4].value = -0.383562;
        model.SV[0][5] = new svm_node();
        model.SV[0][5].index = 6;
        model.SV[0][5].value = -1.0;
        model.SV[0][6] = new svm_node();
        model.SV[0][6].index = 7;
        model.SV[0][6].value = -1.0;
        model.SV[0][7] = new svm_node();
        model.SV[0][7].index = 8;
        model.SV[0][7].value = 0.0687023;
        model.SV[0][8] = new svm_node();
        model.SV[0][8].index = 9;
        model.SV[0][8].value = -1.0;
        model.SV[0][9] = new svm_node();
        model.SV[0][9].index = 10;
        model.SV[0][9].value = -0.903226;
        model.SV[0][10] = new svm_node();
        model.SV[0][10].index = 11;
        model.SV[0][10].value = -1;
        model.SV[0][11] = new svm_node();
        model.SV[0][11].index = 12;
        model.SV[0][11].value = -1;
        model.SV[0][12] = new svm_node();
        model.SV[0][12].index = 13;
        model.SV[0][12].value = 1;

        model.rho = new double[1];
        model.rho[0] = 0.42446205176771579;
        model.probA = null;
        model.probB = null;
        model.label = new int[2];
        model.label[0] = 1;
        model.label[1] = -1;
        model.nSV = new int[2];
        model.nSV[0] = 0;
        model.nSV[1] = 1;
        model.nr_class = 2;
        model.param.svm_type = 0;
        model.param.kernel_type = 2;
        model.param.cache_size = 0.0;
        model.param.eps = 0.0;
        model.param.C = 0.0;
        model.param.nr_weight = 0;
        model.param.weight_label = null;
        model.param.weight = null;
        model.param.nu = 0.0;
        model.param.p = 0.0;
        model.param.shrinking = 0;
        model.param.probability = 0;
        model.sv_coef = new double[model.label.length - 1][1];
        model.sv_coef[0][0] = -1;
        model.l = 1;
        svm_node[] x = new svm_node[12];
        x[0] = new svm_node();
        x[0].index = 1;
        x[0].value = 0.708333;
        x[1] = new svm_node();
        x[1].index = 2;
        x[1].value = 1;
        x[2] = new svm_node();
        x[2].index = 3;
        x[2].value = 1;
        x[3] = new svm_node();
        x[3].index = 4;
        x[3].value = -0.320755;
        x[4] = new svm_node();
        x[4].index = 5;
        x[4].value = -0.105023;
        x[5] = new svm_node();
        x[5].index = 6;
        x[5].value = -1.0;
        x[6] = new svm_node();
        x[6].index = 7;
        x[6].value = 1.0;
        x[7] = new svm_node();
        x[7].index = 8;
        x[7].value = -0.419847;
        x[8] = new svm_node();
        x[8].index = 9;
        x[8].value = -1.0;
        x[9] = new svm_node();
        x[9].index = 10;
        x[9].value = -0.225806;
        x[10] = new svm_node();
        x[10].index = 12;
        x[10].value = 1;
        x[11] = new svm_node();
        x[11].index = 13;
        x[11].value = -1;
        double[] dec_values = new double[1];
        double result = svm.svm_predict_values(model, x, dec_values);
        System.out.println("预测结果: " + result);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        svmTest svmTest = com.quark.datastream.runtime.task.ml_svm.LibSvmTest.svmTest.getInstance();

//        System.out.println("开始训练模型"); //训练模型
//        svmTest.trainByLibSVM();
//
//        System.out.println("开始预测结果"); //预测结果
//        svmTest.predictByLibSVM();

//        System.out.println("开始读取测试文件"); // 读取ML测试文件并写入数据库
//        svmTest.readNWrite();
        svmTest.readNWrite2();

//        System.out.println("开始执行单条预测");// 单条预测
//        svmTest.singlePredict();
    }

}
