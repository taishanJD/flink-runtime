package com.quark.datastream.runtime.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class TaskNodeParam extends HashMap<String, Object> implements Serializable {
    /**
     * Create Instance of TaskNodeParam
     * @param data : Contents to carry
     * @return
     */
    public static TaskNodeParam create(String data) {
        TaskNodeParam self = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            self = mapper.readValue(data, new TypeReference<TaskNodeParam>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return self;
        }
    }

    @Deprecated
    /**
     * Transform List<Double> in the List to native 2-D double array list
     * @deprecated
     * @param param List<List<Double>>
     * @return double[][]
     */
    public static double[][] extractDouble2DArray(List<List<Number>> param) {
        double[][] rtn = new double[param.size()][];
        List<Number> subParam;
        for (int i = 0; i < param.size(); i++) {
            subParam = param.get(i);
            rtn[i] = new double[subParam.size()];
            for (int j = 0; j < subParam.size(); j++) {
                // Converting to double from string is inefficient,
                // but it is difficult to know what data type the element is beforehand.
                rtn[i][j] = Double.parseDouble(String.valueOf(subParam.get(j)));
            }
        }
        return rtn;
    }

    @Deprecated
    /**
     * Transform List<Double> in the List to native 2-D double array list
     * @deprecated
     * @param param List<Double>
     * @return double[]
     */
    public static double[] extractDouble1DArray(List<Number> param) {
        double[] rtn =
                ArrayUtils.toPrimitive(param.toArray(new Double[param.size()]));
        return rtn;
    }

    /**
     * Transform List<Double> in the List to native 2-D double array list
     *
     * @param param List<List<Double>>
     * @return double[][]
     */
    public static double[][] transformToNativeDouble2DArray(List<List<Number>> param) {
        double[][] rtn = new double[param.size()][];
        List<Number> subParam;
        for (int i = 0; i < param.size(); i++) {
            subParam = param.get(i);
            rtn[i] = new double[subParam.size()];
            for (int j = 0; j < subParam.size(); j++) {
                // Converting to double from string is inefficient,
                // but it is difficult to know what data type the element is beforehand.
                rtn[i][j] = Double.parseDouble(String.valueOf(subParam.get(j)));
            }
        }
        return rtn;
    }

    /**
     * Transform List<List<Double>> in the ArrayList to 2-D Double array list
     *
     * @param param List<List<Double>>
     * @return Double[][]
     */
    public static Double[][] transformToDouble2DArray(List<List<Number>> param) {
        Double[][] rtn = new Double[param.size()][];
        List<Number> subParam;
        for (int i = 0; i < param.size(); i++) {
            subParam = param.get(i);
            rtn[i] = new Double[subParam.size()];
            for (int j = 0; j < subParam.size(); j++) {
                // Converting to double from string is inefficient,
                // but it is difficult to know what data type the element is beforehand.
                rtn[i][j] = Double.parseDouble(String.valueOf(subParam.get(j)));
            }
        }
        return rtn;
    }

    /**
     * Transform List<List<Integer>> in the ArrayList to native 2-D int array list
     *
     * @param param List<List<Integer>>
     * @return int[][]
     */
    public static int[][] transformToNativeInt2DArray(List<List<Number>> param) {
        int[][] rtn = new int[param.size()][];
        List<Number> subParam;
        for (int i = 0; i < param.size(); i++) {
            subParam = param.get(i);
            rtn[i] = new int[subParam.size()];
            for (int j = 0; j < subParam.size(); j++) {
                // Converting to double from string is inefficient,
                // but it is difficult to know what data type the element is beforehand.
                rtn[i][j] = Integer.parseInt(String.valueOf(subParam.get(j)));
            }
        }
        return rtn;
    }

    /**
     * Transform List<Integer> to native 2-D Integer array list
     *
     * @param param List<Integer>
     * @return Integer[][]
     */
    public static Integer[][] transformToInt2DArray(List<List<Number>> param) {
        Integer[][] rtn = new Integer[param.size()][];
        List<Number> subParam;
        for (int i = 0; i < param.size(); i++) {
            subParam = param.get(i);
            rtn[i] = new Integer[subParam.size()];
            for (int j = 0; j < subParam.size(); j++) {
                // Converting to double from string is inefficient,
                // but it is difficult to know what data type the element is beforehand.
                rtn[i][j] = Integer.parseInt(String.valueOf(subParam.get(j)));
            }
        }
        return rtn;
    }

    /**
     * Transform List<Double> to double[]
     * @param param
     * @return
     */
    public static double[] transformToNativeDouble1DArray(List<Number> param) {
        int length = param.size();
        double[] res = new double[length];
        for (int i = 0; i < length; i++) {
            res[i] = param.get(i).doubleValue();
        }
        return res;
        //return ArrayUtils.toPrimitive(param.toArray(new Double[param.size()]));
    }

    /**
     * Transform List<Double> to Double[]
     * @param param
     * @return
     */
    public static Double[] transformToDouble1DArray(List<Number> param) {
        return param.toArray(new Double[param.size()]);
    }

    /**
     * Transform List<Integer> to int[]
     * @param param
     * @return
     */
    public static int[] transformToNativeInt1DArray(List<Number> param) {
        return ArrayUtils.toPrimitive(param.toArray(new Integer[param.size()]));
    }

    /**
     * Transform List<Integer> to Integer[]
     * @param param
     * @return
     */
    public static Integer[] transformToInt1DArray(List<Number> param) {
        return param.toArray(new Integer[param.size()]);
    }

    @Override
    /**
     * Represents contents in the task model parameters in string
     * @param param
     * @return
     */
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }
}
