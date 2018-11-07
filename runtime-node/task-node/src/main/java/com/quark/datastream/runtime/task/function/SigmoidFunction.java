package com.quark.datastream.runtime.task.function;

public class SigmoidFunction {
    private SigmoidFunction() {   }

    public enum TYPESIGMOID { LOGISTIC, TANH,  INVALID  }

    private static double logistic(double value) {
        return (1 / (1 + Math.exp(-1 * value)));
    }

    private static double hyperbolicTan(double value) {
        return Math.tanh(value);
    }

    /**
     * Calculate sigmoid value
     * @param value
     * @param type SIGMOID Function type (LOGISTIC, TANH)
     * @return calculated value
     */
    public static double calculate(double value, TYPESIGMOID type) {
        if (type == TYPESIGMOID.LOGISTIC) {
            return logistic(value);
        } else if (type == TYPESIGMOID.TANH) {
            return hyperbolicTan(value);
        } else {
            System.err.println("NOT SUPPORTED TYPE");
            return 0;
        }
    }
}
