package com.quark.datastream.runtime.task.function;

public final class KernelFunction {

    private KernelFunction() {  }

    public enum KERNELTYPE { GAUSSIAN, LOGISTIC, SIGMOID, INVALID  }

    private static double gaussian(double value) {

        double exponant = ((-1.0) / 2.0) * (Math.pow(value, 2));
        double front = (1.0 / Math.sqrt(2.0 * Math.PI));

        return (front * Math.exp(exponant));
    }

    private static double logistic(double value) {
        return (1.0 / (Math.exp(value) + 2 + Math.exp((-1.0) * value)));
    }

    private static double sigmod(double value) {
        return (2.0 / Math.sqrt(Math.PI)) * (1.0 / (Math.exp(value) + Math.exp((-1.0) * value)));
    }

    /**
     * Calculated value with kernel function
     * @param value
     * @param type Kernel function type (GAUSSIAN, LOGISTIC, SIGMOID)
     * @return Calculated value
     */
    public static double calculate(double value, KERNELTYPE type) {
        if (type == KERNELTYPE.GAUSSIAN) {

            return gaussian(value);

        } else if (type == KERNELTYPE.LOGISTIC) {

            return logistic(value);

        } else if (type == KERNELTYPE.SIGMOID) {

            return sigmod(value);

        } else {

            System.err.println("NOT SUPPORTED TYPE");

            return 0;
        }
    }
}
