package com.quark.datastream.runtime.task.function;

public final class ErrorFunction {

    private ErrorFunction() {
    }

    public enum MEASURE { MSE, RMSE, ME, MAE, INVALID }

    /**
     * Link : https://en.wikipedia.org/wiki/Mean_squared_error.
     *
     * @param predictor
     * @param observation
     * @return error
     */
    private static double meanSquaredError(double[] predictor, double[] observation) {
        if (predictor.length != observation.length) {
            System.err.println("ARRAY LENGTH NOT MATCHED");
            throw new ArrayIndexOutOfBoundsException();
        }
        int len = predictor.length;
        double rss = 0.0;
        for (int i = 0; i < len; i++) {
            rss += Math.pow(observation[i] - predictor[i], 2);
        }

        return (rss / len);
    }

    /**
     * @param predictor
     * @param observation
     * @return error
     */
    private static double meanError(double[] predictor, double[] observation) {
        if (predictor.length != observation.length) {
            System.err.println("ARRAY LENGTH NOT MATCHED");
            throw new ArrayIndexOutOfBoundsException();
        }
        int len = predictor.length;
        double rss = 0.0;
        for (int i = 0; i < len; i++) {
            rss += (observation[i] - predictor[i]);
        }

        return (rss / len);
    }

    /**
     * @param predictor
     * @param observation
     * @return error
     */
    private static double meanAbsoluteError(double[] predictor, double[] observation) {
        if (predictor.length != observation.length) {
            System.err.println("ARRAY LENGTH NOT MATCHED");
            throw new ArrayIndexOutOfBoundsException();
        }
        int len = predictor.length;
        double rss = 0.0;
        for (int i = 0; i < len; i++) {
            rss += Math.abs(observation[i] - predictor[i]);
        }

        return (rss / len);
    }

    /**
     * Link : https://en.wikipedia.org/wiki/Root-mean-square_deviation.
     *
     * @param predictor
     * @param observation
     * @return error
     */
    private static double rootMeanSquaredError(double[] predictor, double[] observation) {
        if (predictor.length != observation.length) {
            System.err.println("ARRAY LENGTH NOT MATCHED");
            throw new ArrayIndexOutOfBoundsException();
        }
        int len = predictor.length;
        double rss = 0.0;
        for (int i = 0; i < len; i++) {
            rss += Math.pow(observation[i] - predictor[i], 2);
        }

        return Math.sqrt(rss / len);
    }

    /**
     * Calculate error value.
     *
     * @param predictor
     * @param observation
     * @param type
     * @return NON-NEGATIVE (if calculated), -1 (if cannot calculate)
     */
    public static double calculate(double[] predictor, double[] observation, MEASURE type) {

        if (predictor.length != observation.length) {
            System.err.println("ARRAY LENGTH NOT MATCHED");
            throw new ArrayIndexOutOfBoundsException();
        }

        if (type == MEASURE.MSE) {
            return meanSquaredError(predictor, observation);
        } else if (type == MEASURE.RMSE) {
            return rootMeanSquaredError(predictor, observation);
        } else if (type == MEASURE.ME) {
            return meanError(predictor, observation);
        } else if (type == MEASURE.MAE) {
            return meanAbsoluteError(predictor, observation);
        } else {
            System.err.println("NOT SUPPORTED TYPE");
            return -1.0;
        }
    }

}
