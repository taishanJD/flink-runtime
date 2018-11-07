package com.quark.datastream.runtime.task.function;

public final class DistanceFunction {

    private DistanceFunction () {    }

    /**
     * Euclidian distance
     * @param first
     * @param second
     * @return
     */
    public static Double distEuclidean (Double[] first, Double[] second) {
        // calculate absolute relative distance
        Double distance = 0.0;

        if(first.length != second.length) {
            System.err.println("Invalid Parameter length First ("+first.length+") Second ("+second.length+")");
        } else {
            for (int i = 0; i < first.length; i++) {
                distance += Math.pow(first[i] - second[i], 2);
            }
            distance = Math.sqrt(distance);
        }
        return distance;
    }

    /**
     * Abs_relative distance
     * @param first
     * @param second
     * @param max
     * @param min
     * @return
     */
    public static Double distABSRelative (Double[] first, Double[] second, Double[] max, Double[] min) {
        // calculate absolute relative distance
        Double distance = 0.0;

        if(first.length != second.length) {

            System.err.println("Invalid Parameter length First ("+first.length+") Second ("+second.length+")");

        } else {

            for (int i = 0; i < first.length; i++) {
                distance += Math.abs(first[i] - second[i]) / (max[i] - min[i]);
            }

            distance = Math.sqrt(distance);
        }

        return distance;
    }

    /**
     * Mahalanobis distance
     * @param first
     * @param second
     * @param covariance : Pre-calculated standard deviation values avg(distance between centroid & point)
     * @return
     */
    public static Double distMahalanobis (Double[] first, Double[] second, Double[] covariance ) {
        // calculate absolute relative distance
        Double distance = 0.0;
        Double z;

        if(first.length != second.length) {

            System.err.println("Invalid Parameter length First ("+first.length+") Second ("+second.length+")");

        } else {

            for (int i = 0; i < first.length; i++) {
                z = ( first[i] - second[i] ) / covariance[i];
                distance += Math.pow( z, 2 );
            }

            distance = Math.sqrt(distance);
        }

        return distance;
    }
}
