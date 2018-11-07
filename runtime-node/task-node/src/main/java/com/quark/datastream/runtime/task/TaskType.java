package com.quark.datastream.runtime.task;

public enum TaskType {
    CUSTOM(0),  CLASSIFICATION(1), CLUSTERING(2), PATTERN(3), TREND(4), PREPROCESSING(5), REGRESSION(6), FILTER(7), ERROR(8),
    OUTLIER(9), QUERY(10), INVALID(11), TEMPLATE(12), SPLITARRAY(13), EMAILSINK(14), JSFUNCTION(15), SCALE(16), STANDARDSCALE(17), Normalization(18);

    private int mType;

    TaskType(int type) {
        mType = type;
    }

    public int toInt() {
        return mType;
    }

    @Override
    public String toString() {
        return this.name().toUpperCase();
    }

    public static TaskType getType(int type) {
        if (type == REGRESSION.toInt()) {
            return REGRESSION;
        } else if (type == CLASSIFICATION.toInt()) {
            return CLASSIFICATION;
        } else if (type == CLUSTERING.toInt()) {
            return CLUSTERING;
        } else if (type == PATTERN.toInt()) {
            return PATTERN;
        } else if (type == TREND.toInt()) {
            return TREND;
        } else if (type == PREPROCESSING.toInt()) {
            return PREPROCESSING;
        } else if (type == FILTER.toInt()) {
            return FILTER;
        } else if (type == ERROR.toInt()) {
            return ERROR;
        } else if (type == OUTLIER.toInt()) {
            return OUTLIER;
        } else if (type == CUSTOM.toInt()) {
            return CUSTOM;
        }  else if (type == QUERY.toInt()) {
            return QUERY;
        } else if (type == TEMPLATE.toInt()) {
            return TEMPLATE;
        } else if (type == JSFUNCTION.toInt()){
            return JSFUNCTION;
        } else if(type == SCALE.toInt()){
            return SCALE;
        } else {
            return INVALID;
        }
    }

    public static TaskType getType(String type) {
        for (TaskType taskType : TaskType.values()) {
            if (taskType.toString().equalsIgnoreCase(type)) {
                return taskType;
            }
        }

        return null;
    }
}
