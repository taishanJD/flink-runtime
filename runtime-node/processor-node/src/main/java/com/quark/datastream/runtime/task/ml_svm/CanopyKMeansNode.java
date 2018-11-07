package com.quark.datastream.runtime.task.ml_svm;

import com.google.gson.Gson;
import com.quark.datastream.runtime.task.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.*;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanopyKMeansNode extends AbstractTaskNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanopyKMeansNode.class);
    private static final String TASK_NAME = "k_means";

    @TaskParam(key = "inputKeys", uiName = "input keys", uiType = TaskParam.UiFieldType.ARRAYSTRING, tooltip = "Enter input keys")
    private List<String> inputKeys;

    @TaskParam(key = "resultKey", uiName = "result key", uiType = TaskParam.UiFieldType.STRING, tooltip = "Enter result key")
    private String resultKey;

    @TaskParam(key = "t1", uiName = "t1", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter t1")
    private Double t1;

    @TaskParam(key = "t2", uiName = "t2", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter t2")
    private Double t2;

    @TaskParam(key = "runClustering", uiName = "runClustering", uiType = TaskParam.UiFieldType.BOOLEAN, tooltip = "Select run clustering or not")
    private Boolean runClustering;

    @TaskParam(key = "convergenceDelta", uiName = "convergence delta", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter convergence delta")
    protected Double convergenceDelta;

    @TaskParam(key = "distanceMeasure", uiName = "distance measure", uiType = TaskParam.UiFieldType.NUMBER, tooltip =
            "Euclidean distance(0), Minkowsk distance(1), Chebyshev distance(2), Manhattan distance(3), Squared euclidean distance(4)" +
                    "Tanimoto distance(5), Weighted euclidean distance(6), Weighted manhattan distance(7)")
    protected Integer distanceMeasure;

    @TaskParam(key = "maxIterations", uiName = "max iterations", uiType = TaskParam.UiFieldType.NUMBER, tooltip = "Enter max iterations")
    private Integer maxIterations;

    @TaskParam(key = "path", uiName = "path", uiType = TaskParam.UiFieldType.STRING, tooltip = "Enter hadoop path")
    private String path;

    /**
     * 设置任务节点参数
     *
     * @param params
     */
    @Override
    public void setParam(TaskNodeParam params) {
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
        if (params.containsKey("t1")) {
            t1 = Double.parseDouble(params.get("t1").toString());
        }
        if (params.containsKey("t2")) {
            t2 = Double.parseDouble(params.get("t2").toString());
        }
        if (params.containsKey("runClustering")) {
            runClustering = (Boolean)params.get("runClustering");
        } else {
            runClustering = false;
        }
        if (params.containsKey("convergenceDelta")) {
            convergenceDelta = Double.parseDouble(params.get("convergenceDelta").toString());
        }
        if (params.containsKey("distanceMeasure")) {
            distanceMeasure = Integer.parseInt(params.get("distanceMeasure").toString());
        }
        if (params.containsKey("maxIterations")) {
            maxIterations = Integer.parseInt(params.get("maxIterations").toString());
        }
        if (params.containsKey("path")) {
            path = params.get("path").toString();
        } else {
            path = "tempClusterDir";
        }
    }

    /**
     * 任务类型
     */
    @Override
    public TaskType getType() {
        return TaskType.CLUSTERING;
    }

    /**
     * 任务名称
     *
     * @return
     */
    @Override
    public String getName() {
        return TASK_NAME;
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
        DataSet out = DataSet.create();
        LOGGER.info("[CANPOY_KMEANS] Start calculation, in= {}", in.toString());
        List<List<Double>> vectors = new ArrayList<>();
        for (int index = 0; index < inputKeys.size(); index++) {
            List<Double> values = in.getValue("/records" + inputKeys.get(index), List.class);
            vectors.add(values);
        }

        try {
            String testDataFile = "tempClusterDataFile";
            String resultDataFile = "tempClusterResultFile";
            LOGGER.info("[CANPOY_KMEANS] vectors= {}", vectors);
            if (vectors.size() == inputKeys.size()) { // 将聚类数据写入临时文件
                DataOutputStream fp = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(testDataFile)));
                for (int row = 0; row < vectors.get(0).size(); row++) {
                    for (int column = 0; column < vectors.size(); column++) {
                        fp.writeBytes(vectors.get(column).get(row).toString() + " ");
                    }
                    fp.writeBytes("\n");
                }
                fp.close();
            } else {
                LOGGER.error("[CANPOY_KMEANS] Feature value extraction from given data failed~!!");
            }
            Path input = new Path(testDataFile);
            Path output = new Path(path);
            Configuration conf = new Configuration();
            HadoopUtil.delete(conf, new Path[]{output});
            DistanceMeasure measure = null;
            switch (distanceMeasure) { //Euclidean distance(0), Minkowsk distance(1), Tanimoto distance(2), Weighted euclidean distance(3), Weighted manhattan distance(4)
                case 0:
                    measure = new EuclideanDistanceMeasure();
                    break;
                case 1:
                    measure = new MinkowskiDistanceMeasure();
                    break;
                case 2:
                    measure = new ChebyshevDistanceMeasure();
                    break;
                case 3:
                    measure = new ManhattanDistanceMeasure();
                    break;
                case 4:
                    measure = new SquaredEuclideanDistanceMeasure();
                    break;
                case 5:
                    measure = new TanimotoDistanceMeasure();
                    break;
                case 6:
                    measure = new WeightedEuclideanDistanceMeasure();
                    break;
                case 7:
                    measure = new WeightedManhattanDistanceMeasure();
                    break;
                default:
                    measure = new EuclideanDistanceMeasure();
                    break;
            }

            Path directoryContainingConvertedInput = new Path(output, "data");
//            System.out.println("Preparing Input");
            LOGGER.info("[CANPOY_KMEANS] Preparing Input");
            InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
//            System.out.println("Running Canopy to get initial clusters");
            LOGGER.info("[CANPOY_KMEANS] Running Canopy to get initial clusters");
            Path canopyOutput = new Path(output, "canopies");
            CanopyDriver.run(new Configuration(), directoryContainingConvertedInput, canopyOutput, measure, t1, t2, false, 0.0D, false);
//            System.out.println("Running KMeans");
            LOGGER.info("[CANPOY_KMEANS] Running KMeans");
            LOGGER.info("[CANPOY_KMEANS] Run Clustering: {}", runClustering);
            KMeansDriver.run(conf, directoryContainingConvertedInput, new Path(canopyOutput, "clusters-0-final"), output, convergenceDelta, maxIterations, runClustering, 0.0D, false);
            Path outGlob = new Path(output, "clusters-*-final");
            Path clusteredPoints = new Path(output, "clusteredPoints");
//            System.out.println("Dumping out clusters from clusters: " + outGlob + " and clusteredPoints:" + clusteredPoints);
            LOGGER.info("[CANPOY_KMEANS] Dumping out clusters from clusters: {} and clusteredPoints: {}", outGlob, clusteredPoints);
            ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
            String[] args = {"-i", path + "/clusters-*-final", "-o", resultDataFile, "-p", path + "/clusteredPoints"};
            clusterDumper.run(args);
            BufferedReader br = new BufferedReader(new FileReader(resultDataFile));
            String line;
            StringBuffer resultSb = new StringBuffer();
            while ((line = br.readLine()) != null) {
                resultSb.append(line + "\n");
            }
            br.close();

            Gson gson = new Gson();
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put(resultKey, resultSb.toString());
            LOGGER.info("[CANPOY_KMEANS] resultValue==== {}", resultSb.toString());
            out.addRecord(gson.toJson(recordMap));

            // 删除临时文件
            File testData = new File(testDataFile);
            File delResult = new File(resultDataFile);
            deleteAll(testData);
            deleteAll(delResult);
            if ("tempClusterDir".equals(path)) {
                File clusterFile = new File(path);
                deleteAll(clusterFile);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOGGER.info("[CANPOY_KMEANS] Returning calculation result");
        return out;
    }

    public static void deleteAll(File file) {

        if (file.isFile() || file.list().length == 0) {
            file.delete();
        } else {
            for (File f : file.listFiles()) {
                deleteAll(f); // 递归删除每一个文件
            }
            file.delete(); // 删除文件夹
        }
    }
}
