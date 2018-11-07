package com.quark.datastream.runtime.task.ml_svm.LibSvmTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.*;
import org.apache.mahout.utils.clustering.ClusterDumper;

import java.io.IOException;

public class Mahout {

    /**
     * 只使用KMeans, 需指定k
     * <p>
     * param:
     * 1.Path Input 所有待聚类的数据点的路径，参数不可缺
     * 2.Path output 聚类结果存储的路径
     * 3.DistanceMeasure measure 数据点间的距离计算方法，参数可缺，默认是 SquaredEuclidean 计算方法
     * 4.Double convergenceDelta 收敛系数 新的簇中心与上次的簇中心的的距离不能超过 convergenceDelta ，如果超过，则继续迭代，否则停止迭代，默认值是 0.001D
     * 5.int maxIterations 最大迭代次数，如果迭代次数小于 maxIterations ，继续迭代，否则停止迭代，与convergenceDelta 满足任何一个停止迭代的条件，则停止迭代
     * 6.boolean runClustering 如果是 true 则在计算簇中心后，计算每个数据点属于哪个簇，否则计算簇中心后结束，默认为 true
     * 7.boolean clusteringOption 采用单机或者 Map Reduce 的方法计算，默认是 false: Map Reduce
     * 8.int k ：簇的个数
     */
    public void kmeansRun() {
        try {
            Path output = new Path("runtime-node/processor-node/src/test/java/com/quark/datastream/runtime/task/ml_svm/LibSvmTest/output");
            Path input = new Path("runtime-node/processor-node/src/test/java/com/quark/datastream/runtime/task/ml_svm/LibSvmTest/testdata");
            Configuration conf = new Configuration();
            HadoopUtil.delete(conf, new Path[]{output});
            int k = 3;
            Double convergenceDelta = 0.5D;
            DistanceMeasure measure = new EuclideanDistanceMeasure();
            int maxIterations = 10;

            Path directoryContainingConvertedInput = new Path(output, "data");
            System.out.println("Preparing Input");
            InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
            System.out.println("Running random seed to get initial clusters");
            Path clusters = new Path(output, "random-seeds");
            clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
            System.out.println("Running KMeans with k = " + k);
            KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, convergenceDelta, maxIterations, false, 0.0D, false);
            Path outGlob = new Path(output, "clusters-*-final");
            Path clusteredPoints = new Path(output, "clusteredPoints");
            System.out.println("Dumping out clusters from clusters: " + outGlob + " and clusteredPoints:" + clusteredPoints);
            ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
            String[] args = {
                    "-i", "output/clusters-*-final",
                    "-o", "points",
//                    "-p", "output/clusteredPoints",
//                    "-of", "TEXT"
            };
            clusterDumper.run(args);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 只使用canopy,得到初始K值及中心点坐标,不需要Canopy方法来做聚类操作
     * <p>
     * param:
     * 1.Path Input 所有待聚类的数据点的路径，参数不可缺
     * 2.Path output 聚类结果存储的路径
     * 3.DistanceMeasure measure 数据点间的距离计算方法，参数可缺，默认是 SquaredEuclidean 计算方法
     * 4.Double t1 弱归属距离,当距离小于T1时，标记当前点为弱归属点，对应的集群加入此点.t1>t2
     * 5.Double t2 强归属距离,当距离小于T2时，标记当前点为强归属点。如果当前点不是强归属点，则以当前点为中心创建一个新的集群。对所有的集群中心点，计算新的集群
     * 6.boolean runClustering ：如果是 true 则在计算簇中心后，计算每个数据点属于哪个簇，否则计算簇中心后结束，默认为 true
     * 7.boolean clusteringOption ：采用单机或者 Map Reduce 的方法计算，默认是 false: Map Reduce
     *
     * @throws Exception
     */
    public void canopyRun() throws Exception {
        Path output = new Path("output");
        Path input = new Path("testdata");
        Configuration conf = new Configuration();
        HadoopUtil.delete(conf, new Path[]{output});
        DistanceMeasure measure = new EuclideanDistanceMeasure();
        Double t1 = 2.0D;
        Double t2 = 1.0D;

        Path directoryContainingConvertedInput = new Path(output, "data");
        System.out.println("Preparing Input");
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
        System.out.println("Running Canopy to get initial clusters");
        CanopyDriver.run(conf, directoryContainingConvertedInput, output, measure, t1, t2, false, 0.0D, false);
        Path outGlob = new Path(output, "clusters-*-final");
        Path clusteredPoints = new Path(output, "clusteredPoints");
        ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
        String[] args = {
                "-i", "output/clusters-*-final",
                "-o", "points",
                "-p", "output/clusteredPoints",
                "-of", "TEXT"
        };
        clusterDumper.run(args);
    }

    /**
     * 用FuzzyKMeans分配数据点到所属的cluster
     * <p>
     * param:
     * 1.Path Input 所有待聚类的数据点的路径
     * 2.Path output 聚类结果存储的路径
     * 3.DistanceMeasure measure 数据点间的距离计算方法，参数可缺，默认是 SquaredEuclidean 计算方法
     * 4.Double t1 弱归属距离,当距离小于T1时，标记当前点为弱归属点，对应的集群加入此点
     * 5.Double t2 强归属距离,当小于T2时，标记当前点为强归属点。如果当前点不是强归属点，则以当前点为中心创建一个新的集群。对所有的集群中心点，计算新的集群
     * 6.fuzziness 模糊参数m,取值在（1，2]区间内，当 m 越大，模糊程度越大
     * 7.Double convergenceDelta 收敛系数 新的簇中心与上次的簇中心的的距离不能超过 convergenceDelta ，如果超过，则继续迭代，否则停止迭代，默认值是 0.001D
     * 8.int maxIterations 最大迭代次数，如果迭代次数小于 maxIterations ，继续迭代，否则停止迭代，与convergenceDelta 满足任何一个停止迭代的条件，则停止迭代
     * 9.boolean runClustering 如果是 true 则在计算簇中心后，计算每个数据点属于哪个簇，否则计算簇中心后结束，默认为 true
     * 10.boolean clusteringOption 采用单机或者 Map Reduce 的方法计算，默认是 false: Map Reduce
     *
     * @throws Exception
     */
    public void fuzzyKMeansRun() throws Exception {

        Path output = new Path("output");
        Path input = new Path("testdata");
        Configuration conf = new Configuration();
        HadoopUtil.delete(conf, new Path[]{output});
        Double convergenceDelta = 0.5D;
        DistanceMeasure measure = new SquaredEuclideanDistanceMeasure();
        int maxIterations = 10;
        float fuzziness = 2.0F;
        int k = 3;

        Path directoryContainingConvertedInput = new Path(output, "data");
        System.out.println("Preparing Input");
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
        Path clusters = new Path(output, "random-seeds");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
        FuzzyKMeansDriver.run(directoryContainingConvertedInput, clusters, output, convergenceDelta, maxIterations, fuzziness, true, true, 0.0D, false);
        Path outGlob = new Path(output, "clusters-*-final");
        Path clusteredPoints = new Path(output, "clusteredPoints");
        System.out.println("Dumping out clusters from clusters: " + outGlob + " and clusteredPoints:" + clusteredPoints);
        ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
        String[] args = {
                "-i", "output/clusters-*-final",
                "-o", "points",
                "-p", "output/clusteredPoints",
                "-of", "JSON"
        };
        clusterDumper.run(args);
    }

    /**
     * 使用canopy得到k, 再用KMeans分配数据点到所属的cluster
     * <p>
     * param:
     * 1.Path Input 所有待聚类的数据点的路径
     * 2.Path output 聚类结果存储的路径
     * 3.DistanceMeasure measure 数据点间的距离计算方法，参数可缺，默认是 SquaredEuclidean 计算方法
     * 4.Double t1 弱归属距离,当距离小于T1时，标记当前点为弱归属点，对应的集群加入此点
     * 5.Double t2 强归属距离,当小于T2时，标记当前点为强归属点。如果当前点不是强归属点，则以当前点为中心创建一个新的集群。对所有的集群中心点，计算新的集群
     * 6.Double convergenceDelta 收敛系数 新的簇中心与上次的簇中心的的距离不能超过 convergenceDelta ，如果超过，则继续迭代，否则停止迭代，默认值是 0.001D
     * 7.int maxIterations 最大迭代次数，如果迭代次数小于 maxIterations ，继续迭代，否则停止迭代，与convergenceDelta 满足任何一个停止迭代的条件，则停止迭代
     * 8.boolean runClustering 如果是 true 则在计算簇中心后，计算每个数据点属于哪个簇，否则计算簇中心后结束，默认为 true
     * 9.boolean clusteringOption 采用单机或者 Map Reduce 的方法计算，默认是 false: Map Reduce
     *
     * @throws Exception
     */
    public void canopyKMeansRun() throws Exception {

        Path output = new Path("output");
        Path input = new Path("testdata");
        Configuration conf = new Configuration();
        HadoopUtil.delete(conf, new Path[]{output});
        Double convergenceDelta = 0.5D;
        DistanceMeasure measure = new SquaredEuclideanDistanceMeasure();
        int maxIterations = 10;
        Double t1 = 2.0D;
        Double t2 = 1.0D;

        Path directoryContainingConvertedInput = new Path(output, "data");
        System.out.println("Preparing Input");
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
        System.out.println("Running Canopy to get initial clusters");
        Path canopyOutput = new Path(output, "canopies");
        CanopyDriver.run(new Configuration(), directoryContainingConvertedInput, canopyOutput, measure, t1, t2, false, 0.0D, false);
        System.out.println("Running KMeans");
        KMeansDriver.run(conf, directoryContainingConvertedInput, new Path(canopyOutput, "clusters-0-final"), output, convergenceDelta, maxIterations, false, 0.0D, false);
        Path outGlob = new Path(output, "clusters-*-final");
        Path clusteredPoints = new Path(output, "clusteredPoints");
        System.out.println("Dumping out clusters from clusters: " + outGlob + " and clusteredPoints:" + clusteredPoints);
        ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
        String[] args = {
                "-i", "output/clusters-*-final",
                "-o", "points",
                "-p", "output/clusteredPoints",
                "-of", "TEXT"
        };
        clusterDumper.run(args);
    }

    /**
     * 使用canopy得到k, 再用FuzzyKMeans分配数据点到所属的cluster
     * <p>
     * param:
     * 1.Path Input 所有待聚类的数据点的路径
     * 2.Path output 聚类结果存储的路径
     * 3.DistanceMeasure measure 数据点间的距离计算方法，参数可缺，默认是 SquaredEuclidean 计算方法
     * 4.Double t1 弱归属距离,当距离小于T1时，标记当前点为弱归属点，对应的集群加入此点
     * 5.Double t2 强归属距离,当小于T2时，标记当前点为强归属点。如果当前点不是强归属点，则以当前点为中心创建一个新的集群。对所有的集群中心点，计算新的集群
     * 6.fuzziness 模糊参数m,取值在（1，2]区间内，当 m 越大，模糊程度越大
     * 7.Double convergenceDelta 收敛系数 新的簇中心与上次的簇中心的的距离不能超过 convergenceDelta ，如果超过，则继续迭代，否则停止迭代，默认值是 0.001D
     * 8.int maxIterations 最大迭代次数，如果迭代次数小于 maxIterations ，继续迭代，否则停止迭代，与convergenceDelta 满足任何一个停止迭代的条件，则停止迭代
     * 9.boolean runClustering 如果是 true 则在计算簇中心后，计算每个数据点属于哪个簇，否则计算簇中心后结束，默认为 true
     * 10.boolean clusteringOption 采用单机或者 Map Reduce 的方法计算，默认是 false: Map Reduce
     *
     * @throws Exception
     */
    public void canopyFuzzyKMeansRun() throws Exception {

        Path output = new Path("output");
        Path input = new Path("testdata");
        Configuration conf = new Configuration();
        HadoopUtil.delete(conf, new Path[]{output});
        Double convergenceDelta = 0.5D;
        DistanceMeasure measure = new ManhattanDistanceMeasure();
        int maxIterations = 10;
        double t1 = 2.0D;
        double t2 = 1.0D;
        float fuzziness = 2.0F;

        Path directoryContainingConvertedInput = new Path(output, "data");
        System.out.println("Preparing Input");
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
        System.out.println("Running Canopy to get initial clusters");
        Path canopyOutput = new Path(output, "canopies");
        CanopyDriver.run(new Configuration(), directoryContainingConvertedInput, canopyOutput, measure, t1, t2, false, 0.0D, false);
        System.out.println("Running KMeans");
        FuzzyKMeansDriver.run(directoryContainingConvertedInput, new Path(canopyOutput, "clusters-0-final"), output, convergenceDelta, maxIterations, fuzziness, false, true, 0.0D, false);
        Path outGlob = new Path(output, "clusters-*-final");
        Path clusteredPoints = new Path(output, "clusteredPoints");
        System.out.println("Dumping out clusters from clusters: " + outGlob + " and clusteredPoints:" + clusteredPoints);
        ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
        String[] args = {
                "-i", "output/clusters-*-final",
                "-o", "points",
                "-p", "output/clusteredPoints",
                "-of", "TEXT"
        };
        clusterDumper.run(args);
    }

    public void dumpTest() throws Exception {

        Path output = new Path("output");
        Path outGlob = new Path(output, "clusters-*-final");
        Path clusteredPoints = new Path(output, "clusteredPoints");
        ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
        String[] args = {
                "-i", "output/clusters-*-final",
                "-o", "points",
//                "-p","output/clusteredPoints",    // The directory containing points sequence files mapping input vectors to their cluster.
                "-of", "TEXT", // The optional output format. Options: TEXT, CSV, JSON or GRAPH_ML
//                "-sp", "3", // Specifies the maximum number of points to include _per_ cluster.
//                "-b", "100", // The number of chars of the asFormatString() to print.每个cluster标题行字数
//                "-n", "0",   // The number of top terms to print.无作用
//                "endPhase"
        };
        clusterDumper.run(args);
//        ClusterDumper.main(args);
    }

    public static void main(String[] args) throws Exception {
        Mahout clusterNode = new Mahout();
//        clusterNode.kmeansRun();
//        clusterNode.canopyRun();
        clusterNode.fuzzyKMeansRun();
//        clusterNode.canopyKMeansRun();
//        clusterNode.canopyFuzzyKMeansRun();
//        clusterNode.dumpTest();
    }
}
