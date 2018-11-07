package com.quark.datastream.runtime.engine.flink.connectors.hive;

import com.quark.datastream.runtime.engine.flink.connectors.hive.common.HiveConfig;
import com.quark.datastream.runtime.engine.flink.connectors.hive.common.HiveConnect;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.*;

public class HiveSink extends RichSinkFunction<DataSet> {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    /**
     * 本地缓存文件路径
     */
    private final String LOCAL_FILE_DIRECTORY = "/tmp/hivecache/";

    private Map hiveProperties;
    private HiveConnect hiveConnect = null;
    private String uploadDataFileName;

    public HiveSink(Map hiveProperties) {
        this.hiveProperties = hiveProperties;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            hiveConnect = new HiveConnect(hiveProperties);
            LOGGER.info("[HIVE SINK INFO] =======>  Hive sink open!");
        } catch (ConnectException | SocketTimeoutException ce) {
            ce.printStackTrace();
            close();
            LOGGER.error("[HIVE SINK ERROR] =======> Connection properties == {}", hiveProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(DataSet dataSet) throws Exception {
        List<DataSet.Record> records = dataSet.getRecords();

        // 数据集内容
        StringBuffer content = new StringBuffer();
        // 文件名
        uploadDataFileName = UUID.randomUUID().toString().replaceAll("-", "") + "-" + System.currentTimeMillis();
        // 创建本地文件
        File dir = new File(LOCAL_FILE_DIRECTORY);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        String localFile = LOCAL_FILE_DIRECTORY + uploadDataFileName;
        if (new File(localFile).createNewFile()) {
            LOGGER.info("[HIVE SINK INFO] =======> Create local file cache == {}", localFile);
        }

        // 写入本地文件
        for (int count = 0; count < records.size() ;count++) {
            content.append(records.get(count)).append(System.getProperty("line.separator"));
            if (count % 1000 == 0 || count == records.size() - 1) {
                convertToFile(localFile, content);
                content.setLength(0);
            }
        }

        // 确定目标路径并上传本地文件到HDFS文件系统
        String targetPath = getHDFSPath();
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        FileInputStream in = new FileInputStream(new File(localFile));
        FileSystem fs = FileSystem.get(URI.create(targetPath), config, String.valueOf(hiveProperties.get(HiveConfig.HIVE_USERNAME)));
        OutputStream out = fs.create(new Path(targetPath));
        IOUtils.copyBytes(in, out, 4096, true);

        // 加载HDFS文件到Hive表
        try {
            hiveConnect.getHiveJdbcTemplate().execute(getLoadDataSQL());
        } finally {
            // 删除本地缓存文件
            new File(localFile).delete();
        }
    }

    @Override
    public void close() {
        if (hiveConnect != null) {
            hiveConnect.close();
        }
        LOGGER.info("[HIVE SINK INFO] =======>  Hive sink closed!");
    }

    /**
     * 存储DataSet为文件
     */
    private static void convertToFile(String outFileLocation, StringBuffer content) {
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(outFileLocation, true);
            fileWriter.write(content.toString());
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 返回上传到HDFS的文件路径
     *
     * @return
     *         HDFS PATH
     */
    private String getHDFSPath() {
        // Hive数据库文件名称HDFS相对路径
        String databasePath = HiveConfig.HIVE_DEFAULT_DB.equals(hiveProperties.get(HiveConfig.HIVE_DATABASE)) ?
                "" : hiveProperties.get(HiveConfig.HIVE_DATABASE) + "/";

        return new StringBuilder().append("hdfs://")
                                  .append(String.valueOf(hiveProperties.get(HiveConfig.HIVE_IP)))
                                  .append(":9000/home/hadoop/hivedata/")
                                  .append(databasePath)
                                  .append(uploadDataFileName).toString();
    }

    /**
     * 返回加载数据SQL
     *
     * @return
     *        HQL
     */
    private String getLoadDataSQL() {
        // Hive数据库文件名称HDFS相对路径
        String databasePath = HiveConfig.HIVE_DEFAULT_DB.equals(hiveProperties.get(HiveConfig.HIVE_DATABASE)) ?
                "" : hiveProperties.get(HiveConfig.HIVE_DATABASE) + "/";

        return new StringBuilder().append("LOAD DATA INPATH '").append("/home/hadoop/hivedata/")
                                  .append(databasePath)
                                  .append(uploadDataFileName)
                                  .append("' INTO TABLE ")
                                  .append(String.valueOf(hiveProperties.get(HiveConfig.HIVE_TABLE))).toString();
    }
}
