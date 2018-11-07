package com.quark.datastream.runtime.engine.flink.connectors.oracle;

import com.quark.datastream.runtime.engine.flink.connectors.oracle.common.OracleConnect;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OracleSink extends RichSinkFunction<DataSet> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSink.class);

    private Map dbConfig = null;
    private OracleConnect dbConnect = null;

    public OracleSink(Map dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            dbConnect = new OracleConnect(dbConfig);
            LOGGER.info("[oracle sink] =======>  oracle sink open!");
        } catch (ConnectException | SocketTimeoutException ce) {
            ce.printStackTrace();
            LOGGER.error("[oracle sink] =======>  oracle connect failed! please check properties == {}", dbConfig);
            close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dbConnect.close();
        }
        LOGGER.info("[oracle sink] =======>  oracle sink closed!");
    }

    @Override
    public void invoke(DataSet dataSet) {

        String tableName = (String) dbConfig.get("table");

        //查字段列表
        List<Map<String, Object>> columnList = dbConnect.getTableStructure(tableName);

        //字段名列表
        List<String> columnNames = new ArrayList<>();

        for (Map<String, Object> column : columnList) {
            String columnName = (String) column.get("columnName");
            columnNames.add(columnName);
        }

        List<DataSet.Record> records = dataSet.getRecords();
        for (int i = 0; i < records.size(); i++) {
            Connection connection = null;
            PreparedStatement pstmt = null;
            try {
                DataSet.Record record = records.get(i);
                if (null == record || record.isEmpty()) {
                    continue;
                }

                Set<String> keySet = record.keySet();
                if (keySet.isEmpty()) {
                    continue;
                }

                //fileds是能够插入到数据库表中的字段集合，格式：a,b,c
                String fields = convertParams(keySet, columnNames);
                if (null == fields || 0 == fields.length()) {
                    continue;
                }

                //从连接池里拿一个连接
                connection = dbConnect.getConnection();
                //拼sql
                StringBuffer sql = new StringBuffer();
                sql.append("insert into ");
                sql.append(tableName.toUpperCase());
                sql.append("(");
                sql.append(fields);
                sql.append(") values(");

                String[] usedfileds = fields.split(",");
                for (String key : usedfileds) {
                    Object val = record.get(key);
                    if (val instanceof String) {
                        sql.append("'");
                        sql.append(val);
                        sql.append("',");
                    } else if (val instanceof Timestamp){
                        sql.append("to_date('");
                        sql.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(val));
                        sql.append("','yyyy-mm-dd hh24:mi:ss')");
                        sql.append(",");
                    } else {
                        sql.append(val);
                        sql.append(",");
                    }
                }
                sql.deleteCharAt(sql.length() - 1);
                sql.append(")");
                pstmt = connection.prepareStatement(sql.toString());
                pstmt.executeUpdate();
                LOGGER.info("[oracle sink] ======>  execute sql == {}", sql);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                //释放连接
                dbConnect.release(null, pstmt, connection);
            }
        }
    }


    /**
     * keySet里的值转为sql语句需要的逗号隔开的格式，如：id，name，password
     * 转换成的sql语句里的字段以表中的字段为准，如：record想插入数据库的字段有a,b,c,但数据库表字段中有b,c,d，则实际插入b,c
     *
     * @param keySet
     * @return
     */
    private static String convertParams(Set<String> keySet, List<String> columnNames) {

        StringBuffer res = new StringBuffer();
        for (String key : keySet) {
            if (columnNames.contains(key.toUpperCase())) {
                res.append(key);
                res.append(",");
            }
        }
        if (0 < res.length()) {
            res.deleteCharAt(res.length() - 1);
        }
        return res.toString();
    }
}
