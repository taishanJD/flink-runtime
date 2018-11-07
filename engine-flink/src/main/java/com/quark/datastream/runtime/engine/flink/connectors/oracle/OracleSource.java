package com.quark.datastream.runtime.engine.flink.connectors.oracle;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBCommonUtils;
import com.quark.datastream.runtime.engine.flink.connectors.oracle.common.OracleConnect;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class OracleSource extends RichSourceFunction<DataSet> {

    private final static Logger LOGGER = LoggerFactory.getLogger(OracleSource.class);


    private Map dbConfig = null;
    private OracleConnect dbConnect = null;
    private transient volatile boolean running;

    public OracleSource(Map dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            super.open(parameters);
            dbConnect = new OracleConnect(dbConfig);
            this.running = true;
            LOGGER.info("[oracle source] =======>  oracle source open!");
        } catch (ConnectException | SocketTimeoutException ce) {
            ce.printStackTrace();
            LOGGER.error("[oracle source] =======>  oracle connect failed! please check properties == {}", dbConfig);
            close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    @Override
    public void run(SourceContext<DataSet> ctx) throws Exception {

        String cron = dbConfig.get("period") == null ? null: dbConfig.get("period").toString();
        String sql = (String) dbConfig.get("sql");

        Gson gson = new Gson();
        while (this.running) {
            if (null != cron && "" != cron.trim()) { // 设置定时周期
                Map<String, Integer> periodMap = gson.fromJson(cron, new TypeToken<TreeMap<String, Integer>>() {
                }.getType());
                int hour = periodMap.get("hour");
                int min = periodMap.get("min");
                int sec = periodMap.get("sec");
                int period = hour * 60 * 60 + min * 60 + sec;

                Thread.sleep(period * 1000);

                DataSet out = DataSet.create();
                List<HashMap<String, Object>> resultMap = null;
                String parsedSql = parseSql(sql);
                resultMap = executeQuery(parsedSql);
                for (int i = 0; i < resultMap.size(); i++) {
                    DataSet.Record record = DataSet.Record.create(resultMap.get(i));
                    out.addRecord(record);
                }
                LOGGER.info("[oracle source] ======> OracleSource out time--->{}, out dataset--->{}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()), out);
                ctx.collect(out);
            } else { // 未设置定时周期
                String parsedSql = parseSql(sql);
                DataSet out = DataSet.create();
                List<HashMap<String, Object>> resultMap = null;
                resultMap = executeQuery(parsedSql);
                for (int i = 0; i < resultMap.size(); i++) {
                    DataSet.Record record = DataSet.Record.create(resultMap.get(i));
                    out.addRecord(record);
                }
                LOGGER.info("[oracle source] ======> OracleSource out--->{}", out);
                ctx.collect(out);
                this.running = false;
            }
        }
    }

    /**
     * 解析sql中的${}
     *
     * @param sql sql语句
     * @return
     * @throws SQLException
     */
    public String parseSql(String sql) {

        //oracle执行的sql语句最后不能有分号
        if (sql.trim().endsWith(";")){
            sql = sql.trim().substring(0,sql.trim().length()-1);
        }
        List<String> conditions = DBCommonUtils.getDateParams(sql);
        if (0 < conditions.size()){
            for (String condition : conditions) { // 计算日期
                String systime = DBCommonUtils.parseSelectDate(condition);
                sql = sql.replace("${" + condition + "}",  "to_date('" + systime+ "', 'yyyy-mm-dd hh24:mi:ss') ");
            }
            return sql;
        }
        return sql;
    }

    /**
     * 从数据库中查询数据
     *
     * @param sql sql语句
     * @return
     * @throws SQLException
     */
    public List<HashMap<String, Object>> executeQuery(String sql) {

        List<HashMap<String, Object>> list = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        if (null != dbConnect){
            try {
                list = new ArrayList<>();
                connection = dbConnect.getConnection();
                pstmt = connection.prepareStatement(sql);
                resultSet = pstmt.executeQuery();
                LOGGER.info("[oracle source] =======> execute sql == {}" ,sql);
                ResultSetMetaData metaData = resultSet.getMetaData();
                int cols_len = metaData.getColumnCount();
                while (resultSet.next()) {
                    HashMap<String, Object> map = new HashMap<String, Object>();
                    for (int i = 0; i < cols_len; i++) {
                        String cols_name = metaData.getColumnLabel(i + 1);
                        Object cols_value = resultSet.getObject(cols_name);
                        map.put(cols_name, cols_value);
                    }
                    list.add(map);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                dbConnect.release(resultSet,pstmt,connection);
            }
        }
        return list;
    }


    @Override
    public void cancel() {
        this.running = false;
        dbConnect.close();
        LOGGER.info("[oracle source] =======>  oracle source closed!");
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.running = false;
        dbConnect.close();
        LOGGER.info("[oracle source] =======>  oracle source closed!");
    }

    public static void main(String[] a) throws Exception {
        Map<String,Object> map1 = new HashMap<>();
        map1.put("ip","192.168.2.114");
        map1.put("port","1522");
        map1.put("database","orcl");
        map1.put("username","test");
        map1.put("password","test123456");
        map1.put("sql","select * from test_2 where systime < ${systime}");
        map1.put("period","{\"hour\":0,\"min\":0,\"sec\":5}");

//        OracleSource oracleSource = new OracleSource(map1);
//        oracleSource.open(null);
////        while (true){
////            oracleSource.run(null);
////        }
////        oracleSource.executeQuery("select * from test_2");
//
////        oracleSource.parseSql("select * from test_2");
//
//        String parsedSql = oracleSource.parseSql("select * from test_2 where systime < ${systime}");
//        DataSet out = DataSet.create();
//        List<HashMap<String, Object>> resultMap = null;
//        resultMap = oracleSource.executeQuery(parsedSql);
//        for (int i = 0; i < resultMap.size(); i++) {
////            DataSet.Record record = DataSet.Record.create(new Gson().toJson(resultMap.get(i)));
//            DataSet.Record record = DataSet.Record.create(resultMap.get(i));
//            out.addRecord(record);
//        }
//        System.out.println("[oracle source] ======> OracleSource out == "+out);
//        oracleSource.close();
//
//        map1.put("table","test_2");
//        OracleSink oracleSink = new OracleSink(map1);
//        oracleSink.open(null);
//        oracleSink.invoke(out);
//        oracleSink.close();

        String s = " select * from test_2;  ";
        if (s.trim().endsWith(";")){
            s = s.trim().substring(0,s.trim().length()-1);
        }
        System.out.println(s);
    }
}
