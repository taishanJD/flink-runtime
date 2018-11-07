package com.quark.datastream.runtime.engine.flink.connectors.mysql;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.quark.datastream.runtime.engine.flink.connectors.commom.db.DBCommonUtils;
import com.quark.datastream.runtime.engine.flink.connectors.mysql.common.MysqlConnect;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MysqlSource extends RichSourceFunction<DataSet> {

    private final static Logger LOGGER = LoggerFactory.getLogger(MysqlSource.class);


    private Map dbConfig = null;
    private MysqlConnect dbConnect = null;
    private transient volatile boolean running;

    public MysqlSource(Map dbConfig) {
        this.dbConfig = dbConfig;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            dbConnect = new MysqlConnect(dbConfig);
            this.running = true;
            LOGGER.info("[mysql source] =======>  mysql source open!");
        } catch (ConnectException | SocketTimeoutException ce) {
            ce.printStackTrace();
            LOGGER.error("[mysql source] =======>  please check connect properties == {}",dbConfig);
            cancel();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }

    }

    @Override
    public void run(SourceContext<DataSet> sourceContext) throws Exception {

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
                LOGGER.info("[mysql source] ======> MysqlSource out time--->{}, out dataset--->{}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()), out);
                sourceContext.collect(out);

            } else { // 未设置定时周期
                String parsedSql = parseSql(sql);
                DataSet out = DataSet.create();
                List<HashMap<String, Object>> resultMap = null;
                resultMap = executeQuery(parsedSql);
                for (int i = 0; i < resultMap.size(); i++) {
                    DataSet.Record record = DataSet.Record.create(resultMap.get(i));
                    out.addRecord(record);
                }
                LOGGER.info("[mysql source] ======> mysqlSource out--->{}", out);
                sourceContext.collect(out);
                this.running = false;
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.running = false;
        dbConnect.close();
        LOGGER.info("[mysql source] =======>  mysql source closed!");
    }

    /**
     * 解析sql中的${}
     *
     * @param sql sql语句
     * @return
     * @throws SQLException
     */
    public String parseSql(String sql) {

        //找sql中的${systime}条件
        List<String> conditions = DBCommonUtils.getDateParams(sql);
        if (0 < conditions.size()){
            for (String condition : conditions) { // 计算日期
                String systime = DBCommonUtils.parseSelectDate(condition);
                sql = sql.replace("${" + condition + "}", "'" + systime + "'");
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
                LOGGER.info("[mysql source] =======> execute sql == {}" ,sql);
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
        LOGGER.info("[mysql source] =======>  mysql source closed!");
    }
}
