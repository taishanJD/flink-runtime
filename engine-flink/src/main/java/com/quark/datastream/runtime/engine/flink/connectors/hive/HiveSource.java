// Hive Source

package com.quark.datastream.runtime.engine.flink.connectors.hive;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.quark.datastream.runtime.engine.flink.connectors.hive.common.HiveConfig;
import com.quark.datastream.runtime.engine.flink.connectors.hive.common.HiveConnect;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveSource extends RichSourceFunction<DataSet> {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    /**
     * 预编译 SQL解析正则表达式
     */
    private static final Pattern PARSE_SQL_PATTERN = Pattern.compile("(?<=\\{)(.+?)(?=\\})");

    private Map hiveProperties;
    private HiveConnect hiveConnect = null;
    private transient volatile boolean running;

    public HiveSource(Map hiveProperties) {
        this.hiveProperties = hiveProperties;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            this.running = true;
            hiveConnect = new HiveConnect(hiveProperties);
            LOGGER.info("[HIVE SOURCE INFO] =======>  Hive source open!");
        } catch (ConnectException | SocketTimeoutException e) {
            e.printStackTrace();
            LOGGER.error("[HIVE SOURCE ERROR] =======> Connection properties == {}", hiveProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(SourceContext<DataSet> sourceContext) {
        String sql = String.valueOf(hiveProperties.get(HiveConfig.HIVE_SQL));
        Object periodObject = hiveProperties.get(HiveConfig.HIVE_PERIOD);

        String parsedSQL = parseSQL(sql);

        // 判断是否周期性执行
        if (periodObject == null) {
            DataSet out = queryForList(parsedSQL);
            sourceContext.collect(out);
            LOGGER.info("[HIVE SOURCE INFO] ======> Hive Source execution time--->{}", new Date());
        } else {
            String period = String.valueOf(periodObject);
            Map<String, Integer> periodMap = new Gson().fromJson(period, new TypeToken<TreeMap<String, Integer>>() {}.getType());
            int hour = periodMap.get("hour");
            int min = periodMap.get("min");
            int sec = periodMap.get("sec");
            int time = hour * 60 * 60 + min * 60 + sec;

            while (this.running) {
                DataSet out = queryForList(parsedSQL);
                sourceContext.collect(out);
                LOGGER.info("[HIVE SOURCE INFO] ======> Hive Source execution time--->{}", new Date());

                try {
                    Thread.sleep(time * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
        if (hiveConnect != null) {
            hiveConnect.close();
        }
        LOGGER.info("[HIVE SOURCE INFO] =======>  Hive source has canceled!");
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.running = false;
        if (hiveConnect != null) {
            hiveConnect.close();
        }
        LOGGER.info("[HIVE SOURCE INFO] =======>  Hive source has closed!");
    }

    /**
     * execution parsedSQL
     *
     * @return com.quark.datastream.runtime.task.DataSet
     */
    private DataSet queryForList(String parsedSql) {
        List<Map<String, Object>> resultMap = hiveConnect.getHiveJdbcTemplate().queryForList(parsedSql);
        DataSet out = DataSet.create();
        for (Map<String, Object> aResultMap : resultMap) {
            DataSet.Record record = DataSet.Record.create(new Gson().toJson(aResultMap));
            out.addRecord(record);
        }

        return out;
    }

    /**
     * 解析sql中的${}
     *
     * SQL语句中可以添加时间变量作为动态的参数，在执行SQL的时候去替换，
     * 变量格式为：${systime-1hour30min} 或 ${systime} 或 ${systime+10min}，分别对应当前系统时间减去1小时30分钟、当前系统时间、当前系统时间加上10分钟，
     * 系统时间精确到分钟，对应的秒为0。其中，小时最大值为24，分钟最大值为59。
     *
     * @param sql SQL语句
     *
     * @return SQL String
     */
    private static String parseSQL(String sql) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 格式化之后的时间字符串
        String systime;

        // 获取动态参数条件(花括号内容)
        List<String> conditions = new ArrayList<>();
        Matcher matcher = PARSE_SQL_PATTERN.matcher(sql);
        while (matcher.find()) {
            conditions.add(matcher.group());
        }

        int index = 0;
        for (String aCondition : conditions) {
            // 消除条件中的空格
            String condition = aCondition.replaceAll(" ", "");
            sql = sql.replace(aCondition, condition);

            // 获取动态参数条件在SQL字符串的起始位置（从0开始）
            index = sql.indexOf(condition, index);

            // 判断systime后面是否有条件
            if (condition.trim().length() > 7) {
                // 获取运算符
                String term = condition.substring(7, 8);
                // 获取运算时间
                String operationTime = condition.substring(8);
                // 获取Calendar实例（代表当前的系统时间）
                Calendar calendar = Calendar.getInstance();
                // 判断[小时]和[分钟]的标识符起始位置
                int ifHasHour = operationTime.indexOf("hour");
                int ifHasMin = operationTime.indexOf("min");

                // 时间运算
                if ("-".equals(term)) {
                    //相减
                    if (-1 != ifHasHour) {
                        String hour = operationTime.substring(0, ifHasHour);
                        calendar.add(Calendar.HOUR_OF_DAY, -Integer.parseInt(hour));
                        if (-1 != ifHasMin) {
                            String min = operationTime.substring(ifHasHour + 4, ifHasMin);
                            calendar.add(Calendar.MINUTE, -Integer.parseInt(min));
                        }
                    } else {
                        if (-1 != ifHasMin) {
                            String min = operationTime.replace("min", "");
                            calendar.add(Calendar.MINUTE, -Integer.parseInt(min));
                        }
                    }
                } else if ("+".equals(term)) {
                    //相加
                    if (-1 != ifHasHour) {
                        String hour = operationTime.substring(0, ifHasHour);
                        calendar.add(Calendar.HOUR_OF_DAY, Integer.parseInt(hour));
                        if (-1 != ifHasMin) {
                            String min = operationTime.substring(ifHasHour + 4, ifHasMin);
                            calendar.add(Calendar.MINUTE, Integer.parseInt(min));
                        }
                    } else {
                        if (-1 != ifHasMin) {
                            String min = operationTime.replace("min", "");
                            calendar.add(Calendar.MINUTE, Integer.parseInt(min));
                        }
                    }
                }

                systime = dateFormat.format(calendar.getTime());
            } else {
                systime = dateFormat.format(new Date());
            }

            sql = sql.replace("${" + condition + "}", "'" + systime + "'");
        }

        return sql;
    }
}
