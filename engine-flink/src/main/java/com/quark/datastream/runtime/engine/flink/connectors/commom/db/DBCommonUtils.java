package com.quark.datastream.runtime.engine.flink.connectors.commom.db;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBCommonUtils {

    /**
     * 解析db source节点，sql输入框中填入的内容
     * SQL语句中可以添加时间变量作为动态的参数，在执行SQL的时候去替换，变量格式为：
     * ${systime-1hour30min}
     * 或 ${systime}
     * 或 ${systime+10min}，分别对应当前系统时间减去1小时30分钟、当前系统时间、当前系统时间加上10分钟，系统时间精确到分钟，对应的秒为0。
     * 其中，小时最大值为24，分钟最大值为59。
     *
     * @param sql sql输入框中填入的内容,如："SELECT * FROM data_point where create_date > ${systime-1hour30min} and create_date < ${systime}"
     * @return list:["systime-1hour30min","systime"]
     */
    public static List<String> getDateParams(String sql){
        List<String> conditions = new ArrayList<>();
        Pattern pattern = Pattern.compile("(?<=\\{)(systime{1})(-|\\+)?(\\d+hour)?(\\d+min)?(?=\\})");
        Matcher matcher = pattern.matcher(sql);

        while (matcher.find()) {
            // 替换大括号内容
            conditions.add(matcher.group());
        }
        return conditions;
    }

    /**
     *
     * 本方法主要将${systime-1hour30min}转为具体的时间
     *
     * @param params 类似于systime-1hour30min, systime, systime+10min
     * @return yyyy-MM-dd HH:mm:ss格式的日期字符串
     */
    public static String parseSelectDate(String params) {

        if (null == params) {
            return null;
        }

        //去掉所有空白字符
        String noBlankParams = params.replaceAll("\\s*", "");
        int noBlankParamsLength = noBlankParams.length();

        if (0 == noBlankParamsLength) {
            return null;
        }

        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String systime = null;

        if ("systime".equals(noBlankParams)) {
            systime = sdf.format(now);
            return systime;
        }

        String endTime = noBlankParams.substring(8, noBlankParamsLength);
        int hour = 0;
        int min = 0;

        if (endTime.contains("hour")) {
            String hourStr = endTime.substring(0, endTime.indexOf("hour"));
            if (0 == hourStr.length()) {
                throw new RuntimeException("wrong sql params ! hour is null");
            }
            hour = Integer.parseInt(hourStr);
        }

        if (endTime.contains("min")) {
            String minStr = null;
            if (endTime.contains("hour")){
                minStr = endTime.substring(endTime.indexOf("hour") + 4, endTime.indexOf("min"));
            } else {
                minStr = endTime.substring(0, endTime.indexOf("min"));
            }

            if (0 == minStr.length()) {
                throw new RuntimeException("wrong sql params ! min is null");
            }
            min = Integer.parseInt(minStr);
        }


        Calendar calendar = Calendar.getInstance();
        calendar.setTime(now);
        if (noBlankParams.startsWith("systime-")) {
            calendar.add(Calendar.HOUR_OF_DAY, -hour);
            calendar.add(Calendar.MINUTE, -min);
        }

        if (noBlankParams.startsWith("systime+")) {
            calendar.add(Calendar.HOUR_OF_DAY, hour);
            calendar.add(Calendar.MINUTE, min);
        }

        systime = sdf.format(calendar.getTime());
        return systime;
    }

    public static void main(String[] a) {
//        System.out.println(parseSelectDate("systime"));
//        System.out.println(parseSelectDate("systime-1hour30min"));
//        System.out.println(parseSelectDate("systime+1hourmin"));

        Pattern pattern = Pattern.compile("(?<=\\{)(systime{1})(-|\\+)?(\\d+hour)?(\\d+min)?(?=\\})");
        Matcher matcher = pattern.matcher("SELECT * FROM data_point where create_date ");
        List<String> conditions = new ArrayList<>();
        while (matcher.find()) {
            // 替换大括号内容
            conditions.add(matcher.group());
        }
        System.out.println(matcher.find());
    }
}
