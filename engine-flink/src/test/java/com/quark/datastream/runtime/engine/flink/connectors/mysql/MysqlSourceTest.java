package com.quark.datastream.runtime.engine.flink.connectors.mysql;

import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MysqlSink.class})
public class MysqlSourceTest {

//    @Test
//    public void testConstructor() {
//
//        MysqlConnectionConfig.Builder builder = new MysqlConnectionConfig.Builder();
//        builder.setIp("localhost").setPort(3306).setDatabase("oneiot").setUsername("root").setPassword("root");
//        MysqlConnectionConfig config = builder.build();
//
//        new MysqlSource(config, "3", "SELECT * FROM data_point where create_date > ${systime-1hour30min}");
//    }
//
//    @Test
//    public void testConnection() throws Exception {
//
//        MysqlConnectionConfig.Builder builder = new MysqlConnectionConfig.Builder();
//        builder.setIp("localhost").setPort(3306).setDatabase("oneiot").setUsername("root").setPassword("root");
//        MysqlConnectionConfig config = builder.build();
//
//        MysqlSource mysqlSource = new MysqlSource(config, "3", "SELECT * FROM data_point where create_date > ${systime-1hour30min}");
//        mysqlSource.open(null);
//        mysqlSource.cancel();
//        mysqlSource.close();
//    }
//
//    @Test
//    public void testParseSql() throws Exception {
//
//        MysqlConnectionConfig.Builder builder = new MysqlConnectionConfig.Builder();
//        builder.setIp("localhost").setPort(3306).setDatabase("oneiot").setUsername("root").setPassword("root");
//        MysqlConnectionConfig config = builder.build();
//
//        String sql = "SELECT * FROM data_point where create_date > ${systime-1hour30min} and create_date < ${system}";
//
//        MysqlSource mysqlSource = new MysqlSource(config, "3", sql);
//
//        String parsedSql = mysqlSource.parseSql(sql);
//        System.out.println("result sql is ---->" + parsedSql);
//    }
//
//    @Test
//    public void testExecuteQuery() throws Exception {
//
//        MysqlConnectionConfig.Builder builder = new MysqlConnectionConfig.Builder();
//        builder.setIp("localhost").setPort(3306).setDatabase("oneiot").setUsername("root").setPassword("root");
//        MysqlConnectionConfig config = builder.build();
//
//        String sql = "SELECT * FROM data_point where create_date > '2018-08-17 14:51:13' and create_date < '2018-08-17 16:21:13'";
//
//        MysqlSource mysqlSource = new MysqlSource(config, "3", sql);
//
//        mysqlSource.open(null);
//        List<Map<String, Object>> resultMap = mysqlSource.executeQuery(sql);
//        System.out.print(resultMap);
//    }
//
//    @Test(timeout = 59000L)
////    @Test
//    public void testRun() throws Exception { // 周期执行
//
//        MysqlConnectionConfig.Builder builder = new MysqlConnectionConfig.Builder();
//        builder.setIp("localhost").setPort(3306).setDatabase("oneiot").setUsername("root").setPassword("root");
//        MysqlConnectionConfig config = builder.build();
//
//        Gson gson = new Gson();
//        Map<String, Integer> periodMap = new HashMap<>();
//        periodMap.put("hour", 0);
//        periodMap.put("min", 0);
//        periodMap.put("sec", 3);
//        String cron = gson.toJson(periodMap);
//
//        String sql = "SELECT * FROM data_point where create_date > ${systime-1hour30min} and create_date < ${system}";
//
//        MysqlSource mysqlSource = new MysqlSource(config, cron, sql);
//
//        mysqlSource.open(null);
//        SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
//        mysqlSource.run(sourceContext);
//    }
//
////    @Test(timeout = 12000L)
//    @Test
//    public void testRun2() throws Exception { // 单次执行
//
//        MysqlConnectionConfig.Builder builder = new MysqlConnectionConfig.Builder();
//        builder.setIp("localhost").setPort(3306).setDatabase("oneiot").setUsername("root").setPassword("root");
//        MysqlConnectionConfig config = builder.build();
//
//        String cron = "";
//
//        String sql = "SELECT * FROM data_point where create_date > ${systime-1hour30min} and create_date < ${system}";
//
//        MysqlSource mysqlSource = new MysqlSource(config, cron, sql);
//
//        mysqlSource.open(null);
//        SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
//        mysqlSource.run(sourceContext);
//    }

}
