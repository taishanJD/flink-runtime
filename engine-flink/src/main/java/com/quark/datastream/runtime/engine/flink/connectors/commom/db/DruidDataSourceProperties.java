package com.quark.datastream.runtime.engine.flink.connectors.commom.db;

public class DruidDataSourceProperties {

    //Druid连接池，常用配置项
    public static final String PROP_URL = "url";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_USERNAME = "username";
    public static final String PROP_DRIVERCLASSNAME = "driverClassName";

    //最大连接数
    public static final String PROP_MAXACTIVE = "maxActive";
    //最大空闲连接(Druid连接池已不再使用，配置了也没有效果)
    public static final String PROP_MAXIDLE = "maxIdle";
    //最小空闲连接
    public static final String PROP_MINIDLE = "minIdle";
    //初始化连接量
    public static final String PROP_INITIALSIZE = "initialSize";
    //超时等待时间
    public static final String NUPROP_MAXWAIT = "maxWait";
    //JDBC驱动建立连接时附带的连接属性,格式必须为这样：[属性名=property;]，不需要包含username和password两个属性
    public static final String PROP_CONNECTIONPROPERTIES = "connectionProperties";
    //指定由连接池所创建的连接的自动提交（auto-commit）状态
    public static final String PROP_DEFAULTAUTOCOMMIT = "defaultAutoCommit";
}
