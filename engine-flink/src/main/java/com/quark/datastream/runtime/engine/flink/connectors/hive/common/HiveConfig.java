package com.quark.datastream.runtime.engine.flink.connectors.hive.common;
/**
 * Hive Connection Configuration Names
 */
public class HiveConfig {

    public static final String HIVE_DEFAULT_DB =  "default";

    /**
     * Hive server config
     */
    public static final String HIVE_IP         =  "ip";
    public static final String HIVE_PORT       =  "port";
    public static final String HIVE_USERNAME   =  "username";
    public static final String HIVE_PASSWORD   =  "password";
    public static final String HIVE_DATABASE   =  "database";

    /**
     * Hive MySQL Metadata Config
     */
    public static final String HIVE_METADATA_IP         =   "metadataIp";
    public static final String HIVE_METADATA_PORT       =   "metadataPort";
    public static final String HIVE_METADATA_USERNAME   =   "metadataUsername";
    public static final String HIVE_METADATA_PASSWORD   =   "metadataPassword";
    public static final String HIVE_METADATA_DATABASE   =   "metadataDatabase";

    /**
     * Hive source config
     */
    public static final String HIVE_SQL     =  "sql";
    public static final String HIVE_PERIOD  =  "period";

    /**
     * Hive sink config
     */
    public static final String HIVE_TABLE  =  "table";
}
