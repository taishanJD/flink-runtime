package com.quark.datastream.runtime.engine.flink.connectors.commom.db;

import java.util.List;
import java.util.Map;

public interface DBOperate {

    List<Map<String, Object>> getTableStructure(String tableName);
}
