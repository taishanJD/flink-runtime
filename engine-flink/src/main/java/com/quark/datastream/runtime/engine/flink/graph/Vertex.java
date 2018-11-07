package com.quark.datastream.runtime.engine.flink.graph;

import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface Vertex {

  Long getId();

  DataStream<DataSet> serve() throws Exception;

  void setInflux(DataStream<DataSet> influx);
}
