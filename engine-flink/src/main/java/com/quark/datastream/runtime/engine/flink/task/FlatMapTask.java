package com.quark.datastream.runtime.engine.flink.task;

import java.util.List;
import java.util.Map;

import com.quark.datastream.runtime.task.DataSet;
import com.quark.datastream.runtime.task.TaskNode;
import com.quark.datastream.runtime.task.TaskNodeParam;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatMapTask extends RichFlatMapFunction<DataSet, DataSet> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlatMapTask.class);

  private TaskNode task;

  private Map<String, Object> properties;

  public FlatMapTask(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // Create task using TaskFactory which is made up by factory pattern.
    if (!properties.containsKey("jar") || !properties.containsKey("className")) {
      throw new IllegalStateException("Unable to load class for flatMap function");
    }
    String jarPath = (String) properties.get("jar");
    String targetClass = (String) properties.get("className");
    ClassLoader classLoader = getRuntimeContext().getUserCodeClassLoader();

    task = new NodeLoader(jarPath, classLoader).newInstance(targetClass);
    if (task == null) {
      throw new Exception("Failed to create a new instance of " + jarPath);
    }

    TaskNodeParam taskModelParam = makeTaskNodeParam();

    task.setParam(taskModelParam);
    task.setInRecordKeys((List<String>) properties.get("inrecord"));
    task.setOutRecordKeys((List<String>) properties.get("outrecord"));

    LOGGER.debug("{} is loaded as a FlatMap task ", this.task.getName());
  }

  private TaskNodeParam makeTaskNodeParam() {
    TaskNodeParam nestedParams = new TaskNodeParam();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      createTaskNodeParam(nestedParams, entry.getKey(), entry.getValue());
    }
    return nestedParams;
  }

  private void createTaskNodeParam(TaskNodeParam parent, String key, Object value) {
    String[] tokens = key.split("/", 2);
    if (tokens.length == 1) { // terminal case
      parent.put(key, value);
      return;
    }
    // recurse
    TaskNodeParam child = new TaskNodeParam();
    parent.put(tokens[0], child);
    createTaskNodeParam(child, tokens[1], value);
  }

  @Override
  public void flatMap(DataSet dataSet, Collector<DataSet> collector) throws Exception {
    dataSet = task.calculate(dataSet);
    if (dataSet != null) {
      collector.collect(dataSet);
    }
  }
}
