package com.quark.datastream.runtime.engine.flink;

import com.google.gson.Gson;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.HashMap;

import com.quark.datastream.runtime.common.workflow.WorkflowData;
import com.quark.datastream.runtime.engine.flink.connectors.file.FileInputSource;
import com.quark.datastream.runtime.engine.flink.connectors.file.FileOutputSink;
import com.quark.datastream.runtime.engine.flink.graph.JobGraph;
import com.quark.datastream.runtime.engine.flink.graph.JobGraphBuilder;
import com.quark.datastream.runtime.engine.flink.task.FlatMapTask;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(Launcher.class);
  private StreamExecutionEnvironment env;

  public static void main(String[] args) throws Exception {
    Launcher launcher = new Launcher();
    launcher.execute(args);
  }

  private void execute(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);

    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.disableOperatorChaining();
    env.getConfig().setGlobalJobParameters(params);

    if (!params.has("json")) {
      throw new RuntimeException("Not any specified job-config file");
    }

    String jsonId = params.get("json");
    Reader jsonReader = null;
    InputStream stream = null;
    try {
      if (params.has("internal")) {
        String inJarPath = "/" + jsonId + ".json";
        LOGGER.debug("{}", getClass().getResource(inJarPath));
        stream = getClass().getResourceAsStream(inJarPath);
        jsonReader = new InputStreamReader(stream, Charset.defaultCharset());
      } else {
        jsonReader = new InputStreamReader(new FileInputStream(jsonId), Charset.defaultCharset());
      }

      WorkflowData workflowData = new Gson().fromJson(jsonReader, WorkflowData.class);

      JobGraph jobGraph = new JobGraphBuilder().getInstance(env, workflowData);
      jobGraph.initialize();

      env.execute(jobGraph.getJobId());

    } catch (NullPointerException e) {
      throw new RuntimeException("Invalid job configuration file", e);
    } finally {
      if (stream != null) {
        stream.close();
      }
      if (jsonReader != null) {
        jsonReader.close();
      }
    }
  }


  private void execute2(String[] args){
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.disableOperatorChaining();

    DataStreamSource<DataSet> A = env.addSource(new FileInputSource("", ""));
    A.name("A");
    SingleOutputStreamOperator<DataSet> B = A.flatMap(new FlatMapTask(new HashMap<>()));
    B.name("B");
    SingleOutputStreamOperator<DataSet> C = B.flatMap(new FlatMapTask(new HashMap<>()));
    C.name("C");
    DataStreamSink<DataSet> D = C.addSink(new FileOutputSink(""));
    D.name("D");

    DataStreamSource<DataSet> E = env.addSource(new FileInputSource("", ""));
    E.name("E");

    DataStream<DataSet> F = B.union(E).flatMap(new FlatMapTask(new HashMap<>()));
    ((SingleOutputStreamOperator<DataSet>) F).name("F");

    DataStreamSink<DataSet> G = F.addSink(new FileOutputSink(""));
    G.name("G");

    try {
      env.execute("test DAG");
    } catch (Exception e) {
      e.printStackTrace();
    }

  }



}



