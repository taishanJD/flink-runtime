package com.quark.datastream.runtime.engine.flink.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.quark.datastream.runtime.common.workflow.*;
import com.quark.datastream.runtime.engine.flink.graph.vertex.FlatMapTaskVertex;
import com.quark.datastream.runtime.engine.flink.graph.vertex.SinkVertex;
import com.quark.datastream.runtime.engine.flink.graph.vertex.SourceVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JobGraphBuilder {

    private WorkflowData jobConfig;

    private Map<Vertex, List<Vertex>> edges;

    /**
     * Returns JobGraph objects that corresponds to the workflow
     * The env argument must specify Flink execution environment on the machine, and the workflowData
     * argument contains sources, sinks, processors and their connections.
     * This method returns JobGraph object only when the given workflow is valid. When the workflow is
     * not suitable for drawing the job graph from source to sink and at least one processor, this
     * method throws an exception.
     *
     * @param env          Flink execution environment
     * @param workflowData Workflow
     * @return JobGraph object that can initiate Flink job
     * @throws Exception If Flink execution environment is missing, source/sink/processor is missing,
     *                   or unreachable edge exists.
     */
    public JobGraph getInstance(StreamExecutionEnvironment env,
                                WorkflowData workflowData) throws Exception {

        if (env == null) {
            throw new NullPointerException("Invalid execution environment");
        }

        if (workflowData == null) {
            throw new NullPointerException("Null job configurations");
        }

        this.jobConfig = workflowData;
        initConfig(env);

        return new JobGraph(jobConfig.getWorkflowName(), edges);
    }

    private void initConfig(StreamExecutionEnvironment env) {
        if (isEmpty(jobConfig.getSources())) {
            throw new NullPointerException("Empty source information");
        }

        if (isEmpty(jobConfig.getSinks())) {
            throw new NullPointerException("Empty sink information");
        }

    /*if (isEmpty(jobConfig.getProcessors())) {
      throw new NullPointerException("Empty task information");
    }*/

        HashMap<Long, Vertex> map = new HashMap<>();
        for (WorkflowSource sourceInfo : jobConfig.getSources()) {
            SourceVertex source = new SourceVertex(env, sourceInfo);
            map.put(sourceInfo.getId(), source);
        }

        for (WorkflowSink sinkInfo : jobConfig.getSinks()) {
            SinkVertex sink = new SinkVertex(sinkInfo);
            map.put(sink.getId(), sink);
        }

        if (!isEmpty(jobConfig.getProcessors())) {
            for (WorkflowProcessor taskInfo : jobConfig.getProcessors()) {
                // TODO: 2018/07/25 扩展其他类型transformations （map、union、filter...），通过processors中task类型找到对应的transformation
                FlatMapTaskVertex task = new FlatMapTaskVertex(taskInfo);
                map.put(task.getId(), task);
            }
        }

        edges = new HashMap<>();
        for (WorkflowEdge edge : jobConfig.getEdges()) {
            Long from = edge.getFromId();
            Long to = edge.getToId();
            if (map.containsKey(from) && map.containsKey(to)) {
                Vertex fromVertex = map.get(from);
                Vertex toVertex = map.get(to);
                if (!edges.containsKey(fromVertex)) {
                    List<Vertex> toes = new ArrayList<>();
                    toes.add(toVertex);
                    edges.put(fromVertex, toes);
                } else {
                    edges.get(fromVertex).add(toVertex);
                }
            } else {
                throw new IllegalStateException("Unavailable vertex included when " + from + "->" + to);
            }
        }
    }

    private <T> boolean isEmpty(List<T> list) {
        return list == null || list.isEmpty();
    }
}
