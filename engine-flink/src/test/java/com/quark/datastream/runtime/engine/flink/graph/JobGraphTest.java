package com.quark.datastream.runtime.engine.flink.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JobGraphTest {

  private static final String ID = "testId";
  private static long count;

  @Before
  public void resetCount() {
    count = 1;
  }

  @Test
  public void testGetJobId() {
    JobGraph jobGraph = new JobGraph(ID, null);
    Assert.assertEquals(jobGraph.getJobId(), ID);
  }

  @Test(expected = IllegalStateException.class)
  public void testInitializeWithCycle() throws Exception {
    Map<Vertex, List<Vertex>> edges = new HashMap<>();
    List<Vertex> vertices = new ArrayList<>();
    vertices.add(new TestVertex(count++));
    vertices.add(new TestVertex(count++));
    vertices.add(new TestVertex(count++));

    List<Vertex> list = new ArrayList<>();
    list.add(vertices.get(1));
    edges.put(vertices.get(0), list);

    list = new ArrayList<>();
    list.add(vertices.get(2));
    edges.put(vertices.get(1), list);

    list = new ArrayList<>();
    list.add(vertices.get(0));
    edges.put(vertices.get(2), list);

    new JobGraph(ID, edges).initialize();
  }

  @Test
  public void testInitExecution() throws Exception {
    Map<Vertex, List<Vertex>> edges = new HashMap<>();
    List<Vertex> vertices = new ArrayList<>();
    vertices.add(new TestVertex(count++));
    vertices.add(new TestVertex(count++));
    vertices.add(new TestVertex(count++));

    List<Vertex> list = new ArrayList<>();
    list.add(vertices.get(1));
    edges.put(vertices.get(0), list);

    list = new ArrayList<>();
    list.add(vertices.get(2));
    edges.put(vertices.get(1), list);

    JobGraph jobGraph = new JobGraph(ID, edges);
    jobGraph.initialize();
    jobGraph.initExecution();
  }

  @Test
  public void testInitExecution2() throws Exception {

    Vertex A = new TestVertex(1l);
    Vertex B = new TestVertex(2l);
    Vertex C = new TestVertex(3l);
    Vertex D = new TestVertex(4l);
    Vertex E = new TestVertex(5l);
    Vertex F = new TestVertex(6l);
    Vertex G = new TestVertex(7l);

    Map<Vertex, List<Vertex>> edges = new HashMap<>();
    List<Vertex> A_l = new ArrayList<>();
    A_l.add(B);
    edges.put(A, A_l);

    List<Vertex> B_l = new ArrayList<>();
    B_l.add(C);
    B_l.add(F);
    edges.put(B, B_l);

    List<Vertex> C_l = new ArrayList<>();
    C_l.add(D);
    edges.put(C, C_l);

    List<Vertex> E_l = new ArrayList<>();
    E_l.add(F);
    edges.put(E, E_l);

    List<Vertex> F_l = new ArrayList<>();
    F_l.add(G);
    edges.put(F, F_l);

    JobGraph jobGraph = new JobGraph(ID, edges);
    jobGraph.initialize();

    jobGraph.initExecution();
  }



  private final class TestVertex implements Vertex {

    private Long id;
    private DataStream<DataSet> data = Mockito.mock(DataStream.class);

    public TestVertex(Long id) {
      this.id = id;
    }

    @Override
    public Long getId() {
      return id;
    }

    @Override
    public DataStream<DataSet> serve() throws Exception {
      return data;
    }

    @Override
    public void setInflux(DataStream<DataSet> influx) {
      return;
    }
  }



}
