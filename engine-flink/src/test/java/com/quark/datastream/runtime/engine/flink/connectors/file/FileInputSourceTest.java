package com.quark.datastream.runtime.engine.flink.connectors.file;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.PrintWriter;
import java.util.UUID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

public class FileInputSourceTest {

  String keyCsvStr = "a,b,c";
  String csvStr = " 10, 20, 30 ";
  String keyTsvStr = "a\tb\tc";
  String tsvStr = " 10\t20\t30 ";
  String errStr = "a b c d e f";

  @Test
  public void testCSVRun() throws Exception {
    String type = "csv";
    File tempFile = makeTempFile(type);
    PrintWriter writer = new PrintWriter(tempFile);
    for (int iter = 0; iter < 10; iter++) {
      writer.println(csvStr.toString());
    }
    writer.flush();
    writer.close();

    SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
    FileInputSource source = new FileInputSource(tempFile.getPath(), type);
    try {
      source.open(new Configuration());
      source.run(sourceContext);
    } finally {
      source.cancel();
      source.close();
      tempFile.deleteOnExit();
    }
  }

  @Test
  public void testTSVRun() throws Exception {
    String type = "tsv";
    File tempFile = makeTempFile(type);
    PrintWriter writer = new PrintWriter(tempFile);
    for (int iter = 0; iter < 10; iter++) {
      writer.println(tsvStr.toString());
    }
    writer.flush();
    writer.close();

    SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
    FileInputSource source = new FileInputSource(tempFile.getPath(), type);
    try {
      source.open(new Configuration());
      source.run(sourceContext);
    } finally {
      source.cancel();
      source.close();
      tempFile.deleteOnExit();
    }
  }

  @Test
  public void testCSVWithKeyRun() throws Exception {
    String type = "csv";
    File tempFile = makeTempFile(type);
    PrintWriter writer = new PrintWriter(tempFile);

    writer.println(keyCsvStr.toString());
    for (int iter = 0; iter < 10; iter++) {
      writer.println(csvStr.toString());
    }
    writer.flush();
    writer.close();

    SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
    FileInputSource source = new FileInputSource(tempFile.getPath(), type);
    source.readFirstLineAsKeyValues(true);
    try {
      source.open(new Configuration());
      source.run(sourceContext);
    } finally {
      source.cancel();
      source.close();
      tempFile.deleteOnExit();
    }
  }

  @Test
  public void testTSVWithKeyRun() throws Exception {
    String type = "tsv";
    File tempFile = makeTempFile(type);
    PrintWriter writer = new PrintWriter(tempFile);

    writer.println(keyTsvStr.toString());
    for (int iter = 0; iter < 10; iter++) {
      writer.println(tsvStr.toString());
    }
    writer.flush();
    writer.close();

    SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
    FileInputSource source = new FileInputSource(tempFile.getPath(), type);
    source.readFirstLineAsKeyValues(true);
    try {
      source.open(new Configuration());
      source.run(sourceContext);
    } finally {
      source.cancel();
      source.close();
      tempFile.deleteOnExit();
    }
  }

  @Test
  public void testCSVERRVRun() throws Exception {
    String type = "csv";
    File tempFile = makeTempFile(type);
    PrintWriter writer = new PrintWriter(tempFile);
    for (int iter = 0; iter < 10; iter++) {
      writer.println(errStr.toString());
    }
    writer.flush();
    writer.close();

    SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
    FileInputSource source = new FileInputSource(tempFile.getPath(), type);
    try {
      source.open(new Configuration());
      source.run(sourceContext);
    } finally {
      source.cancel();
      source.close();
      tempFile.deleteOnExit();
    }
  }

  @Test
  public void testTSVERRVRun() throws Exception {
    String type = "tsv";
    File tempFile = makeTempFile(type);
    PrintWriter writer = new PrintWriter(tempFile);
    for (int iter = 0; iter < 10; iter++) {
      writer.println(errStr.toString());
    }
    writer.flush();
    writer.close();

    SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
    FileInputSource source = new FileInputSource(tempFile.getPath(), type);
    try {
      source.open(new Configuration());
      source.run(sourceContext);
    } finally {
      source.cancel();
      source.close();
      tempFile.deleteOnExit();
    }
  }

  @Test
  public void testERRVRun() throws Exception {
    String type = "aaa";
    File tempFile = makeTempFile(type);
    PrintWriter writer = new PrintWriter(tempFile);
    for (int iter = 0; iter < 10; iter++) {
      writer.println(errStr.toString());
    }
    writer.flush();
    writer.close();

    SourceFunction.SourceContext sourceContext = mock(SourceFunction.SourceContext.class);
    FileInputSource source = new FileInputSource(tempFile.getPath(), type);
    try {
      source.open(new Configuration());
      source.run(sourceContext);
    } finally {
      source.cancel();
      source.close();
      tempFile.deleteOnExit();
    }
  }

  private File makeTempFile(String type) {
    String property = "java.io.tmpdir";
    String tempDir = System.getProperty(property);
    File temp = new File(tempDir,
        UUID.randomUUID().toString() + "." + type);
    System.out.println("Temp File: " + temp.getPath());
    return temp;
  }
}
