package com.quark.datastream.runtime.engine.flink.connectors.file;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class FileOutputSink extends RichSinkFunction<DataSet> {

  private static final long serialVersionUID = 1L;

  private File outputFile = null;

  private transient PrintWriter writer = null;

  public FileOutputSink(String outputFilePath) {
    this.outputFile = new File(outputFilePath);
  }

  @Override
  public void invoke(DataSet dataSet) throws Exception {
    if (this.writer == null) {
      this.writer = new PrintWriter(this.outputFile, Charset.defaultCharset().name());
    }

    this.writer.println(dataSet.toString());
    this.writer.flush();
  }

  @Override
  public void close() throws Exception {
    if (this.writer != null) {
      this.writer.close();
    }
    super.close();
  }
}
