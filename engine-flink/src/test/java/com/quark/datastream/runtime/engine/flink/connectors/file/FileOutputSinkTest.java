
package com.quark.datastream.runtime.engine.flink.connectors.file;

import java.io.File;
import java.util.UUID;

import com.quark.datastream.runtime.task.DataSet;
import org.junit.Test;

public class FileOutputSinkTest {

  @Test
  public void testPayloadStreamData() throws Exception {
    File tempFile = makeTempFile();
    FileOutputSink sink = new FileOutputSink(tempFile.getPath());
    System.out.println("Writing to " + tempFile.getPath());
    try {
      DataSet streamData = DataSet.create("{}");
      for (int i = 0; i < 5; i++) {
        streamData.put("/i" + i, String.valueOf(i * i));
      }
      sink.invoke(streamData);
    } finally {
      sink.close();
      tempFile.deleteOnExit();
    }
  }

  @Test
  public void testSimpleStreamData() throws Exception {
    File tempFile = makeTempFile();
    FileOutputSink sink = new FileOutputSink(tempFile.getPath());
    System.out.println("Writing to " + tempFile.getPath());
    try {
      sink.invoke(DataSet.create("{}"));
    } finally {
      sink.close();
      tempFile.deleteOnExit();
    }
  }

  private File makeTempFile() {
    String property = "java.io.tmpdir";
    String tempDir = System.getProperty(property);
    System.out.println("Temp dir: " + tempDir);
    File temp = new File(tempDir, UUID.randomUUID().toString());
    return temp;
  }
}
