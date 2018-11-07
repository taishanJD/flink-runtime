package com.quark.datastream.runtime.engine.flink.schema;

import java.nio.charset.Charset;

import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class DataSetSchema implements SerializationSchema<DataSet>, DeserializationSchema<DataSet> {

  @Override
  public DataSet deserialize(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    } else {
      return DataSet.create(new String(bytes, Charset.defaultCharset()));
    }
  }

  @Override
  public boolean isEndOfStream(DataSet dataSet) {
    return false;
  }

  @Override
  public TypeInformation<DataSet> getProducedType() {
    return TypeExtractor.getForClass(DataSet.class);
  }

  @Override
  public byte[] serialize(DataSet dataSet) {
    if (dataSet == null) {
      throw new IllegalStateException("DataSet is null.");
    }
    String str = dataSet.toString();
    // Currently, there is no case when DataSet.toString returns null or emtpy string
    // If there is a case, uncomment these out
    //if (str == null || str.isEmpty()) {
    //  throw new IllegalStateException("DataSet in string is null.");
    //} else {
    return str.getBytes(Charset.defaultCharset());
    //}
  }
}
