
package com.quark.datastream.runtime.engine.flink.schema;

import java.io.IOException;

import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Assert;
import org.junit.Test;

public class DataSetSchemeTest {

  @Test
  public void testSerialize() {
    DataSet dataSet = DataSet.create("testId", "{}");
    DataSetSchema schema = new DataSetSchema();
    byte[] b = schema.serialize(dataSet);
    Assert.assertNotNull(b);
    Assert.assertTrue(b.length > 0);
  }

  @Test
  public void testDeserialize() throws IOException {
    DataSet dataSet = DataSet.create("testId", "{}");
    DataSetSchema schema = new DataSetSchema();
    byte[] b = schema.serialize(dataSet);
    Assert.assertNotNull(b);
    Assert.assertTrue(b.length > 0);

    DataSet newDataSet = schema.deserialize(b);
    Assert.assertNotNull(newDataSet);
    Assert.assertEquals(newDataSet.toString().length(),
        dataSet.toString().length());
    Assert.assertEquals(newDataSet.values().size(), dataSet.values().size());
  }

  @Test
  public void testTypeInformation() {
    DataSetSchema schema = new DataSetSchema();
    TypeInformation<DataSet> producedType = schema.getProducedType();
    Assert.assertNotNull(producedType);
    Assert.assertEquals(producedType.getTypeClass(), DataSet.class);
  }

  @Test
  public void testIsEndOfStream() {
    DataSet streamData = DataSet.create("testId", "{}");
    DataSetSchema schema = new DataSetSchema();
    Assert.assertFalse(schema.isEndOfStream(streamData));
  }

  @Test
  public void testDeserializeFail() {
    DataSetSchema schema = new DataSetSchema();
    Assert.assertNull(schema.deserialize(null));
  }

  @Test(expected = IllegalStateException.class)
  public void testSerializeWithNullDataSet() {
    DataSetSchema schema = new DataSetSchema();
    schema.serialize(null);
  }
}
