package com.quark.datastream.runtime.engine.flink.connectors.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;

import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbSink extends RichSinkFunction<DataSet> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSink.class);

  private final String ip;
  private final int port;
  private final String dbName;
  private final String tableName;

  private transient MongoClient client = null;
  private transient MongoDatabase db = null;
  private transient MongoCollection<Document> table = null;

  /**
   * Class constructor specifying target DB.   *
   *
   * @param ip IP address or hostname of target MongoDB
   * @param port port number of target MongoDB
   * @param dbName Target database name
   * @param tableName Target table name
   */
  public MongoDbSink(String ip, int port, String dbName, String tableName) {
    this.ip = ip;
    this.port = port;
    this.dbName = dbName;
    this.tableName = tableName;
  }

  @Override
  public void invoke(DataSet dataSet) throws Exception {

    // You could loop through records like this:
    List<Document> list = new ArrayList<>();
    for (DataSet.Record record : dataSet.getRecords()) {
      LOGGER.info("Writing to {}:{}. DataSet: {}", ip, port, record.toString());

      list.add(Document.parse(record.toString()));
    }
    if (list.size() > 0) {
      table.insertMany(list);
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    client = new MongoClient(ip, port);
    db = client.getDatabase(dbName);
    table = db.getCollection(tableName);

    LOGGER.info("Initiate MongoDB Connection Intialization : " + ip + " : " + port);
    LOGGER.info("DB Name {}, Table Name {} : ", dbName, tableName);

  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Close MongoDB Connection");
    client.close();
    super.close();
  }
}
