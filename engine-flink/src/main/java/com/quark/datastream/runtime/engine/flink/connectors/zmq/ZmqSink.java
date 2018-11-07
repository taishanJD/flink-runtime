package com.quark.datastream.runtime.engine.flink.connectors.zmq;

import com.quark.datastream.runtime.engine.flink.connectors.zmq.common.ZmqConnectionConfig;
import com.quark.datastream.runtime.engine.flink.connectors.zmq.common.ZmqUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class ZmqSink<DataT> extends RichSinkFunction<DataT> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(ZmqSink.class);

  private final ZmqConnectionConfig zmqConnectionConfig;
  private final String topic;
  private SerializationSchema<DataT> schema;

  private transient ZMQ.Context zmqContext;
  private transient ZMQ.Socket zmqSocket;

  /**
   * Class constructor specifying ZeroMQ connection with data schema.
   *
   * @param zmqConnectionConfig ZeroMQ connection configuration including ip, port, parallelism
   * @param topic ZeroMQ topic name
   * @param schema Serialization schema when reading the entity on ZeroMQ
   */
  public ZmqSink(ZmqConnectionConfig zmqConnectionConfig, String topic,
      SerializationSchema<DataT> schema) {
    this.zmqConnectionConfig = zmqConnectionConfig;
    this.topic = topic;
    this.schema = schema;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.zmqContext = ZMQ.context(this.zmqConnectionConfig.getIoThreads());
    this.zmqSocket = this.zmqContext.socket(ZMQ.PUB);

    // Attempt to bind first, connect if bind fails.
    LOGGER.info("Binding ZMQ to {}", this.zmqConnectionConfig.getConnectionAddress());
    this.zmqSocket.bind(this.zmqConnectionConfig.getConnectionAddress());
  }

  @Override
  public void close() throws Exception {
    super.close();

    if (this.zmqSocket != null) {
      this.zmqSocket.close();
    }

    if (this.zmqContext != null) {
      this.zmqContext.close();
    }
  }

  @Override
  public void invoke(DataT dataT) throws Exception {
    byte[] msg = schema.serialize(dataT);
    this.zmqSocket.sendMore(this.topic);
    this.zmqSocket.send(ZmqUtil.encode(msg));
  }
}
