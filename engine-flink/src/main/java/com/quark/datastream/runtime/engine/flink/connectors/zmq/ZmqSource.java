package com.quark.datastream.runtime.engine.flink.connectors.zmq;

import com.quark.datastream.runtime.engine.flink.connectors.zmq.common.ZmqConnectionConfig;
import com.quark.datastream.runtime.engine.flink.connectors.zmq.common.ZmqUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;

public class ZmqSource<OUT> extends RichSourceFunction<OUT> implements ResultTypeQueryable<OUT> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(ZmqSource.class);

  private final ZmqConnectionConfig zmqConnectionConfig;
  private final String topic;

  private DeserializationSchema<OUT> schema;

  private transient ZMQ.Context zmqContext = null;
  private transient ZMQ.Socket zmqSocket = null;

  private transient volatile boolean running;

  /**
   * Class constructor specifying ZeroMQ connection with data schema.
   *
   * @param zmqConnectionConfig ZeroMQ connection configuration including ip, port, parallelism
   * @param topic ZeroMQ topic name
   * @param deserializationSchema Deserialization schema when writing the entity on ZeroMQ
   */
  public ZmqSource(ZmqConnectionConfig zmqConnectionConfig, String topic,
      DeserializationSchema<OUT> deserializationSchema) {
    this.zmqConnectionConfig = zmqConnectionConfig;
    this.topic = topic;
    this.schema = deserializationSchema;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.zmqContext = ZMQ.context(this.zmqConnectionConfig.getIoThreads());
    this.zmqSocket = this.zmqContext.socket(ZMQ.SUB);

    LOGGER.info("Connecting ZMQ to {}", this.zmqConnectionConfig.getConnectionAddress());
    this.zmqSocket.connect(this.zmqConnectionConfig.getConnectionAddress());

    LOGGER.info("Subscribing ZMQ to {}", this.topic);
    this.zmqSocket.subscribe(this.topic.getBytes(Charset.defaultCharset()));
    this.running = true;
  }

  @Override
  public void run(SourceContext<OUT> sourceContext) throws Exception {
    String data;
    while (this.running) {
      try {
        this.zmqSocket.recvStr(); // discard this, not used (message envelop)
        data = this.zmqSocket.recvStr();
        if(data != null) {
          byte[] b = ZmqUtil.decode(data);
          sourceContext.collect(this.schema.deserialize(b));
        }
        else {
          LOGGER.error("recv data is null");
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void cancel() {
    if (this.zmqSocket != null) {
      this.zmqSocket.close();
    }

    if (this.zmqContext != null) {
      this.zmqContext.close();
    }

    this.running = false;
  }

  @Override
  public TypeInformation<OUT> getProducedType() {
    return this.schema.getProducedType();
  }
}
