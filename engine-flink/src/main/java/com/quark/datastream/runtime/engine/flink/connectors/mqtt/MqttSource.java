package com.quark.datastream.runtime.engine.flink.connectors.mqtt;


import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.quark.datastream.runtime.engine.flink.connectors.mqtt.common.EmqUtil;
import com.quark.datastream.runtime.task.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MqttSource extends RichSourceFunction<DataSet> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttSource.class);

    private final String ip;
    private final Integer port;
    private final String clientId;
    private final Boolean isNewSession;
    private final String username;
    private final String password;
    private final String topic;
    private final Integer qos;

    private transient MqttClient mqttClient = null;

    private transient volatile boolean running;

    public MqttSource(String ip, Integer port, String clientId, Boolean isNewSession, String username, String password, String topic, Integer qos) {
        this.ip = ip;
        this.port = port;
        this.clientId = clientId;
        this.isNewSession = isNewSession;
        this.username = username;
        this.password = password;
        this.topic = topic;
        this.qos = qos;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        LOGGER.info("connecting to mqtt broker: {}", "tcp://" + ip + ":" + port);
//        System.out.println("connecting to mqtt broker: "+ "tcp://" + ip + ":" + port);
        this.mqttClient = EmqUtil.getEmqConnect(ip, port, clientId, isNewSession, username, password);
        LOGGER.info("subscribing topic: {}", topic);
//        System.out.println("subscribing topic: "+ topic);
        mqttClient.subscribe(topic,qos);
        this.running = true;
    }

    @Override
    public void run(SourceContext<DataSet> sourceContext) throws Exception {
        while (this.running) {
            mqttClient.setCallback(new MyCallback(sourceContext, topic));
        }
    }

    @Override
    public void cancel() {
        if (null != this.mqttClient) {
            try {
                this.mqttClient.close();
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
        this.running = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private class MyCallback implements MqttCallback {

        private SourceContext<DataSet> sourceContext;
        private String topic;

        public MyCallback(SourceContext<DataSet> sourceContext, String topic) {
            this.sourceContext = sourceContext;
            this.topic = topic;
        }

        @Override
        public void connectionLost(Throwable throwable) {

        }

        @Override
        public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
            String msg = new String(mqttMessage.getPayload());
            //LOGGER.info("接收到：{}",msg);
            //System.out.println("接收到："+msg);

            // 判断mqttMessage的格式，封装DataSet,并发往下一个节点
            JsonParser jsonParser = new JsonParser();
            if (jsonParser.parse(msg).isJsonArray()) { // msg格式：[{},{},...{}]
                JsonArray jsonArray = (JsonArray) jsonParser.parse(msg);
                DataSet dataSet = DataSet.create(jsonArray.get(0).toString());
                for (int i = 1; i < jsonArray.size(); i++) {
                    dataSet.addRecord(jsonArray.get(i).toString());
                }
                LOGGER.info("mqtt source out: {}",dataSet.toString());
                sourceContext.collect(dataSet);
                //System.out.println("接收到：" + dataSet.toString());
            } else if (jsonParser.parse(msg).isJsonObject()) { // msg格式：{}
                DataSet dataSet = DataSet.create(msg);
                sourceContext.collect(dataSet);
            } else {
                // todo 空message或不合法message
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

        }
    }
}
