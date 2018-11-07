package com.quark.datastream.runtime.engine.flink.connectors.mqtt.common;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class EmqUtil {

    public static MqttClient getEmqConnect(String ip, Integer port, String clientId, Boolean isNewSession, String userName, String password) {
        MqttClient sampleClient = null;
        try {
            String broker = "tcp://" + ip + ":" + port;
            MemoryPersistence persistence = new MemoryPersistence();
            sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();

            // 使用新会话
            connOpts.setCleanSession(isNewSession);
            /** 设置会话心跳和超时时间，避免以下异常
             * 已断开连接 (32109) - java.io.EOFException
             at org.eclipse.paho.client.mqttv3.internal.CommsReceiver.run(CommsReceiver.java:181)
             at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
             at java.util.concurrent.FutureTask.run(FutureTask.java:266)
             at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
             at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
             at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
             at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
             at java.lang.Thread.run(Thread.java:745)
             Caused by: java.io.EOFException
             at java.io.DataInputStream.readByte(DataInputStream.java:267)
             at org.eclipse.paho.client.mqttv3.internal.wire.MqttInputStream.readMqttWireMessage(MqttInputStream.java:92)
             at org.eclipse.paho.client.mqttv3.internal.CommsReceiver.run(CommsReceiver.java:133)
             ... 7 more
             */
            // 设置超时时间
            connOpts.setConnectionTimeout(10);
            // 设置会话心跳时间
            connOpts.setKeepAliveInterval(20);

            connOpts.setUserName(userName);
            connOpts.setPassword(password.toCharArray());
            sampleClient.connect(connOpts);
        } catch (MqttException e) {
            e.printStackTrace();
        }
        return sampleClient;
    }
}
