package com.quark.datastream.runtime.engine.flink.connectors.mqtt;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MqttSource.class})
public class MqttSourceTest {

    @Test
    public void testOpen() throws Exception {
        MqttSource mqttSource = new MqttSource("localhost",11883,"test-client-id",false,"admin","public","$share/group-redirect-data//up/#",0);
        mqttSource.open(null);
        mqttSource.run(null);
    }

    @Test
    public void testRun() throws Exception {

    }
}
