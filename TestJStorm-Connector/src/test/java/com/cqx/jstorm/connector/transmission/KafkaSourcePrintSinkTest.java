package com.cqx.jstorm.connector.transmission;

import backtype.storm.utils.Utils;
import com.cqx.jstorm.test.TestSpoutBoltTransmission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * KafkaSourcePrintSinkTest
 *
 * @author chenqixu
 */
public class KafkaSourcePrintSinkTest extends TestSpoutBoltTransmission {
    @Before
    public void setUp() throws Exception {
        //初始化
        prepare("KafkaSourceConnector",
                "PrintSinkConnector",
                "kafka_to_print_mix.config.yaml");
//                "kafka_to_print.config.yaml");
    }

    @After
    public void tearDown() throws Exception {
        stopTask();
    }

    @Test
    public void exec() {
        startTask();
        Utils.sleep(5000);
    }
}
