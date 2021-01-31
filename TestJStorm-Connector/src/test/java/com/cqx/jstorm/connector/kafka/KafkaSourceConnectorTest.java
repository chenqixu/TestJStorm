package com.cqx.jstorm.connector.kafka;

import com.cqx.jstorm.comm.test.TestSpout;
import com.cqx.jstorm.comm.test.TestTuple;
import com.cqx.jstorm.comm.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceConnectorTest extends TestSpout {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceConnectorTest.class);

    @Before
    public void setUp() throws Exception {
        // 获取配置
//        conf = getResourceClassPath("kafka_to_print.config.yaml");
        conf = getResourceClassPath("kafka_to_print_mix.config.yaml");
        // spout初始化
        super.prepare(conf, "KafkaSourceConnector");
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    @Test
    public void nextTuple() throws Exception {
        ack();
        for (int i = 0; i < 100; i++) {
            iSpout.nextTuple();
            Utils.sleep(50);
        }
    }

    private void ack() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    TestTuple testTuple = pollTuple();
                    if (testTuple != null) {
                        logger.info("tuple：{}，messageId：{}", testTuple, testTuple.getMessageId());
                        iSpout.ack(testTuple.getMessageId());
                    }
                    Utils.sleep(50);
                }
            }
        }).start();
    }
}