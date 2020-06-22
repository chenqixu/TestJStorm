package com.cqx.jstorm.spout;

import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.test.TestSpout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FastFailureSpoutTest extends TestSpout {

    @Before
    public void setUp() throws Exception {
        // spout初始化
        iSpout = new FastFailureSpout();
        conf = getResourceClassPath("fastfailure.config.yaml");
        super.prepare(conf);
        iSpout.open(stormConf, context);
    }

    @After
    public void tearDown() throws Exception {
        iSpout.close();
    }

    @Test
    public void nextTuple() throws Exception {
        ack();
        int i = 0;
        while (i < 50) {
            iSpout.nextTuple();
            i++;
            SleepUtil.sleepMilliSecond(50);
        }
        SleepUtil.sleepMilliSecond(5000);
    }

    private void ack() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Object messageId = pollMessage();
                    if (messageId != null) {
                        iSpout.fail(messageId);
                    }
                    SleepUtil.sleepMilliSecond(200);
                }
            }
        }).start();
    }
}