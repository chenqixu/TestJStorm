package com.cqx.jstorm.dpi.spout;

import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.comm.test.TestSpout;
import com.cqx.jstorm.comm.util.TimeCostUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendSpoutTest extends TestSpout {
    private static final Logger logger = LoggerFactory.getLogger(SendSpoutTest.class);

    @Before
    public void setUp() throws Exception {
        conf = getResourceClassPath("test1.yaml");
        prepare(conf, "SendSpout");
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    @Test
    public void exec() throws Exception {
        TimeCostUtil tc = new TimeCostUtil();
        for (int j = 0; j < 10; j++) {
            tc.start();
            for (int i = 0; i < 20000; i++)
                nextTuple();
            logger.info("cost {}", tc.stopAndGet());
            SleepUtil.sleepMilliSecond(500);
        }
    }
}