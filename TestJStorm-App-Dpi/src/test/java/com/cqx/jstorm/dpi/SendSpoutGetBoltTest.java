package com.cqx.jstorm.dpi;

import backtype.storm.utils.Utils;
import com.cqx.jstorm.comm.test.TestSpoutBoltTransmission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * SendSpoutGetBoltTest
 *
 * @author chenqixu
 */
public class SendSpoutGetBoltTest extends TestSpoutBoltTransmission {
    @Before
    public void setUp() throws Exception {
        //初始化
        prepare("SendSpout",
                "GetBolt",
                "test1.yaml");
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
