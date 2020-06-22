package com.cqx.jstorm.spout;

import com.cqx.jstorm.test.TestSpout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EmitTimeSpoutTest extends TestSpout {

    @Before
    public void setUp() throws Exception {
        // spout初始化
        iSpout = new EmitTimeSpout();
        conf = getResourceClassPath("config.local.s1mme.yaml");
        super.prepare(conf);
        iSpout.open(stormConf, context);
    }

    @After
    public void tearDown() throws Exception {
        iSpout.close();
    }

    @Test
    public void nextTuple() throws Exception {
        iSpout.nextTuple();
    }
}