package com.cqx.jstorm.spout;

import com.cqx.jstorm.test.TestSpout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EmitTimeSpoutTest extends TestSpout {

    private String conf = "file:///D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\config.local.s1mme.yaml";

    @Before
    public void setUp() throws Exception {
        // spout初始化
        iSpout = new EmitTimeSpout();
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