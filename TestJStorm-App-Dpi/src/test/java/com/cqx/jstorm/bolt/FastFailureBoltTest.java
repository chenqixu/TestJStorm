package com.cqx.jstorm.bolt;

import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.spout.FastFailureSpout;
import com.cqx.jstorm.test.TestBolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FastFailureBoltTest extends TestBolt {

    @Before
    public void setUp() throws Exception {
        iBolt = new FastFailureBolt();
        conf = getResourceClassPath("fastfailure.config.yaml");
        super.prepare(conf);
        iBolt.prepare(stormConf, context);
    }

    @After
    public void tearDown() throws Exception {
        iBolt.cleanup();
    }

    @Test
    public void execute() throws Exception {
        iBolt.execute(buildTuple(FastFailureSpout.FAST_FAILURE_FIELD1, "0-Task"));
        iBolt.execute(buildTuple(FastFailureSpout.FAST_FAILURE_FIELD1, "1-Task"));
        SleepUtil.sleepMilliSecond(2000);
        iBolt.execute(buildTuple(FastFailureSpout.FAST_FAILURE_FIELD1, "2-Task"));
        SleepUtil.sleepMilliSecond(2000);
    }
}