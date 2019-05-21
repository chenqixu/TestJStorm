package com.cqx.jstorm.bolt;

import com.cqx.jstorm.spout.EmitDpiSpout;
import com.cqx.jstorm.test.TestBolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EmitDpiErrBoltTest extends TestBolt {

    @Before
    public void setUp() throws Exception {
        iBolt = new EmitDpiErrBolt();
        super.prepare(conf);
        iBolt.prepare(stormConf, context);
    }

    @After
    public void tearDown() throws Exception {
        iBolt.cleanup();
    }

    @Test
    public void execute() throws Exception {
        for (int i = 0; i < 10; i++)
            iBolt.execute(buildTuple(EmitDpiIBolt.ERR_STREAM_ID, EmitDpiSpout.VALUES, "LTE_S1UHTTP_008388787002_20190411080100.txt|错误内容xxxxxxxxxxxxxxx"));
    }
}