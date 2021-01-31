package com.cqx.jstorm.comm.bolt;

import com.cqx.jstorm.dpi.bolt.EmitTestBolt;
import com.cqx.jstorm.comm.test.TestBolt;
import com.cqx.jstorm.comm.util.AppConst;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EmitTestBoltTest extends TestBolt {

    @Before
    public void setUp() throws Exception {
        iBolt = new EmitTestBolt();
        super.prepare(conf);
        iBolt.prepare(stormConf, context);
    }

    @After
    public void tearDown() throws Exception {
        iBolt.cleanup();
    }

    @Test
    public void execute() throws Exception {
        iBolt.execute(buildTuple(AppConst.FIELDS, 5));
    }

}