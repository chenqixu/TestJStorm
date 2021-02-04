package com.cqx.jstorm.dpi.bolt;

import com.cqx.jstorm.comm.test.TestBoltTransmission;
import com.cqx.jstorm.comm.test.TestTuple;
import com.cqx.jstorm.comm.util.AppConst;
import com.cqx.jstorm.comm.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBoltTransmissionTest extends TestBoltTransmission {

    @Before
    public void setUp() throws Exception {
        prepare("EmitTestBolt",
                "EmitTestGetBolt",
                "config.local.yaml");
    }

    @After
    public void tearDown() throws Exception {
        stopTask();
    }

    @Test
    public void exec() throws Exception {
        startTask();
        for (int i = 0; i < 10; i++)
            addTuple(TestTuple.builder().put(AppConst.FIELDS, "d:\\tmp\\data\\dpi\\dpi_s1mme\\streaminput\\LTE_S1MME_028470736002_20190603110100.txt"));
        Utils.sleep(5000);
    }
}