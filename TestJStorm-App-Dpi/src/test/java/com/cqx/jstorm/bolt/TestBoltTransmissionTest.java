package com.cqx.jstorm.bolt;

import com.cqx.jstorm.test.TestBolt;
import com.cqx.jstorm.test.TestBoltTransmission;
import com.cqx.jstorm.test.TestTuple;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBoltTransmissionTest {

    private TestBoltTransmission testBoltTransmission;
    private TestBolt iBoltStart;
    private TestBolt iBoltEnd;

    @Before
    public void setUp() throws Exception {
        iBoltStart = TestBolt.builder(new EmitTestBolt());
        iBoltEnd = TestBolt.builder(new EmitTestGetBolt());
        testBoltTransmission = new TestBoltTransmission(iBoltStart, iBoltEnd);
    }

    @After
    public void tearDown() throws Exception {
        testBoltTransmission.stop();
    }

    @Test
    public void start() throws Exception {
        testBoltTransmission.start();
        for (int i = 0; i < 10; i++)
            iBoltStart.execute(TestTuple.builder().put(AppConst.FIELDS, "d:\\tmp\\data\\dpi\\dpi_s1mme\\streaminput\\LTE_S1MME_028470736002_20190603110100.txt"));
        Utils.sleep(5000);
    }
}