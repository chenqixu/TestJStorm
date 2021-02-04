package com.cqx.jstorm.dpi.bolt;

import com.cqx.common.utils.system.ClassUtil;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.dpi.bolt.EmitHdfsBolt;
import com.cqx.jstorm.comm.test.TestBolt;
import com.cqx.jstorm.comm.util.AppConst;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class EmitHdfsBoltTest extends TestBolt {

    @Before
    public void setUp() throws Exception {
        ClassUtil classUtil = new ClassUtil();
        iBolt = new EmitHdfsBolt();
        super.prepare(classUtil.getResource("config.hdfs.yaml").toString());
        iBolt.prepare(stormConf, context);
    }

    @After
    public void tearDown() throws Exception {
        iBolt.cleanup();
    }

    @Test
    public void execute() throws Exception {
        List<String> msgList = new ArrayList<>();
        msgList.add("1234567890");
        msgList.add("abcdefghijkl");
        iBolt.execute(buildTuple(AppConst.FIELDS, msgList));
        //wait
        SleepUtil.sleepMilliSecond(5000);
    }
}