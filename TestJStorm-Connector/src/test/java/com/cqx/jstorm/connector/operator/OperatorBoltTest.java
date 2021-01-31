package com.cqx.jstorm.connector.operator;

import com.cqx.jstorm.comm.test.TestBolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OperatorBoltTest extends TestBolt {

    @Before
    public void setUp() throws Exception {
        // 获取配置
        conf = getResourceClassPath("kafka_groupby_print.config.yaml");
        // bolt初始化
        super.prepare(conf, "OperatorBolt");
//        iBolt = new OperatorBolt();
//        super.prepare(conf);
//        iBolt.prepare(stormConf, context);
    }

    @After
    public void tearDown() throws Exception {
        cleanup();
    }

    @Test
    public void execute() {
    }
}