package com.cqx.jstorm.connector.sys;

import com.cqx.jstorm.sql.bean.Column;
import com.cqx.jstorm.test.TestBolt;
import com.cqx.jstorm.test.TestTuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PrintSinkConnectorTest extends TestBolt {

    @Before
    public void setUp() throws Exception {
        // 获取配置
        conf = getResourceClassPath("kafka_to_print.config.yaml");
        // bolt初始化
        super.prepare(conf, "PrintSinkConnector");
    }

    @After
    public void tearDown() throws Exception {
        cleanup();
    }

    @Test
    public void execute() throws Exception {
        iBolt.execute(TestTuple.builder()
                .put("xdr_id", new Column("xdr_id", "string", "440680c6d19cb900"))
                .put("msisdn", new Column("msisdn", "string", "18876266655"))
        );
    }
}