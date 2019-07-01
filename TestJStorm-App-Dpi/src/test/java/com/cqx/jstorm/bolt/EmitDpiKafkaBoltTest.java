package com.cqx.jstorm.bolt;

import com.cqx.jstorm.bean.KafkaTuple;
import com.cqx.jstorm.test.TestBolt;
import com.cqx.jstorm.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class EmitDpiKafkaBoltTest extends TestBolt {

    @Before
    public void setUp() throws Exception {
        iBolt = new EmitDpiKafkaBolt();
        super.prepare(conf);
        iBolt.prepare(stormConf, context);
    }

    @After
    public void tearDown() throws Exception {
        iBolt.cleanup();
    }

    /**
     * 一边造数据，一边线程进行消费
     *
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
        String topic = "nmc_tb_lte_http";
        String key = "13500000000";
        Map<String, String> fields = new HashMap<>();
        fields.put("city_1", "1");
        fields.put("imsi", "1");
        fields.put("imei", "1");
        fields.put("msisdn", "13500000000");
        fields.put("tac", "1");
        fields.put("eci", "1");
        fields.put("rat", "1");
        fields.put("procedure_start_time", "1");
        fields.put("app_class", "1");
        fields.put("host", "1");
        fields.put("uri", "1");
        fields.put("apply_classify", "1");
        fields.put("apply_name", "1");
        fields.put("web_classify", "1");
        fields.put("web_name", "1");
        fields.put("search_keyword", "1");
        fields.put("procedure_end_time", "1");
        fields.put("upbytes", "1");
        fields.put("downbytes", "1");
        KafkaTuple kafkaTuple = new KafkaTuple(topic, key, fields);
        for (int i = 0; i < 1000000; i++)
            iBolt.execute(buildTuple(EmitDpiIBolt.KAFKA_STREAM_ID, EmitDpiIBolt.KAFKA_FIELDS, kafkaTuple));
        Utils.sleep(15000);
    }

}