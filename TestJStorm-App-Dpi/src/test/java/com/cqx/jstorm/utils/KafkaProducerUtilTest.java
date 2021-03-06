package com.cqx.jstorm.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerUtilTest {

    private static Logger logger = LoggerFactory.getLogger(KafkaProducerUtilTest.class);
    private KafkaProducerUtil<String, byte[]> kafkaProducerUtil;
    private GenericRecordUtil genericRecordUtil;
    private String topic = "nmc_tb_lte_http";
    private String schemaUrl = "http://localhost:18061/SchemaService/getSchema?t=";
    private String conf = "D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-App-Dpi\\src\\main\\resources\\producer.properties";

    @Before
    public void setUp() throws Exception {
        kafkaProducerUtil = new KafkaProducerUtil<>(conf, "alice", "alice");
        genericRecordUtil = new GenericRecordUtil(schemaUrl);
        genericRecordUtil.addTopic(topic);
    }

    @After
    public void tearDown() throws Exception {
        kafkaProducerUtil.release();
    }

    @Test
    public void send() {
        String key = "13500000000";
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("city_1", "1");
        valueMap.put("imsi", "1");
        valueMap.put("imei", "1");
        valueMap.put("msisdn", "13500000000");
        valueMap.put("tac", "1");
        valueMap.put("eci", "1");
        valueMap.put("rat", "1");
        valueMap.put("procedure_start_time", "1");
        valueMap.put("app_class", "1");
        valueMap.put("host", "1");
        valueMap.put("uri", "1");
        valueMap.put("apply_classify", "1");
        valueMap.put("apply_name", "1");
        valueMap.put("web_classify", "1");
        valueMap.put("web_name", "1");
        valueMap.put("search_keyword", "1");
        valueMap.put("procedure_end_time", "1");
        valueMap.put("upbytes", "1");
        valueMap.put("downbytes", "1");
        logger.info("valueMap：{}", valueMap);
        byte[] value = genericRecordUtil.genericRecord(topic, valueMap);
        kafkaProducerUtil.send(topic, key, value);
        logger.info("send，topic：{}，key：{}，value：{}", topic, key, value);
    }
}