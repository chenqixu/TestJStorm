package com.cqx.jstorm.util.kafka;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * KafkaConsumerGRUtil
 * <pre>
 *     开发参数参考：
 *       kafkaconf.bootstrap.servers: "10.1.8.200:9092,10.1.8.201:9092,10.1.8.202:9092"
 *       kafkaconf.key.deserializer: "org.apache.kafka.common.serialization.StringDeserializer"
 *       kafkaconf.value.deserializer: "org.apache.kafka.common.serialization.ByteArrayDeserializer"
 *       kafkaconf.security.protocol: "SASL_PLAINTEXT"
 *       kafkaconf.sasl.mechanism: "PLAIN"
 *       kafkaconf.group.id: "throughput_jstorm"
 *       kafkaconf.enable.auto.commit: "true"
 *       kafkaconf.fetch.min.bytes: "52428800"
 *       kafkaconf.max.poll.records: "12000"
 *       kafkaconf.newland.kafka_username: admin
 *       kafkaconf.newland.kafka_password: admin
 *       schema_url: "http://10.1.8.203:19090/nl-edc-cct-sys-ms-dev/SchemaService/getSchema?t="
 * </pre>
 *
 * @author chenqixu
 */
public class KafkaConsumerGRUtil extends KafkaConsumerUtil<String, byte[]> {
    private SchemaUtil schemaUtil;
    private RecordConvertor recordConvertor = null;

    public KafkaConsumerGRUtil(Map stormConf) throws IOException {
        super(stormConf);
        String schema_url = (String) stormConf.get("schema_url");
        //schema工具类
        schemaUtil = new SchemaUtil(schema_url);
    }

    /**
     * 订阅话题
     *
     * @param topic
     */
    @Override
    public void subscribe(String topic) {
        super.subscribe(topic);
        //记录转换工具类
        recordConvertor = new RecordConvertor(schemaUtil.getSchemaByTopic(topic));
    }

    /**
     * 消费
     *
     * @param timeout
     * @return
     */
    public List<GenericRecord> polls(long timeout) {
        List<byte[]> values = poll(timeout, false);
        List<GenericRecord> records = new ArrayList<>();
        for (byte[] bytes : values) {
            records.add(recordConvertor.binaryToRecord(bytes));
        }
        return records;
    }
}
