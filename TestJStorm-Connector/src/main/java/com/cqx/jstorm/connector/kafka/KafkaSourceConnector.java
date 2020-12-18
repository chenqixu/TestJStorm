package com.cqx.jstorm.connector.kafka;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.bean.SendBean;
import com.cqx.jstorm.spout.ISpout;
import com.cqx.jstorm.sql.bean.Column;
import com.cqx.jstorm.util.kafka.KafkaConsumerUtil;
import com.cqx.jstorm.util.kafka.KafkaSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * KafkaSourceConnector
 *
 * @author chenqixu
 */
public class KafkaSourceConnector extends ISpout {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceConnector.class);
    private KafkaConsumerUtil kafkaConsumerUtil = null;
    private KafkaFormatUtil kafkaFormatUtil;
    private long poll_time = 1000L;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        String topic = (String) conf.get("topic");
        String bootstrap_servers = (String) conf.get("properties.bootstrap.servers");
        String group_id = (String) conf.get("properties.group.id");
        String security_protocol = ((String) conf.get("properties.security.protocol")).toUpperCase();
        String sasl_mechanism = ((String) conf.get("properties.sasl.mechanism")).toUpperCase();
        String sasl_jaas_config = (String) conf.get("properties.sasl.jaas.config");
        String scan_startup_mode = (String) conf.get("scan.startup.mode");
        String format = (String) conf.get("format");
        String format_content = (String) conf.get("format.content");

        KafkaSecurityUtil kafkaSecurityUtil = new KafkaSecurityUtil();
        kafkaSecurityUtil.parser(sasl_jaas_config);

        logger.info("topic：{}，bootstrap_servers：{}，sasl_jaas_config：{}，username：{}，password：{}，format_content：{}",
                topic, bootstrap_servers, sasl_jaas_config, kafkaSecurityUtil.getUserName(), kafkaSecurityUtil.getPassWord(), format_content);

        Map<String, String> kafkaConf = new HashMap<>();
        kafkaConf.put("kafkaconf.bootstrap.servers", bootstrap_servers);
        kafkaConf.put("kafkaconf.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConf.put("kafkaconf.value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaConf.put("kafkaconf.security.protocol", security_protocol);
        kafkaConf.put("kafkaconf.sasl.mechanism", sasl_mechanism);
        kafkaConf.put("kafkaconf.group.id", group_id);
        kafkaConf.put("kafkaconf.enable.auto.commit", "true");

        //消费者工具类，提供kafka配置
        kafkaConsumerUtil = new KafkaConsumerUtil(kafkaConf, kafkaSecurityUtil.getUserName(), kafkaSecurityUtil.getPassWord());
        //订阅话题
        kafkaConsumerUtil.subscribe(topic);
        //从末尾开始消费
        kafkaConsumerUtil.modeSet(scan_startup_mode);
        //格式化工具类
        kafkaFormatUtil = new KafkaFormatUtil();
        kafkaFormatUtil.init(format, format_content);
    }

    @Override
    public void nextTuple() throws Exception {
        List<byte[]> values = kafkaConsumerUtil.poll(poll_time);
        logger.info("fetch_size：{}", values.size());
        for (byte[] value : values) {
            for (SendBean sendBean : getSendBeanList()) {
                Column[] val = kafkaFormatUtil.read(value, sendBean);
                if (sendBean.getStreamId() != null) {
                    this.collector.emit(sendBean.getStreamId(), new Values(val), grenerateUUIDMessageId());
                } else {
                    this.collector.emit(new Values(val), grenerateUUIDMessageId());
                }
            }
        }
    }

    @Override
    public void close() {
        if (kafkaConsumerUtil != null) kafkaConsumerUtil.close();
    }
}
