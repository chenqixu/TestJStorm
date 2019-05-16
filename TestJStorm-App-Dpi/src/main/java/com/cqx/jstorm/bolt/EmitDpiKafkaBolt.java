package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bean.KafkaTuple;
import com.cqx.jstorm.bean.TypeDef;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.utils.GenericRecordUtil;
import com.cqx.jstorm.utils.KafkaProducerUtil;

import java.util.List;
import java.util.Map;

/**
 * EmitDpiKafkaBolt
 * <pre>
 *     获取配置，kafka配置，schema url
 *     请求schema
 *     解析数据，封装成GenericRecord
 *     往kafka发送
 * </pre>
 *
 * @author chenqixu
 */
public class EmitDpiKafkaBolt extends IBolt {

    private String kafkaConfDir;
    private String schemaUrl;
    private GenericRecordUtil genericRecordUtil;
    private List<TypeDef> typeDefList;
    private KafkaProducerUtil<String, byte[]> kafkaProducerUtil;

    @Override
    protected void prepare(Map stormConf, TopologyContext context) throws Exception {
        logger.info("getThisComponentId：{}，getThisTaskId：{}，getThisTaskIndex：{}",
                context.getThisComponentId(), context.getThisTaskId(), context.getThisTaskIndex());
        // 从配置中解析所有的TypeDef
        typeDefList = TypeDef.parser(stormConf.get(AppConst.TYPEDEFS));
        logger.info("typeDefList：{}", typeDefList);
        // 从配置中获取kafka配置文件路径
        kafkaConfDir = (String) stormConf.get("kafkaConfDir");
        // 从配置中获取schema url服务地址
        schemaUrl = (String) stormConf.get("schemaUrl");
        logger.info("kafkaConfDir：{}，schemaUrl：{}", kafkaConfDir, schemaUrl);
        // 初始化工具类
        genericRecordUtil = new GenericRecordUtil(schemaUrl);
        // 初始化所有的schema
        for (TypeDef typeDef : typeDefList) {
            String _topic = typeDef.getTopic();
            if (_topic != null && _topic.length() > 0)
                genericRecordUtil.addTopic(typeDef.getTopic());
        }
        // 初始化生产者
        kafkaProducerUtil = new KafkaProducerUtil<>(kafkaConfDir);
    }

    @Override
    protected void execute(Tuple input) throws Exception {
        if (input.getSourceStreamId().equals(EmitDpiIBolt.KAFKA_STREAM_ID)) {
            KafkaTuple kafkaTuple = (KafkaTuple) input.getValueByField(EmitDpiIBolt.KAFKA_FIELDS);
            String topic = kafkaTuple.getTopic();
            Map<String, String> values = kafkaTuple.getFields();
            logger.info("receive kafka data：{}，topic：{}，values：{}，prepare to push to kafka.", kafkaTuple, topic, values);
            // 解析内容，封装GenericRecord
            byte[] msg = genericRecordUtil.genericRecord(topic, values);
            // 发往kafka
            kafkaProducerUtil.send(topic, msg);
        }
    }

    @Override
    protected void cleanup() {
        if (kafkaProducerUtil != null) kafkaProducerUtil.release();
    }
}
