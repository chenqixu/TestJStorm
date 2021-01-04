package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bean.KafkaTuple;
import com.cqx.jstorm.bean.TypeDef;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.Utils;
import com.cqx.jstorm.util.kafka.GenericRecordUtil;
import com.cqx.jstorm.util.kafka.KafkaProducerUtil;
import com.cqx.jstorm.utils.KafkaTupleBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    // 监控
    private static final String getCounter = "getCounter";
    private static final String sendCounter = "sendCounter";
    private static final String sendSizeCounter = "sendSizeCounter";

    private static Logger logger = LoggerFactory.getLogger(EmitDpiKafkaBolt.class);
    private String kafkaConfDir;
    private String schemaUrl;
    private GenericRecordUtil genericRecordUtil;
    private List<TypeDef> typeDefList;
    private KafkaProducerUtil<String, byte[]> kafkaProducerUtil;
    //    private BlockingQueue<KafkaTuple> kafkaTupleBlockingQueue;
    private KafkaTupleBlockingQueue kafkaTupleBlockingQueue;
    private SendKafka sendKafka;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        logger.info("getThisComponentId：{}，getThisTaskId：{}，getThisTaskIndex：{}",
                context.getThisComponentId(), context.getThisTaskId(), context.getThisTaskIndex());
        // 从配置中解析所有的TypeDef
        typeDefList = TypeDef.parser(stormConf.get(AppConst.TYPEDEFS));
        logger.info("typeDefList：{}", typeDefList);
        // 从配置中获取kafka配置文件路径
        kafkaConfDir = (String) stormConf.get("kafkaConfDir");
        // 从配置中获取schema url服务地址
        schemaUrl = (String) stormConf.get("schemaUrl");
        String kafka_username = (String) stormConf.get("kafka_username");
        String kafka_password = (String) stormConf.get("kafka_password");
        logger.info("kafkaConfDir：{}，schemaUrl：{}，kafka_username：{}，kafka_password：{}",
                kafkaConfDir, schemaUrl, kafka_username, kafka_password);
        // 初始化工具类
        genericRecordUtil = new GenericRecordUtil(schemaUrl);
        // 初始化所有的schema
        for (TypeDef typeDef : typeDefList) {
            String _topic = typeDef.getTopic();
            if (_topic != null && _topic.length() > 0)
                genericRecordUtil.addTopic(typeDef.getTopic());
        }
        // 初始化生产者
        kafkaProducerUtil = new KafkaProducerUtil<>(kafkaConfDir, kafka_username, kafka_password);
        // 初始化队列
//        kafkaTupleBlockingQueue = new LinkedBlockingQueue<>();
        kafkaTupleBlockingQueue = new KafkaTupleBlockingQueue();
        // 启动消费队列
        sendKafka = new SendKafka();
        new Thread(sendKafka).start();
        // 监控注册
        registerCounter(getCounter);
        registerCounter(sendCounter);
        registerCounterSize(sendSizeCounter);
    }

    @Override
    public void execute(Tuple input) throws Exception {
        if (input.getSourceStreamId().equals(EmitDpiIBolt.KAFKA_STREAM_ID)) {
            // 单条记录
            KafkaTuple kafkaTuple = (KafkaTuple) input.getValueByField(EmitDpiIBolt.KAFKA_FIELDS);
            // 如果队列满，则阻塞
            kafkaTupleBlockingQueue.put(kafkaTuple);
//            // 一批记录
//            List<KafkaTuple> kafkaTuples = (List<KafkaTuple>) input.getValueByField(EmitDpiIBolt.KAFKA_FIELDS);
//            for (KafkaTuple kafkaTuple : kafkaTuples)
//                kafkaTupleBlockingQueue.put(kafkaTuple);
            int queueSize = kafkaTupleBlockingQueue.getQueueSize();
            if (queueSize > 4000) {
//                logger.warn("kafkaTupleBlockingQueue size > 4000，now：{}", queueSize);
                kafkaTupleBlockingQueue.poll();
            }
        }
    }

    @Override
    public void cleanup() {
        if (kafkaTupleBlockingQueue != null) kafkaTupleBlockingQueue.stop();
        if (sendKafka != null) sendKafka.stop();
        if (kafkaProducerUtil != null) kafkaProducerUtil.release();
    }

    class SendKafka implements Runnable {

        private volatile boolean flag = true;

        public void stop() {
            this.flag = false;
        }

        @Override
        public void run() {
            KafkaTuple kafkaTuple;
            logger.info("启动kafka消费线程");
            while (flag) {
                // 消费队列
                while ((kafkaTuple = kafkaTupleBlockingQueue.poll()) != null) {
                    String topic = kafkaTuple.getTopic();
                    Map<String, String> values = kafkaTuple.getFields();
                    String key = kafkaTuple.getKey();
                    logger.debug("receive kafka data：{}，topic：{}，values：{}，prepare to push to kafka.", kafkaTuple, topic, values);
                    // 解析内容，封装GenericRecord
                    byte[] msg = genericRecordUtil.genericRecord(topic, values);
                    // 发往kafka
                    kafkaProducerUtil.send(topic, key, msg);
//                    counterInc(sendCounter);
//                    counterSize(sendSizeCounter, msg.length);
                    logger.debug("send，topic：{}，key：{}，value：{}", topic, key, msg);
                }
                // 休眠1秒
//                logger.info("kafka消费队列为空，休眠1秒");
                Utils.sleep(1);
            }
            logger.info("kafka消费线程已停止");
        }
    }

}
