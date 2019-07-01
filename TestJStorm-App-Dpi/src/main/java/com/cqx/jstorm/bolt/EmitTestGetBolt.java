package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bean.KafkaTuple;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.Utils;
import com.cqx.jstorm.utils.GenericRecordUtil;
import com.cqx.jstorm.utils.KafkaProducerUtil;
import com.cqx.jstorm.utils.KafkaTupleBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * EmitTestGetBolt
 *
 * @author chenqixu
 */
public class EmitTestGetBolt extends IBolt {
    private static final Logger logger = LoggerFactory.getLogger(EmitTestGetBolt.class);
    //    private int printcnt = 0;
    private AtomicInteger printcnt = new AtomicInteger();
    private KafkaProducerUtil<String, byte[]> kafkaProducerUtil;
    private GenericRecordUtil genericRecordUtil;
    private String topic;
    private String schemaUrl;
    private String kafkaConfDir;

    private KafkaTupleBlockingQueue kafkaTupleBlockingQueue;
    private SendKafka sendKafka;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        logger.info("####EmitTestGetBolt.prepare");
        String kafkaUser = (String) stormConf.get("kafka_username");
        String kafkaPass = (String) stormConf.get("kafka_password");
        topic = (String) stormConf.get("topic");
        schemaUrl = (String) stormConf.get("schemaUrl");
        kafkaConfDir = (String) stormConf.get("kafkaConfDir");
        kafkaProducerUtil = new KafkaProducerUtil<>(kafkaConfDir, kafkaUser, kafkaPass);
        genericRecordUtil = new GenericRecordUtil(schemaUrl);
        genericRecordUtil.addTopic(topic);
        kafkaTupleBlockingQueue = new KafkaTupleBlockingQueue();
        sendKafka = new SendKafka();
        new Thread(sendKafka).start();
//        new Thread(sendKafka).start();
    }

    @Override
    public void execute(Tuple input) throws Exception {
        KafkaTuple kafkaTuple = (KafkaTuple) input.getValueByField(AppConst.FIELDS);
        kafkaTupleBlockingQueue.put(kafkaTuple);
//        String key = kafkaTuple.getKey();
//        Map<String, String> valueMap = kafkaTuple.getFields();
//        String topic = kafkaTuple.getTopic();
//        byte[] value = genericRecordUtil.genericRecord(topic, valueMap);
//        kafkaProducerUtil.send(topic, key, value);
//        printcnt++;
//        if (printcnt % 2000 == 0) {
//            logger.info("printcnt：{}", printcnt);
//        }
    }

    @Override
    public void cleanup() {
        logger.info("####{} to cleanup", this);
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
                    String key = kafkaTuple.getKey();
                    Map<String, String> valueMap = kafkaTuple.getFields();
                    String topic = kafkaTuple.getTopic();
                    byte[] value = genericRecordUtil.genericRecord(topic, valueMap);
                    kafkaProducerUtil.send(topic, key, value);
                    int p = printcnt.incrementAndGet();
                    if (p % 2000 == 0) {
                        logger.info("printcnt：{}", printcnt);
                    }
                }
                // 休眠1秒
//                logger.info("kafka消费队列为空，休眠1秒");
                Utils.sleep(50);
            }
        }
    }
}