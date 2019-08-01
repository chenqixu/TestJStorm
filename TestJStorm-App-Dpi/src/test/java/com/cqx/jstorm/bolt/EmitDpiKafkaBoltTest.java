package com.cqx.jstorm.bolt;

import com.cqx.jstorm.bean.KafkaTuple;
import com.cqx.jstorm.test.TestBolt;
import com.cqx.jstorm.util.TimeCostUtil;
import com.cqx.jstorm.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class EmitDpiKafkaBoltTest extends TestBolt {

    private static Logger logger = LoggerFactory.getLogger(EmitDpiKafkaBoltTest.class);
    private BlockingQueue<KafkaTuple> kafkaTupleBlockingQueue = new LinkedBlockingQueue<>();
    private ListT listT = new ListT();

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

    @Test
    public void listTest() {
        TimeCostUtil timeCostUtil = new TimeCostUtil();
        for (int j = 0; j < 20; j++) {
            timeCostUtil.start();
            for (int i = 0; i < 27000; i++) {
                String filename = "nmc_tb_lte_http";
                String keyWord = "13500000000";
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
                listT.add(filename, keyWord, fields);
            }
            listT.endSend();
            long cost = timeCostUtil.stopAndGet();
            logger.info("j：{}，cost：{}", j, cost);
        }
    }

    private void sendKafkaMsg(List<KafkaTuple> kafkaTuples) {
        new Thread(new SendThread(kafkaTuples)).start();
    }

    private void sendKafkaMsgSingle(KafkaTuple kafkaTuple) {
        try {
//            kafkaTupleBlockingQueue.put(kafkaTuple);
//            if (kafkaTupleBlockingQueue.size() > 5000) {
//                kafkaTupleBlockingQueue.poll();
//            }
            iBolt.execute(buildTuple(EmitDpiIBolt.KAFKA_STREAM_ID, EmitDpiIBolt.KAFKA_FIELDS, kafkaTuple));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class ListT {
        private static final int MAXCOUNT = 4000;
        private BlockingQueue<KafkaTuple> values = new LinkedBlockingQueue<>();

        public void add(String filename, String keyWord, Map<String, String> kafkaContent) {
            try {
//                values.put(new KafkaTuple(filename, keyWord, kafkaContent));
//                if (values.size() == MAXCOUNT) {
//                    List<KafkaTuple> tmpKafkaTuples = new ArrayList<>();
//                    KafkaTuple tuple;
//                    while ((tuple = values.poll()) != null) {
//                        tmpKafkaTuples.add(tuple);
//                    }
//                    sendKafkaMsg(tmpKafkaTuples);
//                }
                sendKafkaMsgSingle(new KafkaTuple(filename, keyWord, kafkaContent));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void endSend() {
            if (values.size() > 0) {
                List<KafkaTuple> tmpKafkaTuples = new ArrayList<>();
                KafkaTuple tuple;
                while ((tuple = values.poll()) != null) {
                    tmpKafkaTuples.add(tuple);
                }
                sendKafkaMsg(tmpKafkaTuples);
            }
        }
    }

    class SendThread implements Runnable {
        List<KafkaTuple> kafkaTuples;

        public SendThread(List<KafkaTuple> kafkaTuples) {
            this.kafkaTuples = kafkaTuples;
        }

        @Override
        public void run() {
            for (KafkaTuple kafkaTuple : kafkaTuples) {
                try {
                    kafkaTupleBlockingQueue.put(kafkaTuple);
//                    iBolt.execute(buildTuple(EmitDpiIBolt.KAFKA_STREAM_ID, EmitDpiIBolt.KAFKA_FIELDS, kafkaTuples));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}