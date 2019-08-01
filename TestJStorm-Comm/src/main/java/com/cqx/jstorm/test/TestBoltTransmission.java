package com.cqx.jstorm.test;

import com.cqx.jstorm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * bolt间的数据传输
 *
 * @author chenqixu
 */
public class TestBoltTransmission {

    private static final Logger logger = LoggerFactory.getLogger(TestBoltTransmission.class);
    private TestBolt iBoltStart;
    private TestBolt iBoltEnd;
    private TestBoltTransmissionRunble testBoltTransmissionRunble;

    public TestBoltTransmission(TestBolt iBoltStart, TestBolt iBoltEnd) {
        this.iBoltStart = iBoltStart;
        this.iBoltEnd = iBoltEnd;
        this.testBoltTransmissionRunble = new TestBoltTransmissionRunble();
    }

    /**
     * 启动数据传输线程
     */
    public void start() {
        new Thread(testBoltTransmissionRunble).start();
    }

    /**
     * 停止数据传输线程
     */
    public void stop() {
        testBoltTransmissionRunble.stop();
        iBoltStart.cleanup();
        iBoltEnd.cleanup();
    }

    /**
     * 数据传输线程
     */
    class TestBoltTransmissionRunble implements Runnable {

        // 线程状态
        volatile boolean flag = true;

        @Override
        public void run() {
            while (flag) {
                HashMap<String, BlockingQueue<HashMap<String, Object>>> tuplesMap = iBoltStart.pollStreamIdMap();
                for (Map.Entry<String, BlockingQueue<HashMap<String, Object>>> entry : tuplesMap.entrySet()) {
                    String streamId = entry.getKey();// streamId
                    TestTuple testTuple = TestTuple.builder();// 每个streamId都有单独的Tuple
                    BlockingQueue<HashMap<String, Object>> tuplesQueue = entry.getValue();// tpule queue
                    HashMap<String, Object> fieldsMap;
                    while ((fieldsMap = tuplesQueue.poll()) != null) {
                        for (Map.Entry<String, Object> entry1 : fieldsMap.entrySet()) {
                            String field = entry1.getKey();// field
                            Object tuple = entry1.getValue();// tuple
                            // 拼接tuple
                            testTuple.put(streamId, field, tuple);
                        }
                        try {
                            // 调用下游bolt
                            iBoltEnd.execute(testTuple);
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                Utils.sleep(1);
            }
        }

        public void stop() {
            flag = false;
        }
    }
}
