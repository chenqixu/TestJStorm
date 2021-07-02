package com.cqx.jstorm.comm.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * spout - bolt间的数据传输
 *
 * @author chenqixu
 */
public class TestSpoutBoltTransmission extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(TestSpoutBoltTransmission.class);
    protected String conf = null;
    private TestSpout testSpout;
    private TestBolt testBolt;
    private Thread threadProducer;
    private Thread threadConsummer;
    private Producer producer;
    private Consummer consummer;

    public void prepare(String spoutName, String boltName, String confPath) throws Exception {
        testSpout = new TestSpout();
        testBolt = new TestBolt();
        conf = getResourceClassPath(confPath);
        testSpout.prepare(conf, spoutName);
        testBolt.prepare(conf, boltName);
        producer = new Producer();
        consummer = new Consummer(testSpout.getAllTuples());
        threadProducer = new Thread(producer);
        threadConsummer = new Thread(consummer);
    }

    /**
     * 启动数据传输线程
     */
    public void startTask() {
        threadProducer.start();
        threadConsummer.start();
    }

    /**
     * 停止数据传输线程
     */
    public void stopTask() {
        if (producer != null) producer.stop();
        if (consummer != null) consummer.stop();
        if (threadProducer != null)
            try {
                threadProducer.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        if (threadConsummer != null)
            try {
                threadConsummer.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        if (testSpout != null) testSpout.close();
        if (testBolt != null) testBolt.cleanup();
    }

    /**
     * Spout生产线程
     */
    class Producer extends TestBaseRunable {

        @Override
        protected void exec() throws Exception {
            testSpout.nextTuple();
        }
    }

    /**
     * 从Spout消费数据，传给Bolt
     */
    class Consummer extends TestBaseRunable {

        HashMap<String, BlockingQueue<TestTuple>> tuplesMap;

        Consummer(HashMap<String, BlockingQueue<TestTuple>> tuplesMap) {
            this.tuplesMap = tuplesMap;
        }

        @Override
        protected void exec() throws Exception {
            for (Map.Entry<String, BlockingQueue<TestTuple>> entry : tuplesMap.entrySet()) {
                BlockingQueue<TestTuple> tuplesQueue = entry.getValue();// tpule queue
                TestTuple testTuple;
                while ((testTuple = tuplesQueue.poll()) != null) {
                    try {
                        // 调用下游bolt
                        testBolt.execute(testTuple);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        }
    }
}
