package com.cqx.jstorm.comm.test;

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
public class TestBoltTransmission extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(TestBoltTransmission.class);
    protected String conf;
    private TestBolt iBoltStart;
    private TestBolt iBoltEnd;
    private TestBoltTransmissionRunble testBoltTransmissionRunble;
    private Thread testBoltTransmissionThread;

    public void prepare(String iBoltStartName, String iBoltEndName, String confPath) throws Exception {
        iBoltStart = new TestBolt();
        iBoltEnd = new TestBolt();
        conf = getResourceClassPath(confPath);
        iBoltStart.prepare(conf, iBoltStartName);
        iBoltEnd.prepare(conf, iBoltEndName);
        testBoltTransmissionRunble = new TestBoltTransmissionRunble();
        testBoltTransmissionThread = new Thread(testBoltTransmissionRunble);
    }

    /**
     * 启动数据传输线程
     */
    public void startTask() {
        if (testBoltTransmissionThread != null) testBoltTransmissionThread.start();
    }

    /**
     * 停止数据传输线程
     */
    public void stopTask() {
        if (testBoltTransmissionRunble != null) testBoltTransmissionRunble.stop();
        if (testBoltTransmissionThread != null)
            try {
                testBoltTransmissionThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        if (iBoltStart != null) iBoltStart.cleanup();
        if (iBoltEnd != null) iBoltEnd.cleanup();
    }

    /**
     * 往第一个bolt发送数据
     *
     * @param testTuple
     * @throws Exception
     */
    public void addTuple(TestTuple testTuple) throws Exception {
        if (iBoltStart != null) iBoltStart.execute(testTuple);
    }

    /**
     * 数据传输线程
     */
    class TestBoltTransmissionRunble extends TestBaseRunable {

        @Override
        protected void exec() throws Exception {
            HashMap<String, BlockingQueue<TestTuple>> tuplesMap = iBoltStart.getAllTuples();
            for (Map.Entry<String, BlockingQueue<TestTuple>> entry : tuplesMap.entrySet()) {
                BlockingQueue<TestTuple> tuplesQueue = entry.getValue();// tpule queue
                TestTuple testTuple;
                while ((testTuple = tuplesQueue.poll()) != null) {
                    try {
                        // 调用下游bolt
                        iBoltEnd.execute(testTuple);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        }
    }
}
