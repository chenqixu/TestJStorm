package com.cqx.jstorm.test;

import backtype.storm.generated.StreamInfo;
import backtype.storm.task.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * TestOutputCollector
 *
 * @author chenqixu
 */
public class TestOutputCollector extends OutputCollector {

    private static Logger logger = LoggerFactory.getLogger(TestOutputCollector.class);
    private TestIOutputCollector testIOutputCollector;

    public TestOutputCollector(TestIOutputCollector delegate) {
        super(delegate);
        this.testIOutputCollector = delegate;
    }

    public static TestOutputCollector build() {
        return new TestOutputCollector(new TestIOutputCollector());
    }

    public synchronized HashMap<String, BlockingQueue<HashMap<String, Object>>> pollStreamIdMap() {
        return testIOutputCollector.getStreamIdMap();
    }

    public synchronized HashMap<String, Object> pollTuples() {
        return pollTuples("default");
    }

    public synchronized HashMap<String, Object> pollTuples(String streamId) {
        return testIOutputCollector.getStreamIdMap().get(streamId).poll();
    }

    public void set_fields(Map<String, StreamInfo> _fields) {
        this.testIOutputCollector.set_fields(_fields);
    }

}
