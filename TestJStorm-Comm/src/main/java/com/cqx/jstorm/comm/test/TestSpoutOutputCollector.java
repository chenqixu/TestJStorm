package com.cqx.jstorm.comm.test;

import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * TestSpoutOutputCollector
 *
 * @author chenqixu
 */
public class TestSpoutOutputCollector extends SpoutOutputCollector {
    private static final Logger logger = LoggerFactory.getLogger(TestSpoutOutputCollector.class);
    private TestSpoutOutputCollectorCb testSpoutOutputCollectorCb;

    public TestSpoutOutputCollector(ISpoutOutputCollector delegate) {
        super(delegate);
    }

    public TestSpoutOutputCollector(TestSpoutOutputCollectorCb delegate) {
        super(delegate);
        this.testSpoutOutputCollectorCb = delegate;
    }

    public static TestSpoutOutputCollector build() {
        return new TestSpoutOutputCollector(new TestSpoutOutputCollectorCb());
    }

    public synchronized HashMap<String, BlockingQueue<TestTuple>> pollStreamIdMap() {
        return testSpoutOutputCollectorCb.getStreamIdMap();
    }

    public synchronized TestTuple pollTuple() {
        return pollTuple("default");
    }

    public synchronized TestTuple pollTuple(String streamId) {
        return testSpoutOutputCollectorCb.getStreamIdMap().get(streamId).poll();
    }

    public void set_fields(Map<String, StreamInfo> _fields) {
        this.testSpoutOutputCollectorCb.set_fields(_fields);
    }
}
