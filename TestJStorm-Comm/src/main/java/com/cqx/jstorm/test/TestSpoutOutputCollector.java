package com.cqx.jstorm.test;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * TestSpoutOutputCollector
 *
 * @author chenqixu
 */
public class TestSpoutOutputCollector extends SpoutOutputCollector {

    private static Logger logger = LoggerFactory.getLogger(TestSpoutOutputCollector.class);
    private static BlockingQueue<Object> messageQueue = new LinkedBlockingQueue<>();
    private static BlockingQueue<List<Object>> tupleQueue = new LinkedBlockingQueue<>();

    public TestSpoutOutputCollector(ISpoutOutputCollector delegate) {
        super(delegate);
    }

    public static TestSpoutOutputCollector build() {
        return new TestSpoutOutputCollector(new TestISpoutOutputCollector());
    }

    public synchronized Object pollMessage() {
        return messageQueue.poll();
    }

    public synchronized List<Object> pollTuple() {
        return tupleQueue.poll();
    }

    static class TestISpoutOutputCollector implements ISpoutOutputCollector {

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            logger.debug("throw emit，streamId：{}，tuple：{}，messageId：{}", streamId, tuple, messageId);
            try {
                messageQueue.put(messageId);
                tupleQueue.put(tuple);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return null;
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
            logger.debug("throw emitDirect，taskId：{}，streamId：{}，tuple：{}，messageId：{}", taskId, streamId, tuple, messageId);
        }

        @Override
        public void reportError(Throwable error) {
            logger.error("throw reportError，error：{}", error);
        }
    }
}
