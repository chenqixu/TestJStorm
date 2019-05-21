package com.cqx.jstorm.test;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * TestOutputCollector
 *
 * @author chenqixu
 */
public class TestOutputCollector extends OutputCollector {

    private static Logger logger = LoggerFactory.getLogger(TestOutputCollector.class);

    public TestOutputCollector(IOutputCollector delegate) {
        super(delegate);
    }

    public static TestOutputCollector build() {
        return new TestOutputCollector(new TestIOutputCollector());
    }

    static class TestIOutputCollector implements IOutputCollector {

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            logger.info("throw emit，streamId：{}，anchors：{}，tuple：{}", streamId, anchors, tuple);
            return null;
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            logger.info("throw emitDirect，taskId：{}，streamId：{}，anchors：{}，tuple：{}", taskId, streamId, anchors, tuple);
        }

        @Override
        public void ack(Tuple input) {
            logger.info("throw ack，input：{}", input);
        }

        @Override
        public void fail(Tuple input) {
            logger.info("throw fail，input：{}", input);
        }

        @Override
        public void reportError(Throwable error) {
            logger.info("throw reportError，error：{}", error);
        }
    }
}
