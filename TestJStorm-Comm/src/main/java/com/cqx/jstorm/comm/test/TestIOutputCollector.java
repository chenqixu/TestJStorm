package com.cqx.jstorm.comm.test;

import backtype.storm.generated.StreamInfo;
import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * TestIOutputCollector
 *
 * @author chenqixu
 */
public class TestIOutputCollector implements IOutputCollector {
    private static Logger logger = LoggerFactory.getLogger(TestIOutputCollector.class);
    private HashMap<String, BlockingQueue<TestTuple>> streamIdMap = new HashMap<>();
    private Map<String, StreamInfo> _fields;

    /**
     * 提交数据并写入队列
     *
     * @param streamId
     * @param tuple
     */
    private void emitDefault(String streamId, List<Object> tuple) {
        try {
            TestTuple testTuple = TestTuple.builder();
            for (int i = 0; i < tuple.size(); i++) {
                String field = _fields.get(streamId).get_output_fields().get(i);// field
                Object tupleObj = tuple.get(i);// tuple
                testTuple.put(streamId, field, tupleObj);
            }
            getTupleQueue(streamId).put(testTuple);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        logger.debug("throw emit，streamId：{}，anchors：{}，tuple：{}", streamId, anchors, tuple);
        emitDefault(streamId, tuple);
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        logger.debug("throw emitDirect，taskId：{}，streamId：{}，anchors：{}，tuple：{}", taskId, streamId, anchors, tuple);
        emitDefault(streamId, tuple);
    }

    private synchronized BlockingQueue<TestTuple> getTupleQueue(String streamId) {
        BlockingQueue<TestTuple> fieldsQueue = streamIdMap.get(streamId);
        if (fieldsQueue == null) {
            fieldsQueue = new LinkedBlockingQueue<>();
            streamIdMap.put(streamId, fieldsQueue);
            logger.info("fieldsQueue init，streamId：{}", streamId);
        }
        return fieldsQueue;
    }

    @Override
    public void ack(Tuple input) {
        logger.debug("throw ack，input：{}", input);
    }

    @Override
    public void fail(Tuple input) {
        logger.warn("throw fail，input：{}", input);
    }

    @Override
    public void reportError(Throwable error) {
        logger.warn("throw reportError，error：{}", error);
    }

    /**
     * 初始化
     *
     * @param _fields
     */
    public void set_fields(Map<String, StreamInfo> _fields) {
        this._fields = _fields;
        for (Map.Entry<String, StreamInfo> entry : _fields.entrySet()) {
            String streamId = entry.getKey();// streamId
            getTupleQueue(streamId);// 初始化streamId
        }
    }

    public HashMap<String, BlockingQueue<TestTuple>> getStreamIdMap() {
        return streamIdMap;
    }
}
