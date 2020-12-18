package com.cqx.jstorm.test;

import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.SpoutOutputCollectorCb;
import backtype.storm.task.ICollectorCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * TestSpoutOutputCollectorCb
 *
 * @author chenqixu
 */
public class TestSpoutOutputCollectorCb extends SpoutOutputCollectorCb {
    private static final Logger logger = LoggerFactory.getLogger(TestSpoutOutputCollectorCb.class);
    private Map<String, StreamInfo> _fields;
    private HashMap<String, BlockingQueue<TestTuple>> streamIdMap = new HashMap<>();

    /**
     * 提交数据并写入队列
     *
     * @param streamId
     * @param tuple
     * @param messageId
     */
    private void emitDefault(String streamId, List<Object> tuple, Object messageId) {
        try {
            TestTuple testTuple;
            if (messageId != null) {
                testTuple = TestTuple.builder(new TestMessageId(messageId));
            } else {
                testTuple = TestTuple.builder();
            }
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
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        logger.debug("throw emit，streamId：{}，tuple：{}，messageId：{}", streamId, tuple, messageId);
        emitDefault(streamId, tuple, messageId);
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        logger.debug("throw emitDirect，taskId：{}，streamId：{}，tuple：{}，messageId：{}", taskId, streamId, tuple, messageId);
        emitDefault(streamId, tuple, messageId);
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return emit(streamId, tuple, messageId, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitDirect(taskId, streamId, tuple, messageId, null);
    }

    @Override
    public void reportError(Throwable error) {
        logger.error("throw reportError，error：{}", error);
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

    private synchronized BlockingQueue<TestTuple> getTupleQueue(String streamId) {
        BlockingQueue<TestTuple> fieldsQueue = streamIdMap.get(streamId);
        if (fieldsQueue == null) {
            fieldsQueue = new LinkedBlockingQueue<>();
            streamIdMap.put(streamId, fieldsQueue);
            logger.info("fieldsQueue init，streamId：{}", streamId);
        }
        return fieldsQueue;
    }
}
