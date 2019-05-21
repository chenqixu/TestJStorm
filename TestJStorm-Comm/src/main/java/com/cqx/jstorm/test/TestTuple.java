package com.cqx.jstorm.test;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 用于测试
 *
 * @author chenqixu
 */
public class TestTuple implements Tuple {

    private Map<String, String> valueMap = new HashMap<>();
    private List<Object> valueList = new ArrayList<>();
    private String sourceStreamId;

    public TestTuple() {
    }

    public static TestTuple builder() {
        return new TestTuple();
    }

    public TestTuple put(String sourceStreamId, String filed, String value) {
        this.sourceStreamId = sourceStreamId;
        valueMap.put(filed, value);
        valueList.add(value);
        return this;
    }

    public TestTuple put(String filed, String value) {
        put(null, filed, value);
        return this;
    }

    @Override
    public int size() {
        return valueList.size();
    }

    @Override
    public boolean contains(String field) {
        return false;
    }

    @Override
    public Fields getFields() {
        return null;
    }

    @Override
    public int fieldIndex(String field) {
        return 0;
    }

    @Override
    public List<Object> select(Fields selector) {
        return null;
    }

    @Override
    public Object getValue(int i) {
        return null;
    }

    @Override
    public String getString(int i) {
        return null;
    }

    @Override
    public Integer getInteger(int i) {
        return null;
    }

    @Override
    public Long getLong(int i) {
        return null;
    }

    @Override
    public Boolean getBoolean(int i) {
        return null;
    }

    @Override
    public Short getShort(int i) {
        return null;
    }

    @Override
    public Byte getByte(int i) {
        return null;
    }

    @Override
    public Double getDouble(int i) {
        return null;
    }

    @Override
    public Float getFloat(int i) {
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];
    }

    @Override
    public Object getValueByField(String field) {
        return null;
    }

    @Override
    public String getStringByField(String field) {
        return valueMap.get(field);
    }

    @Override
    public Integer getIntegerByField(String field) {
        return null;
    }

    @Override
    public Long getLongByField(String field) {
        return null;
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return null;
    }

    @Override
    public Short getShortByField(String field) {
        return null;
    }

    @Override
    public Byte getByteByField(String field) {
        return null;
    }

    @Override
    public Double getDoubleByField(String field) {
        return null;
    }

    @Override
    public Float getFloatByField(String field) {
        return null;
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return new byte[0];
    }

    @Override
    public List<Object> getValues() {
        return valueList;
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return null;
    }

    @Override
    public String getSourceComponent() {
        return null;
    }

    @Override
    public int getSourceTask() {
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        return sourceStreamId;
    }

    @Override
    public MessageId getMessageId() {
        return null;
    }
}
