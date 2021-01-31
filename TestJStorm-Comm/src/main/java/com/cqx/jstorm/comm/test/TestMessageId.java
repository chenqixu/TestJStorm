package com.cqx.jstorm.comm.test;

import backtype.storm.tuple.MessageId;

/**
 * TestMessageId
 *
 * @author chenqixu
 */
public class TestMessageId extends MessageId {

    private Object messageId;

    public TestMessageId(Object messageId) {
        super(null);
        this.messageId = messageId;
    }

    @Override
    public String toString() {
        return this.messageId.toString();
    }
}
