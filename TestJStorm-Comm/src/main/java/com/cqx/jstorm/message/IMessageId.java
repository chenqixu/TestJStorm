package com.cqx.jstorm.message;

import java.io.Serializable;

/**
 * IMessageId
 *
 * @author chenqixu
 */
public abstract class IMessageId implements Comparable, Serializable {
    private String messageId;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    @Override
    public int compareTo(Object o) {
        return this.messageId.equals(((IMessageId) o).getMessageId()) ? 0 : 1;
    }
}
