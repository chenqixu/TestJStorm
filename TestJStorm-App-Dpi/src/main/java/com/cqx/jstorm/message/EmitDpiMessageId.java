package com.cqx.jstorm.message;

import com.cqx.jstorm.message.IMessageId;
import com.cqx.jstorm.spout.ISpout;

/**
 * EmitDpiMessageId
 *
 * @author chenqixu
 */
public class EmitDpiMessageId extends IMessageId {

    private String tag;
    private String filename;

    public EmitDpiMessageId() {
    }

    public EmitDpiMessageId(ISpout iSpout, String tag, String filename) {
        setMessageId(iSpout.grenerateMessageId());
        this.tag = tag;
        this.filename = filename;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
