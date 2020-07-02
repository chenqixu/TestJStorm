package com.cqx.jstorm.message;

import com.cqx.jstorm.bean.HdfsLSBean;
import com.cqx.jstorm.spout.ISpout;

/**
 * FastFailureMessage
 *
 * @author chenqixu
 */
public class FastFailureMessage extends IMessageId {

    private HdfsLSBean hdfsLSBean;

    public FastFailureMessage() {
    }

    public FastFailureMessage(ISpout iSpout, HdfsLSBean hdfsLSBean) {
        setMessageId(iSpout.grenerateMessageId());
        this.hdfsLSBean = hdfsLSBean;
    }

    public HdfsLSBean getHdfsLSBean() {
        return hdfsLSBean;
    }

    public void setHdfsLSBean(HdfsLSBean hdfsLSBean) {
        this.hdfsLSBean = hdfsLSBean;
    }
}
