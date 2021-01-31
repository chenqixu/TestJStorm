package com.cqx.jstorm.dpi.message;

import com.cqx.jstorm.dpi.bean.HdfsLSBean;
import com.cqx.jstorm.comm.message.IMessageId;
import com.cqx.jstorm.comm.spout.ISpout;

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
