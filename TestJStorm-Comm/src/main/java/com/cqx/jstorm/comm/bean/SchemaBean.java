package com.cqx.jstorm.comm.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * SchemaBean
 *
 * @author chenqixu
 */
public class SchemaBean {
    private String topic;
    private JSONObject schemaStr;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public JSONObject getSchemaStr() {
        return schemaStr;
    }

    public void setSchemaStr(JSONObject schemaStr) {
        this.schemaStr = schemaStr;
    }
}
