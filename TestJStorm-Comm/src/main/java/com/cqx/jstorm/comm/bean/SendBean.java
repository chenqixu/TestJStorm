package com.cqx.jstorm.comm.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SendBean
 *
 * @author chenqixu
 */
public class SendBean implements Serializable {
    private String streamId;
    private List<String> output_fields;

    public SendBean() {
    }

    public SendBean(String streamId, List<String> output_fields) {
        this.streamId = streamId;
        this.output_fields = output_fields;
    }

    public SendBean(Map map) {
        setStreamId((String) map.get("streamId"));
        setOutput_fields((ArrayList<String>) map.get("output_fields"));
    }

    public Map<Object, Object> toMap() {
        Map<Object, Object> map = new HashMap<>();
        map.put("streamId", getStreamId());
        map.put("output_fields", getOutput_fields());
        return map;
    }

    @Override
    public String toString() {
        return "streamId：" + getStreamId() + "，output_fields：" + getOutput_fields();
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public List<String> getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(List<String> output_fields) {
        this.output_fields = output_fields;
    }
}
