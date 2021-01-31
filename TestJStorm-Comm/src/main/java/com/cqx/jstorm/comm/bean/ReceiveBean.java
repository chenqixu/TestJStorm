package com.cqx.jstorm.comm.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ReceiveBean
 *
 * @author chenqixu
 */
public class ReceiveBean implements Serializable {
    private String componentId;
    private String streamId;
    private List<String> fieldsgrouping_fields;
    private List<String> output_fields;

    public ReceiveBean() {
    }

    public ReceiveBean(String componentId, String streamId, List<String> fieldsgrouping_fields, List<String> output_fields) {
        this.componentId = componentId;
        this.streamId = streamId;
        this.fieldsgrouping_fields = fieldsgrouping_fields;
        this.output_fields = output_fields;
    }

    public ReceiveBean(Map map) {
        setComponentId((String) map.get("componentId"));
        setStreamId((String) map.get("streamId"));
        setFieldsgrouping_fields((ArrayList<String>) map.get("fieldsgrouping_fields"));
        setOutput_fields((ArrayList<String>) map.get("output_fields"));
    }

    public Map<Object, Object> toMap() {
        Map<Object, Object> map = new HashMap<>();
        map.put("componentId", getComponentId());
        map.put("streamId", getStreamId());
        map.put("fieldsgrouping_fields", getFieldsgrouping_fields());
        map.put("output_fields", getOutput_fields());
        return map;
    }

    @Override
    public String toString() {
        return "componentId：" + getComponentId() + "，streamId：" + getStreamId() + "，fieldsgrouping_fields：" + getFieldsgrouping_fields() + "，output_fields：" + getOutput_fields();
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public List<String> getFieldsgrouping_fields() {
        return fieldsgrouping_fields;
    }

    public void setFieldsgrouping_fields(List<String> fieldsgrouping_fields) {
        this.fieldsgrouping_fields = fieldsgrouping_fields;
    }

    public List<String> getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(List<String> output_fields) {
        this.output_fields = output_fields;
    }
}
