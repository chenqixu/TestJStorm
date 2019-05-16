package com.cqx.jstorm.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TypeDef
 *
 * @author chenqixu
 */
public class TypeDef {
    private String name;
    private String keyWord;
    private boolean isDpi;
    private String value;
    private String hwField;
    private String sinkField;
    private String rtmField;
    private String topic;

    public static TypeDef newbuilder() {
        return new TypeDef();
    }

    public static List<TypeDef> parser(Object param) {
        List<Map<String, ?>> parser = (ArrayList<Map<String, ?>>) param;
        List<TypeDef> result = new ArrayList<>();
        for (Map<String, ?> map : parser) {
            result.add(TypeDef.newbuilder().parserMap(map));
        }
        return result;
    }

    public TypeDef parserMap(Map<String, ?> param) {
        setName((String) param.get("name"));
        setKeyWord((String) param.get("keyWord"));
        setDpi((Boolean) param.get("isDpi"));
        setValue((String) param.get("value"));
        setHwField((String) param.get("hwField"));
        setSinkField((String) param.get("sinkField"));
        setRtmField((String) param.get("rtmField"));
        setTopic((String) param.get("topic"));
        return this;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public boolean isDpi() {
        return isDpi;
    }

    public void setDpi(boolean dpi) {
        isDpi = dpi;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getHwField() {
        return hwField;
    }

    public void setHwField(String hwField) {
        this.hwField = hwField;
    }

    public String getSinkField() {
        return sinkField;
    }

    public void setSinkField(String sinkField) {
        this.sinkField = sinkField;
    }

    public String getRtmField() {
        return rtmField;
    }

    public void setRtmField(String rtmField) {
        this.rtmField = rtmField;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
