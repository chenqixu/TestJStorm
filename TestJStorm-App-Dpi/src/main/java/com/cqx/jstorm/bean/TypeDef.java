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
    private String value;// 总字段
    private String dpirequestField;// dpi发送请求字段
    private String hwField;// dpi去掉发送请求字段后的返回识别字段
    private String sinkField;// 落地字段
    private String rtmField;// kafka字段
    private String rtmkey;// kafka.key
    private String topic;// 话题

    private String[] sourceFields;
    private String[] hwFields;
    private String[] sendFields;
    private String[] dpireceiveFields;
    private String[] sinkFields;
    private String[] rtmFields;

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
        // 解析参数
        setName((String) param.get("name"));
        setKeyWord((String) param.get("keyWord"));
        setDpi((Boolean) param.get("isDpi"));
        setValue((String) param.get("value"));
        setDpirequestField((String) param.get("dpirequestField"));
        setHwField((String) param.get("hwField"));
        setSinkField((String) param.get("sinkField"));
        setRtmField((String) param.get("rtmField"));
        setRtmkey((String) param.get("rtmkey"));
        setTopic((String) param.get("topic"));
        // 分解
        // 分解源字段
        checkValue("源字段", getValue(), false);
        if (getValue() != null && getValue().length() > 0)
            setSourceFields(getValue().split(",", -1));
        // 分解DPI字段
        checkValue("DPI字段", getHwField(), true);
        if (getHwField() != null && getHwField().length() > 0)
            setHwFields(getHwField().split(",", -1));
        // 分解发送字段
        checkValue("发送字段", getDpirequestField(), true);
        if (getDpirequestField() != null && getDpirequestField().length() > 0)
            setSendFields(getDpirequestField().split(",", -1));
        // 分解返回字段
        checkValue("返回字段", getHwField(), true);
        if (getHwField() != null && getHwField().length() > 0)
            setDpireceiveFields(getHwField().split(",", -1));
        // 分解落地字段
        checkValue("落地字段", getSinkField(), false);
        if (getSinkField() != null && getSinkField().length() > 0)
            setSinkFields(getSinkField().split(",", -1));
        // 分解入kafka字段
        if (getRtmField() != null && getRtmField().length() > 0)
            setRtmFields(getRtmField().split(",", -1));
        return this;
    }

    private void checkValue(String tag, String value, boolean isDpiMust) {
        // dpi必填
        if (isDpiMust) {
            // 配置有配置dpi的情况下才进行验证
            if (isDpi() && (value == null || value.length() == 0)) {
                throw new NullPointerException(tag + " value is null，please check！");
            }
        } else if (value == null || value.length() == 0) { // 非dpi必填
            throw new NullPointerException(tag + " value is null，please check！");
        }
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

    public String getDpirequestField() {
        return dpirequestField;
    }

    public void setDpirequestField(String dpirequestField) {
        this.dpirequestField = dpirequestField;
    }

    public String[] getSourceFields() {
        return sourceFields;
    }

    public void setSourceFields(String[] sourceFields) {
        this.sourceFields = sourceFields;
    }

    public String[] getSendFields() {
        return sendFields;
    }

    public void setSendFields(String[] sendFields) {
        this.sendFields = sendFields;
    }

    public String[] getDpireceiveFields() {
        return dpireceiveFields;
    }

    public void setDpireceiveFields(String[] dpireceiveFields) {
        this.dpireceiveFields = dpireceiveFields;
    }

    public String[] getSinkFields() {
        return sinkFields;
    }

    public void setSinkFields(String[] sinkFields) {
        this.sinkFields = sinkFields;
    }

    public String[] getRtmFields() {
        return rtmFields;
    }

    public void setRtmFields(String[] rtmFields) {
        this.rtmFields = rtmFields;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getRtmkey() {
        return rtmkey;
    }

    public void setRtmkey(String rtmkey) {
        this.rtmkey = rtmkey;
    }

    public String[] getHwFields() {
        return hwFields;
    }

    public void setHwFields(String[] hwFields) {
        this.hwFields = hwFields;
    }
}