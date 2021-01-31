package com.cqx.jstorm.connector.operator.bean;

import java.util.Map;

/**
 * OperatorBean
 *
 * @author chenqixu
 */
public class OperatorBean {
    private String field_name;
    private String rule;
    private String aliasname;

    public OperatorBean(Map map) {
        setField_name((String) map.get("field_name"));
        setRule((String) map.get("rule"));
        setAliasname((String) map.get("aliasname"));
    }

    public String toString() {
        return "field_name：" + getField_name() + "，rule：" + getRule() + "，aliasname：" + getAliasname();
    }

    public String getField_name() {
        return field_name;
    }

    public void setField_name(String field_name) {
        this.field_name = field_name;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public String getAliasname() {
        return aliasname;
    }

    public void setAliasname(String aliasname) {
        this.aliasname = aliasname;
    }
}
