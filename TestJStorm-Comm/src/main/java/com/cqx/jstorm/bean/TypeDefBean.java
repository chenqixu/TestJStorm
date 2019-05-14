package com.cqx.jstorm.bean;

import java.util.HashMap;

/**
 * TypeDefBean
 *
 * @author chenqixu
 */
public class TypeDefBean extends HashMap<String, Object> {

    public static TypeDefBean newbuilder() {
        return new TypeDefBean();
    }

    public TypeDefBean parser(String key, Object param) {
        this.put(key, param);
        return this;
    }
}
