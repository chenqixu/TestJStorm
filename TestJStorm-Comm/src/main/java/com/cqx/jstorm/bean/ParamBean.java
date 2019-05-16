package com.cqx.jstorm.bean;

import java.util.HashMap;
import java.util.Map;

/**
 * ParamBean
 *
 * @author chenqixu
 */
public class ParamBean extends HashMap<String, Object> {

    public static ParamBean newbuilder() {
        return new ParamBean();
    }

    public ParamBean parser(Object param) {
        Map<String, ?> tmp = (Map<String, ?>) param;
        this.putAll(tmp);
        return this;
    }
}
