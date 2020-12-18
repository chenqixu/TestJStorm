package com.cqx.jstorm.sql.util;

import com.cqx.jstorm.bean.SendBean;
import com.cqx.jstorm.bean.SpoutBean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SpoutBuilder
 *
 * @author chenqixu
 */
public class SpoutBuilder {
    private SpoutBean spoutBean = new SpoutBean();
    private List<Map<Object, Object>> sendBeanList = new ArrayList<>();

    public SpoutBuilder setName(String name) {
        spoutBean.setName(name);
        return this;
    }

    public SpoutBuilder setParall(int parall) {
        spoutBean.setParall(parall);
        return this;
    }

    public SpoutBuilder setPackagename(String packagename) {
        spoutBean.setPackagename(packagename);
        return this;
    }

    public SpoutBuilder addSendBean(SendBean sendBean) {
        sendBeanList.add(sendBean.toMap());
        return this;
    }

    public Map<Object, Object> get() {
        Map<Object, Object> map = new HashMap<>();
        map.put("name", spoutBean.getName());
        map.put("packagename", spoutBean.getPackagename());
        map.put("parall", spoutBean.getParall());
        map.put("send", sendBeanList);
        return map;
    }
}
