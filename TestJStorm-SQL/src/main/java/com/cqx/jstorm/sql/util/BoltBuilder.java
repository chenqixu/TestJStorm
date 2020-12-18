package com.cqx.jstorm.sql.util;

import com.cqx.jstorm.bean.BoltBean;
import com.cqx.jstorm.bean.GroupingCode;
import com.cqx.jstorm.bean.ReceiveBean;
import com.cqx.jstorm.bean.SendBean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BoltBuilder
 *
 * @author chenqixu
 */
public class BoltBuilder {
    private BoltBean boltBean = new BoltBean();
    private List<Map<Object, Object>> receiveBeanList = new ArrayList<>();
    private List<Map<Object, Object>> sendBeanList = new ArrayList<>();

    public BoltBuilder setName(String name) {
        boltBean.setName(name);
        return this;
    }

    public BoltBuilder setParall(int parall) {
        boltBean.setParall(parall);
        return this;
    }

    public BoltBuilder setGroupingcode(String groupingcode) {
        boltBean.setGroupingcode(GroupingCode.valueOf(groupingcode));
        return this;
    }

    public BoltBuilder setPackagename(String packagename) {
        boltBean.setPackagename(packagename);
        return this;
    }

    public BoltBuilder addReceiveBean(ReceiveBean receiveBean) {
        receiveBeanList.add(receiveBean.toMap());
        return this;
    }

    public BoltBuilder addSendBean(SendBean sendBean) {
        sendBeanList.add(sendBean.toMap());
        return this;
    }

    public Map<Object, Object> get() {
        Map<Object, Object> map = new HashMap<>();
        map.put("name", boltBean.getName());
        map.put("packagename", boltBean.getPackagename());
        map.put("parall", boltBean.getParall());
        map.put("groupingcode", boltBean.getGroupingcode().toString());
        map.put("receive", receiveBeanList);
        map.put("send", sendBeanList);
        return map;
    }
}
