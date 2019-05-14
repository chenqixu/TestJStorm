package com.cqx.jstorm.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * bolt
 *
 * @author chenqixu
 */
public class BoltBean {
    private String name;
    private String packagename;
    private int parall;
    private GroupingCode groupingcode;
    private String componentId;
    private String streamId;

    public static BoltBean newbuilder() {
        return new BoltBean();
    }

    public static List<BoltBean> parser(Object param) {
        List<Map<String, ?>> parser = (ArrayList<Map<String, ?>>) param;
        List<BoltBean> result = new ArrayList<>();
        for (Map<String, ?> map : parser) {
            result.add(BoltBean.newbuilder().parserMap(map));
        }
        return result;
    }

    public BoltBean parserMap(Map<String, ?> param) {
        setName((String) param.get("name"));
        setPackagename((String) param.get("packagename"));
        setParall((Integer) param.get("parall"));
        setGroupingcode(GroupingCode.valueOf((String) param.get("groupingcode")));
        setComponentId((String) param.get("componentId"));
        setStreamId((String) param.get("streamId"));
        return this;
    }

    public String getGenerateClassName() {
        return getPackagename() + "." + getName();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getParall() {
        return parall;
    }

    public void setParall(int parall) {
        this.parall = parall;
    }

    public GroupingCode getGroupingcode() {
        return groupingcode;
    }

    public void setGroupingcode(GroupingCode groupingcode) {
        this.groupingcode = groupingcode;
    }

    public String getPackagename() {
        return packagename;
    }

    public void setPackagename(String packagename) {
        this.packagename = packagename;
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
}
