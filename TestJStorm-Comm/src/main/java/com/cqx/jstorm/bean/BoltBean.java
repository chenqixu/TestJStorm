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
    private String[] componentId;
    private String[] streamId;

    public static BoltBean newbuilder() {
        return new BoltBean();
    }

    public static List<BoltBean> parser(Object param) {
        List<BoltBean> result = new ArrayList<>();
        if (param != null) {
            List<Map<String, ?>> parser = (ArrayList<Map<String, ?>>) param;
            for (Map<String, ?> map : parser) {
                result.add(BoltBean.newbuilder().parserMap(map));
            }
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
        // 如果streamid和componentid都不为空，则长度要一一对应
        if (streamId != null && componentId != null) {
            if (streamId.length != componentId.length)
                throw new RuntimeException("BoltName：" + getName() + "，streamid和componentid长度不一致，请检查！");
        }
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

    public String[] getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        if (componentId != null && componentId.length() > 0) {
            this.componentId = componentId.split(",", -1);
        }
    }

    public String[] getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        if (streamId != null && streamId.length() > 0) {
            this.streamId = streamId.split(",", -1);
        }
    }
}
