package com.cqx.jstorm.bean;

import java.util.Map;

/**
 * spout
 *
 * @author chenqixu
 */
public class SpoutBean {
    private String name;
    private int parall;
    private String packagename;

    public static SpoutBean newbuilder() {
        return new SpoutBean();
    }

    public SpoutBean parser(Object param) {
        Map<String, ?> tmp = (Map<String, ?>) param;
        setParall((Integer) tmp.get("parall"));
        setName((String) tmp.get("name"));
        setPackagename((String) tmp.get("packagename"));
        return this;
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

    public String getPackagename() {
        return packagename;
    }

    public void setPackagename(String packagename) {
        this.packagename = packagename;
    }

    public String getGenerateClassName() {
        return getPackagename() + "." + getName();
    }
}
