package com.cqx.jstorm.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * spout
 *
 * @author chenqixu
 */
public class SpoutBean {
    private static final String SINGLE_CLASS = "java.util.LinkedHashMap";
    private static final String MULTIPLE_CLASS = "java.util.ArrayList";
    private String name;
    private int parall;
    private String packagename;

    public static SpoutBean newbuilder() {
        return new SpoutBean();
    }

    public static List<SpoutBean> parser(Object param) {
        List<SpoutBean> result = new ArrayList<>();
        if (param != null) {
            //java.util.ArrayList
            //java.util.LinkedHashMap
            //判断是单个Spout还是多个Spout
            String className = param.getClass().getName();
            if (className.equals(SINGLE_CLASS)) {
                result.add(SpoutBean.newbuilder().parserMap((Map<String, ?>) param));
            } else if (className.equals(MULTIPLE_CLASS)) {
                List<Map<String, ?>> parser = (ArrayList<Map<String, ?>>) param;
                for (Map<String, ?> map : parser) {
                    result.add(SpoutBean.newbuilder().parserMap(map));
                }
            } else {
                throw new UnsupportedOperationException("不支持的spout配置，既不是单个spout的配置也不是多个spout的配置，请检查！");
            }
        } else {
            throw new NullPointerException("spout配置为空，请检查！");
        }
        return result;
    }

    public SpoutBean parserMap(Map<String, ?> param) {
        setParall((Integer) param.get("parall"));
        setName((String) param.get("name"));
        setPackagename((String) param.get("packagename"));
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
