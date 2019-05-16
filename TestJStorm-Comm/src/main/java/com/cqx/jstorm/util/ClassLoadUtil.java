package com.cqx.jstorm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

/**
 * ClassLoadUtil
 * <pre>
 *     用于初始化时候的自定义类加载，比如JAAS认证
 * </pre>
 *
 * @author chenqixu
 */
public class ClassLoadUtil {

    private static Logger logger = LoggerFactory.getLogger(ClassLoadUtil.class);

    public static ClassLoadUtil newbuilder() {
        return new ClassLoadUtil();
    }

    public void load(String className, Map conf) {
        try {
            if (className != null && className.length() > 0) {
                logger.info("load：{}", className);
                Class clazz = Class.forName(className);
                Constructor<? extends IClassLoad> c = clazz.getConstructor(Map.class);
                IClassLoad iClassLoad = c.newInstance(conf);
                iClassLoad.init();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void parser(Object param, Map conf) {
        List<String> tmp = (List<String>) param;
        for (String _class : tmp) {
            load(_class, conf);
        }
    }
}
