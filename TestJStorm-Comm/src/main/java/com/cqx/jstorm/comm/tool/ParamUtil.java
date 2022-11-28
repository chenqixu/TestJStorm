package com.cqx.jstorm.comm.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * ParamUtil
 *
 * @author chenqixu
 */
public class ParamUtil {
    private static final Logger logger = LoggerFactory.getLogger(ParamUtil.class);

    public static <T> T setNumberValDefault(Map param, String paramKey, T defaultValue) throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        if (defaultValue == null) throw new NullPointerException(paramKey + "默认值不能为空！");
        Number value = (Number) param.get(paramKey);
        T t = defaultValue;
        if (value != null) {
            String className = defaultValue.getClass().getName();
            Class cls = defaultValue.getClass();
            //参数列表
            Class<?>[] parameterTypes = {String.class};
            //获取参数对应的构造方法
            Constructor<T> constructor = cls.getConstructor(parameterTypes);
            //根据类型设置参数
            switch (className) {
                case "java.lang.Long":
                    //带参构造
                    t = constructor.newInstance(String.valueOf(value.longValue()));
                    break;
                case "java.lang.Integer":
                    //带参构造
                    t = constructor.newInstance(String.valueOf(value.intValue()));
                    break;
                default:
                    break;
            }
        } else {
            logger.info("获取{}配置为空，使用默认值：{}", paramKey, defaultValue);
        }
        return t;
    }

    public static <T> T setValDefault(Map param, String paramKey, T defaultValue) throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        if (defaultValue == null) throw new NullPointerException(paramKey + "默认值不能为空！");
        Object value = param.get(paramKey);
        T t = defaultValue;
        if (value != null) {
            String className = defaultValue.getClass().getName();
            Class cls = defaultValue.getClass();
            //参数列表
            Class<?>[] parameterTypes = {String.class};
            //获取参数对应的构造方法
            Constructor<T> constructor = cls.getConstructor(parameterTypes);
            //根据类型设置参数
            switch (className) {
                case "java.lang.Long":
                    //带参构造
                    t = constructor.newInstance(String.valueOf(((Number) value).longValue()));
                    break;
                case "java.lang.Integer":
                    //带参构造
                    t = constructor.newInstance(String.valueOf(((Number) value).intValue()));
                    break;
                case "java.lang.String":
                    t = constructor.newInstance((String) value);
                    break;
                case "java.lang.Boolean":
                    t = constructor.newInstance(String.valueOf(value));
                    break;
                default:
                    break;
            }
        } else {
            logger.info("获取{}配置为空，使用默认值：{}", paramKey, defaultValue);
        }
        return t;
    }
}
