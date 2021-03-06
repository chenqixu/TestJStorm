package com.cqx.jstorm.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

/**
 * TestBase
 *
 * @author chenqixu
 */
public class TestBase {

    private static final Logger logger = LoggerFactory.getLogger(TestBase.class);

    /**
     * 获取资源的文件路径
     *
     * @param fileName
     * @return
     */
    protected String getResourceClassPath(String fileName) {
        Object obj = new Object();
        URL url = obj.getClass().getResource("/");
        if (url != null) {
            String path = "file://" + url.getPath() + fileName;
            logger.info("加载到配置文件：{}", path);
            return path;
        } else {
            logger.error("加载不到配置文件：{}", fileName);
            return null;
        }
    }
}
