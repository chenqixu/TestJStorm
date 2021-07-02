package com.cqx.jstorm.comm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static String getNow() {
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(now);
    }

    public static String getNow(String format) {
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(now);
    }

    /**
     * 获取资源的文件路径
     *
     * @param fileName
     * @return
     */
    public static String getResourceClassPath(String fileName, boolean isAddFilePre) {
        Object obj = new Object();
        URL url = obj.getClass().getResource("/");
        if (url != null) {
            String urlPath = url.getPath();
            if (!isAddFilePre && urlPath.startsWith("/")) urlPath = urlPath.substring(1);
            String path = (!isAddFilePre ? "" : "file://") + urlPath + fileName;
            logger.info("加载到配置文件：{}", path);
            return path;
        } else {
            logger.error("加载不到配置文件：{}", fileName);
            return null;
        }
    }

    /**
     * 获取资源的文件路径
     *
     * @param fileName
     * @return
     */
    public static String getResourceClassPath(String fileName) {
        return getResourceClassPath(fileName, false);
    }
}
