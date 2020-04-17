package com.cqx.jstorm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * IpUtils
 *
 * @author chenqixu
 */
public class IpUtils {
    public static String ip = null;
    public static String hostName = null;
    private static Logger logger = LoggerFactory.getLogger(IpUtils.class);

    static {
        init();
    }

    public static void init() {
        InetAddress addr;
        try {
            addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress();
            hostName = addr.getHostAddress();
        } catch (Exception e) {
            logger.error("无法获取IP地址", e);
            ip = "无法获取IP地址";
        }
    }
}
