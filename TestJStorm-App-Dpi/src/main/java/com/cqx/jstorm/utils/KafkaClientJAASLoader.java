package com.cqx.jstorm.utils;

import com.cqx.jstorm.util.IClassLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * KafkaClientJAASLoader
 *
 * @author chenqixu
 */
public class KafkaClientJAASLoader extends IClassLoad {

    public static final String SECURITY_AUTH = "java.security.auth.login.config";
    private static Logger logger = LoggerFactory.getLogger(KafkaClientJAASLoader.class);

    public KafkaClientJAASLoader(Map conf) {
        super(conf);
    }

    @Override
    public void init() {
        if (conf == null) return;
        String kafkaConfDir = (String) conf.get("kafkaConfDir");
        logger.info("kafkaConfDir：{}", kafkaConfDir);
        if (kafkaConfDir == null || kafkaConfDir.length() == 0) return;
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(kafkaConfDir));
            //java.security.auth.login.config 变量设置
            String propertyAuth = properties.getProperty(SECURITY_AUTH);
            if (propertyAuth != null && !"".equals(propertyAuth)) {
                logger.info("System.setProperty，{} is，{}", SECURITY_AUTH, propertyAuth);
                System.setProperty(SECURITY_AUTH, propertyAuth);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
