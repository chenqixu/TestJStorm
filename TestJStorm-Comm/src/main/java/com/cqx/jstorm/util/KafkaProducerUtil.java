package com.cqx.jstorm.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * KafkaProducerUtil
 *
 * @author chenqixu
 */
public class KafkaProducerUtil<K, V> {
    public static final String SECURITY_AUTH = "java.security.auth.login.config";
    private static Logger logger = LoggerFactory.getLogger(KafkaProducerUtil.class);
    private KafkaProducer<K, V> producer;

    public KafkaProducerUtil(String conf) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(conf));
        init(properties);
    }

    public static void initKafkaClientJAAS(Map<String, ?> param) {
        if (param == null) return;
        String conf = (String) param.get("kafkaConfDir");
        if (conf == null || conf.length() == 0) return;
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(conf));
            //java.security.auth.login.config 变量设置
            String propertyAuth = properties.getProperty(SECURITY_AUTH);
            if (propertyAuth != null && !"".equals(propertyAuth)) {
                logger.info("System.setProperty，{} is，{}", SECURITY_AUTH, propertyAuth);
                System.setProperty(SECURITY_AUTH, propertyAuth);
                properties.remove(SECURITY_AUTH);
                logger.info("{} remove from properties file.", SECURITY_AUTH);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void init(Properties properties) {
        //java.security.auth.login.config 变量设置
        String propertyAuth = properties.getProperty(SECURITY_AUTH);
        if (propertyAuth != null && !"".equals(propertyAuth)) {
            logger.info("System.setProperty，{} is，{}", SECURITY_AUTH, propertyAuth);
            System.setProperty(SECURITY_AUTH, propertyAuth);
            properties.remove(SECURITY_AUTH);
            logger.info("{} remove from properties file.", SECURITY_AUTH);
        }
        logger.info("properties：{}", properties);
        producer = new KafkaProducer<>(properties);
    }

    public void send(String topic, K key, V value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public void send(String topic, V value) {
        producer.send(new ProducerRecord<K, V>(topic, value));
    }

    public void release() {
        if (producer != null) producer.close();
    }
}
