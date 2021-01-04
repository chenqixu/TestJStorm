package com.cqx.jstorm.util.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
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
    public static final String KAFKA_HEADER = "kafkaconf.";
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerUtil.class);
    private KafkaProducer<K, V> producer;

    public KafkaProducerUtil(String conf, String kafka_username, String kafka_password) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(conf));
        Configuration.setConfiguration(new SimpleClientConfiguration(kafka_username, kafka_password));
        producer = new KafkaProducer<>(properties);
    }

    public KafkaProducerUtil(Map stormConf) throws IOException {
        Properties properties = initConf(stormConf);
        String kafka_username = properties.getProperty("newland.kafka_username");
        String kafka_password = properties.getProperty("newland.kafka_password");
        Configuration.setConfiguration(new SimpleClientConfiguration(kafka_username, kafka_password));
        producer = new KafkaProducer<>(properties);
    }

    /**
     * 使用内存中的配置来替代每一台的配置文件
     *
     * @param stormConf
     * @param kafka_username
     * @param kafka_password
     * @throws IOException
     */
    public KafkaProducerUtil(Map stormConf, String kafka_username, String kafka_password) throws IOException {
        Properties properties = initConf(stormConf);
        Configuration.setConfiguration(new SimpleClientConfiguration(kafka_username, kafka_password));
        producer = new KafkaProducer<>(properties);
    }

    /**
     * 根据内存中的内存拼接一个配置类
     *
     * @param stormConf
     * @return
     */
    private Properties initConf(Map stormConf) {
        Properties properties = new Properties();
        for (Object entry : stormConf.entrySet()) {
            String key = ((Map.Entry<String, String>) entry).getKey();
            if (key.startsWith(KAFKA_HEADER)) {
                String _key = key.replace(KAFKA_HEADER, "");
                String value = ((Map.Entry<String, String>) entry).getValue();
                logger.info("从参数中获取到的kafka配置的key：{}，value：{}", _key, value);
                properties.setProperty(_key, value);
            }
        }
        return properties;
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
