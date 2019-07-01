package com.cqx.jstorm.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * KafkaProducerUtil
 *
 * @author chenqixu
 */
public class KafkaProducerUtil<K, V> implements Serializable {

    private KafkaProducer<K, V> producer;

    public KafkaProducerUtil(String conf, String kafka_username, String kafka_password) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(conf));
        init(properties, kafka_username, kafka_password);
    }

    public void init(Properties properties, String kafka_username, String kafka_password) {
        Configuration.setConfiguration(new SimpleClientConfiguration(kafka_username, kafka_password));
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
