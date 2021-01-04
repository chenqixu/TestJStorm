package com.cqx.jstorm.util.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * KafkaConsumerUtil
 *
 * @author chenqixu
 */
public class KafkaConsumerUtil<K, V> {
    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerUtil.class);
    private KafkaConsumer<K, V> consumer;
    private String topic;

    public KafkaConsumerUtil(String conf) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(conf));
        init(properties);
    }

    public KafkaConsumerUtil(Map stormConf) throws IOException {
        Properties properties = initConf(stormConf);
        String kafka_username = properties.getProperty("newland.kafka_username");
        String kafka_password = properties.getProperty("newland.kafka_password");
        Configuration.setConfiguration(new SimpleClientConfiguration(kafka_username, kafka_password));
        consumer = new KafkaConsumer<>(properties);
    }

    public KafkaConsumerUtil(String conf, String kafka_username, String kafka_password) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(conf));
        Configuration.setConfiguration(new SimpleClientConfiguration(kafka_username, kafka_password));
        consumer = new KafkaConsumer<>(properties);
    }

    /**
     * 使用内存中的配置来替代每一台的配置文件
     *
     * @param stormConf
     * @param kafka_username
     * @param kafka_password
     * @throws IOException
     */
    public KafkaConsumerUtil(Map stormConf, String kafka_username, String kafka_password) throws IOException {
        Properties properties = initConf(stormConf);
        Configuration.setConfiguration(new SimpleClientConfiguration(kafka_username, kafka_password));
        consumer = new KafkaConsumer<>(properties);
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
            if (key.startsWith(KafkaProducerUtil.KAFKA_HEADER)) {
                String _key = key.replace(KafkaProducerUtil.KAFKA_HEADER, "");
                String value = ((Map.Entry<String, String>) entry).getValue();
                logger.info("从参数中获取到的kafka配置的key：{}，value：{}", _key, value);
                properties.setProperty(_key, value);
            }
        }
        return properties;
    }

    private void init(Properties properties) {
        //java.security.auth.login.config 变量设置
        String propertyAuth = properties.getProperty("java.security.auth.login.config");
        if (propertyAuth != null && !"".equals(propertyAuth)) {
            logger.info("java.security.auth.login.config is not null，{}", propertyAuth);
            System.setProperty("java.security.auth.login.config", propertyAuth);
            properties.remove("java.security.auth.login.config");
            logger.info("java.security.auth.login.config remove from properties");
        }
        consumer = new KafkaConsumer<>(properties);
    }

    /**
     * 订阅话题
     *
     * @param topic
     */
    public void subscribe(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        this.topic = topic;
    }

    /**
     * 消费模式选择，默认从消费组ID继续消费
     *
     * @param mode
     * @throws Exception
     */
    public void modeSet(String mode) throws Exception {
        switch (mode) {
            case "fromBeginning":
                fromBeginning();
                break;
            case "fromEnd":
                fromEnd();
                break;
            case "fromGroupID":
            default:
                fromGroupID();
                break;
        }
    }

    /**
     * 从头开始消费
     *
     * @throws Exception
     */
    public void fromBeginning() throws Exception {
        // 获取当前所有分区
        Set<TopicPartition> assignment = getConsumerAssignment();
        // 指定分区从头消费
        Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(assignment);
        for (TopicPartition tp : assignment) {
            Long offset = beginOffsets.get(tp);
            logger.info("分区 {} 从 {} 开始消费", tp, offset);
            consumer.seek(tp, offset);
        }
    }

    /**
     * 从末尾消费
     *
     * @throws Exception
     */
    public void fromEnd() throws Exception {
        // 获取当前所有分区
        Set<TopicPartition> assignment = getConsumerAssignment();
        // 指定分区从末尾最新位置消费
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
            Long offset = endOffsets.get(tp);
            logger.info("分区 {} 从 {} 开始消费", tp, offset);
            consumer.seek(tp, offset);
        }
    }

    /**
     * 从某个位置开始消费
     *
     * @param offset
     * @throws Exception
     */
    public void fromOffsetng(long offset) throws Exception {
        // 获取当前所有分区
        Set<TopicPartition> assignment = getConsumerAssignment();
        // 指定分区从指定位置消费
        for (TopicPartition tp : assignment) {
            logger.info("分区 {} 从 {} 开始消费", tp, offset);
            consumer.seek(tp, offset);
        }
    }

    /**
     * 从消费组ID开始消费，空实现
     */
    public void fromGroupID() {
    }

    /**
     * 获取当前所有分区
     *
     * @return
     */
    private Set<TopicPartition> getConsumerAssignment() {
        Set<TopicPartition> assignment = new HashSet<>();
        // 在poll()方法内部执行分区分配逻辑，该循环确保分区已被分配。
        // 当分区消息为0时进入此循环，如果不为0，则说明已经成功分配到了分区。
        while (assignment.size() == 0) {
            // poll data from kafka server to prevent lazy operation
            consumer.poll(100);
            // assignment()方法是用来获取消费者所分配到的分区消息的
            // assignment的值为：topic-demo-3, topic-demo-0, topic-demo-2, topic-demo-1
            assignment = consumer.assignment();
        }
        return assignment;
    }

    /**
     * 消费
     *
     * @param timeout
     * @param isCommitSync
     * @return
     */
    public List<V> poll(long timeout, boolean isCommitSync) {
        List<V> resultList = new ArrayList<>();
        ConsumerRecords<K, V> records = consumer.poll(timeout);
        for (ConsumerRecord<K, V> record : records.records(topic)) {
            V msgByte = record.value();
            logger.debug("######## offset = {}, key = {}, value = {}", record.offset(), record.key(), msgByte);
            resultList.add(msgByte);
        }
        if (isCommitSync) {
            // 同步提交 消费偏移量，todo 已经设置了自动提交，其实这里可以不需要
            consumer.commitSync();
        }
        return resultList;
    }

    /**
     * 消费
     *
     * @param timeout
     * @return
     */
    public List<V> poll(long timeout) {
        return poll(timeout, false);
    }

    public void close() {
        // 关闭
        consumer.close();
    }
}
