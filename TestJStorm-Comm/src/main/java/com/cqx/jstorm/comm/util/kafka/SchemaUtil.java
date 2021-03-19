package com.cqx.jstorm.comm.util.kafka;

import com.alibaba.fastjson.JSON;
import com.cqx.jstorm.comm.bean.SchemaBean;
import com.cqx.jstorm.comm.util.http.HttpUtil;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * SchemaUtil
 *
 * @author chenqixu
 */
public class SchemaUtil {
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtil.class);
    private String urlStr;
    private HttpUtil httpUtil;
    private boolean isUseHttpUtil;

    public SchemaUtil(String urlStr) {
        this(urlStr, false);
    }

    public SchemaUtil(String urlStr, boolean isUseHttpUtil) {
        this.urlStr = urlStr;
        this.isUseHttpUtil = isUseHttpUtil;
        logger.info("urlStr：{}，isUseHttpUtil：{}", urlStr, isUseHttpUtil);
        if (isUseHttpUtil) {
            if (!urlStr.endsWith("/")) throw new RuntimeException("使用Http工具类，url不能到具体服务，请修改！");
            httpUtil = new HttpUtil();
        }
    }

    public Schema getSchemaByTopic(String topic) {
        return new Schema.Parser().parse(readUrlContent(topic));
    }

    public Schema getSchemaByString(String str) {
        return new Schema.Parser().parse(str);
    }

    public String readUrlContent(String topic) {
        StringBuilder contentBuffer = new StringBuilder();
        try {
            BufferedReader reader;
            URL url = new URL(urlStr + topic);
            logger.info("{} url：{}", topic, urlStr + topic);
            URLConnection con = url.openConnection();
            reader = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
            String tmpStr;
            while ((tmpStr = reader.readLine()) != null) {
                contentBuffer.append(tmpStr);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        logger.info("{} schema：{}", topic, contentBuffer.toString());
        return contentBuffer.toString();
    }

    public String readUrlContentByHttpUtil(String topic) {
        logger.info("readUrlContent.topic：{}", topic);
        if (isUseHttpUtil) {
            return httpUtil.doGet(urlStr + "getSchema?t=" + topic);
        } else {
            logger.warn("isUseHttpUtil：{}", isUseHttpUtil);
            return null;
        }
    }

    public void updateSchemaByHttpUtil(String topic, String schemaStr) {
        logger.info("updateSchema.topic：{}，schemaStr：{}", topic, schemaStr);
        if (isUseHttpUtil) {
            SchemaBean schemaBean = new SchemaBean();
            schemaBean.setTopic(topic);
            schemaBean.setSchemaStr(JSON.parseObject(schemaStr));
            httpUtil.doPost(urlStr + "updateSchema", JSON.toJSONString(schemaBean));
        } else {
            logger.warn("isUseHttpUtil：{}", isUseHttpUtil);
        }
    }
}
