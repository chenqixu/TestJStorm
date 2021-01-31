package com.cqx.jstorm.comm.util;

import backtype.storm.Config;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

/**
 * YamlParser
 *
 * @author chenqixu
 */
public class YamlParser {
    private YamlParser() {
    }

    public static YamlParser builder() {
        return new YamlParser();
    }

    /**
     * 解析yaml文件，转换成AppConst
     *
     * @param path
     * @return
     * @throws IOException
     */
    public AppConst parserConf(String path) throws IOException {
        Yaml yaml;
        InputStream is = null;
        Map<?, ?> map;
        AppConst appConst = new AppConst();
        try {
            // 加载yaml配置文件
            yaml = new Yaml();
            URL url = new URL(path);
            is = url.openStream();
            map = yaml.loadAs(is, Map.class);
            is.close();
            // 解析
            appConst.parserParam(map);
        } finally {
            if (is != null)
                is.close();
        }
        return appConst;
    }

    /**
     * 解析Map，转换成AppConst
     *
     * @param map
     * @return
     * @throws IOException
     */
    public AppConst parserMap(Map<Object, Object> map) throws IOException {
        AppConst appConst = new AppConst();
        // 解析
        appConst.parserParam(map);
        return appConst;
    }

    /**
     * 解析yaml文件，转换成Map
     *
     * @param path
     * @return
     * @throws IOException
     */
    public Map parserConfToMap(String path) throws IOException {
        Yaml yaml;
        InputStream is = null;
        Map map;
        try {
            // 加载yaml配置文件
            yaml = new Yaml();
            URL url = new URL(path);
            is = url.openStream();
            map = yaml.loadAs(is, Map.class);
            is.close();
        } finally {
            if (is != null)
                is.close();
        }
        return map;
    }

    /**
     * 设置公共参数
     *
     * @param conf
     * @param appConst
     */
    public void setConf(Map<String, Object> conf, AppConst appConst) {
        // nimbus地址
        conf.put(Config.NIMBUS_HOST, appConst.getJstormBean().getNimbus_host());
        // nimbus thrift端口
        conf.put(Config.NIMBUS_THRIFT_PORT, Integer.valueOf(appConst.getJstormBean().getNimbus_thrift_port()));
        // zookeeper地址
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, appConst.getJstormBean().getStorm_zookeeper_servers());
        // zookeeper端口
        conf.put(Config.STORM_ZOOKEEPER_PORT, appConst.getJstormBean().getStorm_zookeeper_port());
        // zookeeper上jstorm路径
        conf.put(Config.STORM_ZOOKEEPER_ROOT, appConst.getJstormBean().getStorm_zookeeper_root());
        // 组件参数
        conf.putAll(appConst.getParamBean());
    }
}
