package com.cqx.jstorm.sql.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * YamlBuilder
 *
 * @author chenqixu
 */
public class YamlBuilder {

    private Map<Object, Object> yaml = null;
    private List<Map<Object, Object>> spoutList = new ArrayList<>();
    private List<Map<Object, Object>> boltList = new ArrayList<>();

    public void builder() {
        yaml = new HashMap<>();
        yaml.putAll(build_jstorm());
        yaml.putAll(build_topology());
    }

    private Map<Object, Object> build_jstorm() {
        Map<Object, Object> jstorm = new HashMap<>();
        Map<Object, Object> map = new HashMap<>();
        map.put("nimbus_host", "10.1.8.203");
        map.put("nimbus_thrift_port", 17627);
        List<String> storm_zookeeper_servers = new ArrayList<>();
        storm_zookeeper_servers.add("10.1.8.198");
        storm_zookeeper_servers.add("10.1.4.185");
        storm_zookeeper_servers.add("10.1.4.186");
        map.put("storm_zookeeper_servers", storm_zookeeper_servers);
        map.put("storm_zookeeper_port", 2183);
        map.put("storm_zookeeper_root", "/udap/collect_jstorm");
        jstorm.put("jstorm", map);
        return jstorm;
    }

    private Map<Object, Object> build_topology() {
        Map<Object, Object> topology = new HashMap<>();
        Map<Object, Object> map = new HashMap<>();
        map.put("worker_num", 1);
        map.put("ack_num", 1);
        map.put("cpu_slotNum", 100);
        map.put("worker_memory", 2147483648L);
        map.put("name", "kafka_print");
        map.put("ip", "10.1.8.203");
        topology.put("topology", map);
        return topology;
    }

    public void add_spout(Map<Object, Object> map) {
        spoutList.add(map);
    }

    public void build_spout() {
        yaml.put("spout", spoutList);
    }

    public void add_bolt(Map<Object, Object> map) {
        boltList.add(map);
    }

    public void build_bolt() {
        yaml.put("bolt", boltList);
    }

    public void build_param(Map<?, ?> params) {
        if (yaml != null) {
            Object param = yaml.get("param");
            if (param != null) {
                ((Map<Object, Object>) param).putAll(params);
            } else {
                yaml.put("param", params);
            }
        }
    }

    public void add_param(String key, String value) {
        if (yaml != null) {
            Object param = yaml.get("param");
            if (param == null) {
                param = new HashMap<>();
                yaml.put("param", param);
            }
            ((Map<Object, Object>) param).put(key, value);
        }
    }

    public Map<Object, Object> getYaml() {
        return yaml;
    }
}
