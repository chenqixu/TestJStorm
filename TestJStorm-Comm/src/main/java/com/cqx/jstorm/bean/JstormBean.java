package com.cqx.jstorm.bean;

import java.util.List;
import java.util.Map;

/**
 * JstormBean
 *
 * @author chenqixu
 */
public class JstormBean {
    private String nimbus_host;
    private int nimbus_thrift_port;
    private List<String> storm_zookeeper_servers;
    private int storm_zookeeper_port;
    private String storm_zookeeper_root;

    public static JstormBean newbuilder() {
        return new JstormBean();
    }

    public JstormBean parser(Object param) {
        Map<String, ?> tmp = (Map<String, ?>) param;
        nimbus_host = (String) tmp.get("nimbus_host");
        nimbus_thrift_port = (Integer) tmp.get("nimbus_thrift_port");
        storm_zookeeper_servers = (List) tmp.get("storm_zookeeper_servers");
        storm_zookeeper_port = (Integer) tmp.get("storm_zookeeper_port");
        storm_zookeeper_root = (String) tmp.get("storm_zookeeper_root");
        return this;
    }

    public String getNimbus_host() {
        return nimbus_host;
    }

    public void setNimbus_host(String nimbus_host) {
        this.nimbus_host = nimbus_host;
    }

    public int getNimbus_thrift_port() {
        return nimbus_thrift_port;
    }

    public void setNimbus_thrift_port(int nimbus_thrift_port) {
        this.nimbus_thrift_port = nimbus_thrift_port;
    }

    public List<String> getStorm_zookeeper_servers() {
        return storm_zookeeper_servers;
    }

    public void setStorm_zookeeper_servers(List<String> storm_zookeeper_servers) {
        this.storm_zookeeper_servers = storm_zookeeper_servers;
    }

    public int getStorm_zookeeper_port() {
        return storm_zookeeper_port;
    }

    public void setStorm_zookeeper_port(int storm_zookeeper_port) {
        this.storm_zookeeper_port = storm_zookeeper_port;
    }

    public String getStorm_zookeeper_root() {
        return storm_zookeeper_root;
    }

    public void setStorm_zookeeper_root(String storm_zookeeper_root) {
        this.storm_zookeeper_root = storm_zookeeper_root;
    }
}
