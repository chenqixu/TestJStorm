package com.cqx.jstorm.sql.util;

import java.util.HashMap;
import java.util.Map;

/**
 * AppConst
 *
 * @author chenqixu
 */
public class AppConst {
    private static Map<String, String> connectorMap = new HashMap<>();
    private static Map<String, String> connectorPackageMap = new HashMap<>();

    static {
        connectorMap.put("kafka|source", "KafkaSourceConnector");
        connectorMap.put("print|sink", "PrintSinkConnector");

        connectorPackageMap.put("kafka|source", "com.cqx.jstorm.connector.kafka");
        connectorPackageMap.put("print|sink", "com.cqx.jstorm.connector.sys");
    }

    public static String getConnector(String connector, String connector_type) {
        return connectorMap.get(connector + "|" + connector_type);
    }

    public static String getConnectorPackageMap(String connector, String connector_type) {
        return connectorPackageMap.get(connector + "|" + connector_type);
    }
}
