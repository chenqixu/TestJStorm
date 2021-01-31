package com.cqx.jstorm.comm.util.kafka;

/**
 * KafkaSecurityUtil
 *
 * @author chenqixu
 */
public class KafkaSecurityUtil {
    private KafkaSecurity kafkaSecurity;

    public void parser(String sasl_jaas_config) {
        kafkaSecurity = new KafkaSecurity();
        String[] sasl_array = sasl_jaas_config.split(" ", -1);
        for (String str : sasl_array) {
            if (str.startsWith("username=")) {
                String[] username_array = str.split("=", -1);
                kafkaSecurity.setUsername(username_array[1].trim().replaceAll("\"", ""));
            } else if (str.startsWith("password=")) {
                String[] password_array = str.split("=", -1);
                kafkaSecurity.setPassword(password_array[1].trim().replaceAll("\"", "").replaceAll(";", ""));
            }
        }
    }

    public String getUserName() {
        assert kafkaSecurity != null;
        return kafkaSecurity.getUsername();
    }

    public String getPassWord() {
        assert kafkaSecurity != null;
        return kafkaSecurity.getPassword();
    }

    class KafkaSecurity {
        String username;
        String password;

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
