package com.cqx.jstorm.bean;

/**
 * AgentBean
 *
 * @author chenqixu
 */
public class AgentBean {
    private String type;
    private String conf;

    public String getConf() {
        if (isWindow())
            return "file:///" + conf;
        else
            return conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toString() {
        return "conf：" + conf + "，type：" + type;
    }

    /**
     * 是否是本地测试
     *
     * @return
     */
    private boolean isWindow() {
        String systemType = System.getProperty("os.name");
        if (systemType.toUpperCase().startsWith("WINDOWS")) {
            return true;
        } else {
            return false;
        }
    }
}
