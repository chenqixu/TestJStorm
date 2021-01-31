package com.cqx.jstorm.comm.bean;

/**
 * AgentBean
 *
 * @author chenqixu
 */
public class AgentBean {
    private String type;
    private String conf;
    private String jarpath;

    public String getConf() {
        return "file:///" + conf;
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

    public String getJarpath() {
        return jarpath;
    }

    public void setJarpath(String jarpath) {
        this.jarpath = jarpath;
    }

    public String toString() {
        return "conf：" + conf + "，type：" + type + "，jarpath：" + jarpath;
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
