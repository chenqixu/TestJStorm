package com.cqx.jstorm.bean;

/**
 * DpiParserBean
 *
 * @author chenqixu
 */
public class DpiParserBean {
    // 端口分隔符
    public static final String SPLIT = ",";

    private String[] dpiPorts;
    private String bolt_num;
    private String sourDir;
    private String backDir;
    private String sinkDir;
    private String tempDir;
    private String errorDir;
    private String separator;
    private String senddpiseparator;
    private String dateLocal;
    private String nameSeparator;
    private String template_id;
    private String maxqueuenum;
    private String endwith;

    public static DpiParserBean builder() {
        return new DpiParserBean();
    }

    public String[] getDpiPorts() {
        return dpiPorts;
    }

    public void setDpiPorts(String dpiPorts) {
        if (dpiPorts != null && dpiPorts.length() > 0)
            this.dpiPorts = dpiPorts.split(SPLIT, -1);
    }

    public String getBolt_num() {
        return bolt_num;
    }

    public void setBolt_num(String bolt_num) {
        this.bolt_num = bolt_num;
    }

    public String getSourDir() {
        return sourDir;
    }

    public void setSourDir(String sourDir) {
        this.sourDir = sourDir;
    }

    public String getBackDir() {
        return backDir;
    }

    public void setBackDir(String backDir) {
        this.backDir = backDir;
    }

    public String getSinkDir() {
        return sinkDir;
    }

    public void setSinkDir(String sinkDir) {
        this.sinkDir = sinkDir;
    }

    public String getTempDir() {
        return tempDir;
    }

    public void setTempDir(String tempDir) {
        this.tempDir = tempDir;
    }

    public String getErrorDir() {
        return errorDir;
    }

    public void setErrorDir(String errorDir) {
        this.errorDir = errorDir;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getSenddpiseparator() {
        return senddpiseparator;
    }

    public void setSenddpiseparator(String senddpiseparator) {
        this.senddpiseparator = senddpiseparator;
    }

    public String getDateLocal() {
        return dateLocal;
    }

    public void setDateLocal(String dateLocal) {
        this.dateLocal = dateLocal;
    }

    public String getNameSeparator() {
        return nameSeparator;
    }

    public void setNameSeparator(String nameSeparator) {
        this.nameSeparator = nameSeparator;
    }

    public String getTemplate_id() {
        return template_id;
    }

    public void setTemplate_id(String template_id) {
        this.template_id = template_id;
    }

    public String getMaxqueuenum() {
        return maxqueuenum;
    }

    public void setMaxqueuenum(String maxqueuenum) {
        this.maxqueuenum = maxqueuenum;
    }

    public String getEndwith() {
        return endwith;
    }

    public void setEndwith(String endwith) {
        this.endwith = endwith;
    }
}
