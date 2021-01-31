package com.cqx.jstorm.comm.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * bolt
 *
 * @author chenqixu
 */
public class BoltBean {
    private String name;
    private String aliasname;//别名
    private String packagename;
    private int parall;
    private GroupingCode groupingcode;
    private List<ReceiveBean> receiveBeanList;
    private List<SendBean> sendBeanList;

    public static BoltBean newbuilder() {
        return new BoltBean();
    }

    public static List<BoltBean> parser(Object param) {
        List<BoltBean> result = new ArrayList<>();
        if (param != null) {
            List<Map<String, ?>> parser = (ArrayList<Map<String, ?>>) param;
            for (Map<String, ?> map : parser) {
                result.add(BoltBean.newbuilder().parserMap(map));
            }
        }
        return result;
    }

    public BoltBean parserMap(Map<String, ?> param) {
        setName((String) param.get("name"));
        setAliasname((String) param.get("aliasname"));
        setPackagename((String) param.get("packagename"));
        setParall((Integer) param.get("parall"));
        setGroupingcode(GroupingCode.valueOf((String) param.get("groupingcode")));
        //定义接收对象
        Object receive = param.get("receive");
        if (receive != null) {
            receiveBeanList = new ArrayList<>();
            List _receiveBeanList = (ArrayList) receive;
            for (Object object : _receiveBeanList) {
                receiveBeanList.add(new ReceiveBean((Map) object));
            }
        }
        //定义发送对象
        Object send = param.get("send");
        if (send != null) {
            sendBeanList = new ArrayList<>();
            List _sendBeanList = (ArrayList) send;
            for (Object object : _sendBeanList) {
                sendBeanList.add(new SendBean((Map) object));
            }
        }
        return this;
    }

    public String getGenerateClassName() {
        return getPackagename() + "." + getName();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getParall() {
        return parall;
    }

    public void setParall(int parall) {
        this.parall = parall;
    }

    public GroupingCode getGroupingcode() {
        return groupingcode;
    }

    public void setGroupingcode(GroupingCode groupingcode) {
        this.groupingcode = groupingcode;
    }

    public String getPackagename() {
        return packagename;
    }

    public void setPackagename(String packagename) {
        this.packagename = packagename;
    }

    public List<ReceiveBean> getReceiveBeanList() {
        return receiveBeanList;
    }

    public void setReceiveBeanList(List<ReceiveBean> receiveBeanList) {
        this.receiveBeanList = receiveBeanList;
    }

    public List<SendBean> getSendBeanList() {
        return sendBeanList;
    }

    public void setSendBeanList(List<SendBean> sendBeanList) {
        this.sendBeanList = sendBeanList;
    }

    public String getAliasname() {
        if (aliasname == null || aliasname.length() == 0) return getName();
        else return aliasname;
    }

    public void setAliasname(String aliasname) {
        this.aliasname = aliasname;
    }
}
