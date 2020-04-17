package com.cqx.jstorm.util;

import com.cqx.jstorm.bean.*;

import java.util.List;
import java.util.Map;

/**
 * 常量
 *
 * @author chenqixu
 */
public class AppConst {
    public static final String JSTORM = "jstorm";
    public static final String TOPOLOGY = "topology";
    public static final String SPOUT = "spout";
    public static final String BOLT = "bolt";
    public static final String PARAM = "param";
    public static final String CLASSLOAD = "classload";
    public static final String TYPEDEFS = "typedefs";
    public static final String FIELDS = "common";

    private JstormBean jstormBean;
    private TopologyBean topologyBean;
    private List<SpoutBean> spoutBeanList;
    private List<BoltBean> boltBeanList;
    private ParamBean paramBean;

    public void parserParam(Map<?, ?> params) {
        jstormBean = JstormBean.newbuilder().parser(params.get(JSTORM));
        topologyBean = TopologyBean.newbuilder().parser(params.get(TOPOLOGY));
        spoutBeanList = SpoutBean.parser(params.get(SPOUT));
        boltBeanList = BoltBean.parser(params.get(BOLT));
        paramBean = ParamBean.newbuilder().parser(params.get(PARAM));
        // 加载自定义类
        ClassLoadUtil.newbuilder().parser(params.get(CLASSLOAD), paramBean);
    }

    public TopologyBean getTopologyBean() {
        return topologyBean;
    }

    public List<SpoutBean> getSpoutBeanList() {
        return spoutBeanList;
    }

    public JstormBean getJstormBean() {
        return jstormBean;
    }

    public ParamBean getParamBean() {
        return paramBean;
    }

    public List<BoltBean> getBoltBeanList() {
        return boltBeanList;
    }
}
