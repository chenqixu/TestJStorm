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
    public static final String SPOUT_IMPL_PACKAGE = "com.cqx.jstorm.spout.impl.";
    public static final String BOLT_IMPL_PACKAGE = "com.cqx.jstorm.bolt.impl.";
    public static final String FIELDS = "common";
    public static final String SOURDIR = "sourDir";
    public static final String BACKDIR = "backDir";
    public static final String SINKDIR = "sinkDir";
    public static final String TEMPDIR = "tempDir";
    public static final String ERRORDIR = "errorDir";
    public static final String SEPARATOR = "separator";
    public static final String DATELOCAL = "dateLocal";
    public static final String NAMESEPARATOR = "nameSeparator";

    private JstormBean jstormBean;
    private TopologyBean topologyBean;
    private SpoutBean spoutBean;
    private List<BoltBean> boltBeanList;
    private ParamBean paramBean;

    public void parserParam(Map<?, ?> params) {
        jstormBean = JstormBean.newbuilder().parser(params.get(JSTORM));
        topologyBean = TopologyBean.newbuilder().parser(params.get(TOPOLOGY));
        spoutBean = SpoutBean.newbuilder().parser(params.get(SPOUT));
        boltBeanList = BoltBean.parser(params.get(BOLT));
        paramBean = ParamBean.newbuilder().parser(params.get(PARAM));
        // 加载自定义类
        ClassLoadUtil.newbuilder().parser(params.get(CLASSLOAD), paramBean);
    }

    public TopologyBean getTopologyBean() {
        return topologyBean;
    }

    public SpoutBean getSpoutBean() {
        return spoutBean;
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
