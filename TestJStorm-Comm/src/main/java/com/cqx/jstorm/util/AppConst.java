package com.cqx.jstorm.util;

import com.cqx.jstorm.bean.BoltBean;
import com.cqx.jstorm.bean.JstormBean;
import com.cqx.jstorm.bean.SpoutBean;
import com.cqx.jstorm.bean.TopologyBean;

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
    public static final String SPOUT_IMPL_PACKAGE = "com.cqx.jstorm.spout.impl.";
    public static final String BOLT_IMPL_PACKAGE = "com.cqx.jstorm.bolt.impl.";
    public static final String FIELDS = "common";

    private JstormBean jstormBean;
    private TopologyBean topologyBean;
    private SpoutBean spoutBean;
    private BoltBean boltBean;

    public void parserParam(Map<?, ?> params) {
        jstormBean = JstormBean.newbuilder().parser(params.get(JSTORM));
        topologyBean = TopologyBean.newbuilder().parser(params.get(TOPOLOGY));
        spoutBean = SpoutBean.newbuilder().parser(params.get(SPOUT));
        boltBean = BoltBean.newbuilder().parser(params.get(BOLT));
    }

    public TopologyBean getTopologyBean() {
        return topologyBean;
    }

    public SpoutBean getSpoutBean() {
        return spoutBean;
    }

    public BoltBean getBoltBean() {
        return boltBean;
    }

    public JstormBean getJstormBean() {
        return jstormBean;
    }
}
