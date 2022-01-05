package com.cqx.jstorm.sql.dml;

import com.cqx.jstorm.comm.base.SubmitTopology;
import com.cqx.jstorm.comm.bean.AgentBean;
import com.cqx.jstorm.comm.bean.ReceiveBean;
import com.cqx.jstorm.comm.bean.SendBean;
import com.cqx.jstorm.sql.bean.Table;
import com.cqx.jstorm.sql.ddl.IDDL;
import com.cqx.jstorm.sql.util.AppConst;
import com.cqx.jstorm.sql.util.BoltBuilder;
import com.cqx.jstorm.sql.util.SpoutBuilder;
import com.cqx.jstorm.sql.util.YamlBuilder;
import com.cqx.jstorm.comm.util.ArgsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ExecTable
 *
 * @author chenqixu
 */
public class ExecTable {
    private static final Logger logger = LoggerFactory.getLogger(ExecTable.class);
    private String sql;
    private boolean isThis = false;
    private String source_table;
    private String sink_table;
    private List<String> query_fields = new ArrayList<>();

    public void setSql(String sql) {
        this.sql = sql.toLowerCase().trim();//大小写不敏感
    }

    /**
     * 语法检查
     *
     * @return
     */
    public boolean check() {
        //先判断是否insert开头
        if (this.sql.startsWith("insert ")) {
            parser();
        }
        return isThis;
    }

    public void parser() {
        try {
            String tmp = this.sql;
            //判断是否"insert "开头
            int v_insert = tmp.indexOf("insert ");
            assert v_insert >= 0;
            //判断是否有" into "
            int v_into = tmp.indexOf(" into ");
            assert v_into >= 0;
            //找到" select "
            int v_select = tmp.indexOf(" select ");
            assert v_select >= 0;
            //找到" from "
            int v_from = tmp.indexOf(" from ");
            //获取sink
            sink_table = tmp.substring(v_into + " into ".length(), v_select).trim();
            //获取source
            source_table = tmp.substring(v_from + " from ".length()).trim();
            //获取query_fields
            String _query_fields = tmp.substring(v_select + " select ".length(), v_from).trim();
            String[] query_fields_array = _query_fields.split(",", -1);
            for (String _query_field : query_fields_array) {
                query_fields.add(_query_field.trim());
            }
            logger.info("sink_table：{}，source_table：{}，query_fields：{}", sink_table, source_table, query_fields);
            //解析完成
            isThis = true;
        } catch (Exception e) {
            logger.error("解析异常，内容：" + e.getMessage(), e);
        }
    }

    public Map<Object, Object> buildYaml(Map<String, IDDL> iddlMap) throws Exception {
        IDDL source = iddlMap.get(source_table);
        assert source != null;
        IDDL sink = iddlMap.get(sink_table);
        assert sink != null;

        Table sourceTable = source.getTable();
        String source_connector = source.getWithMap().get("connector");
        String source_connector_type = source.getWithMap().get("connector.type");

        String sink_connector = sink.getWithMap().get("connector");
        String sink_connector_type = sink.getWithMap().get("connector.type");

        YamlBuilder yamlBuilder = new YamlBuilder();
        yamlBuilder.builder();

        //配置source
        yamlBuilder.add_spout(new SpoutBuilder()
                .setName(AppConst.getConnector(source_connector, source_connector_type))
                .setPackagename(AppConst.getConnectorPackageMap(source_connector, source_connector_type))
                .setParall(1)
                .addSendBean(new SendBean(null, query_fields))
                .get()
        );
        yamlBuilder.build_spout();
        yamlBuilder.build_param(source.getWithMap());
        yamlBuilder.add_param("format.content", sourceTable.toAvro());

        //配置sink
        yamlBuilder.add_bolt(new BoltBuilder()
                .setName(AppConst.getConnector(sink_connector, sink_connector_type))
                .setPackagename(AppConst.getConnectorPackageMap(sink_connector, sink_connector_type))
                .setParall(1)
                .setGroupingcode("LOCALFIRSTGROUPING")
                .addReceiveBean(new ReceiveBean(
                        AppConst.getConnector(source_connector, source_connector_type),
                        null, null, query_fields))
                .get()
        );
        yamlBuilder.build_bolt();
        yamlBuilder.build_param(sink.getWithMap());

        logger.info("{}", yamlBuilder.getYaml());
        return yamlBuilder.getYaml();
    }

    /**
     * 获取sink_table和source_table的信息，生成配置，提交任务
     *
     * @param iddlMap
     */
    public void submit(Map<String, IDDL> iddlMap) throws Exception {
        IDDL source = iddlMap.get(source_table);
        assert source != null;
        IDDL sink = iddlMap.get(sink_table);
        assert sink != null;

        Table sourceTable = source.getTable();
        String source_connector = source.getWithMap().get("connector");
        String source_connector_type = source.getWithMap().get("connector.type");

        String sink_connector = sink.getWithMap().get("connector");
        String sink_connector_type = sink.getWithMap().get("connector.type");

        YamlBuilder yamlBuilder = new YamlBuilder();
        yamlBuilder.builder();

        //配置source
        yamlBuilder.add_spout(new SpoutBuilder()
                .setName(AppConst.getConnector(source_connector, source_connector_type))
                .setPackagename(AppConst.getConnectorPackageMap(source_connector, source_connector_type))
                .setParall(1)
                .addSendBean(new SendBean(null, query_fields))
                .get()
        );
        yamlBuilder.build_spout();
        yamlBuilder.build_param(source.getWithMap());
        yamlBuilder.add_param("format.content", sourceTable.toAvro());

        //配置sink
        yamlBuilder.add_bolt(new BoltBuilder()
                .setName(AppConst.getConnector(sink_connector, sink_connector_type))
                .setPackagename(AppConst.getConnectorPackageMap(sink_connector, sink_connector_type))
                .setParall(1)
                .setGroupingcode("LOCALFIRSTGROUPING")
                .addReceiveBean(new ReceiveBean(
                        AppConst.getConnector(source_connector, source_connector_type),
                        null, null, query_fields))
                .get()
        );
        yamlBuilder.build_bolt();
        yamlBuilder.build_param(sink.getWithMap());

        logger.info("{}", yamlBuilder.getYaml());

        // 解析参数
        String[] args = new String[]{"--conf", "null",
                "--type", "remote",
                "--jarpath", "I:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
        ArgsParser argsParser = ArgsParser.builder();
        argsParser.addParam("--conf");
        argsParser.addParam("--type");
        argsParser.addParam("--jarpath");
        argsParser.perser(args);
        AgentBean agentBean = new AgentBean();
        agentBean.setConf(argsParser.getParamValue("--conf"));
        agentBean.setType(argsParser.getParamValue("--type"));
        agentBean.setJarpath(argsParser.getParamValue("--jarpath"));
        logger.info("agentBean：{}", agentBean);
        // 远程提交任务
        SubmitTopology.builder().setAppConst(yamlBuilder.getYaml()).submit(agentBean);
    }
}
