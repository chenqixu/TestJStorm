package com.cqx.jstorm.comm.util;

import com.cqx.jstorm.comm.bean.AgentBean;
import com.cqx.jstorm.dpi.bean.TypeDef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class YamlParserTest {

    public static final String EXCEL_SPLIT = "\t";
    public static final String CONF = "I:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\";
    private static Logger logger = LoggerFactory.getLogger(YamlParserTest.class);
    private static Map<String, String> dfgxSave = new HashMap<>();

    static {
//        // lte
//        dfgxSave.put("HTTP", "msisdn,app_class_top,app_class,unknow,eci,imei,tac,procedure_start_time,procedure_end_time,server_ip,destination_port,user_agent,uri,host,http_content_type,upbytes,downbytes,city_1,imsi,delay_time,ownclass,busi_bear_type,mcc,refer_uri,app_content,unknow,unknow,target_action,upflow,downflow,label,apply_classify,apply_name,web_classify,web_name,search_keyword,urlmd5,parser_tag");
//        dfgxSave.put("OTHER", "msisdn,app_class_top,app_class,unknow,eci,imei,tac,procedure_start_time,procedure_end_time,server_ip,destination_port,unknow,unknow,unknow,unknow,upbytes,downbytes,city_1,imsi,delay_time,ownclass,busi_bear_type,mcc,unknow,app_content,unknow,unknow,unknow,upflow,downflow,label,apply_classify,apply_name,web_classify,web_name,search_keyword,urlmd5,parser_tag");
//        dfgxSave.put("RTSP", "msisdn,app_class_top,app_class,unknow,eci,imei,tac,procedure_start_time,procedure_end_time,server_ip,destination_port,user_agent,url,unknow,unknow,upbytes,downbytes,city_1,imsi,delay_time,ownclass,busi_bear_type,mcc,unknow,app_content,unknow,unknow,unknow,upflow,downflow,label,apply_classify,apply_name,web_classify,web_name,search_keyword,urlmd5,parser_tag");
        // gn
        dfgxSave.put("IP", "msisdn,appbigclass,appsmallclass,lac,cid,imei,unknow,start_time,end_time,serverip,serverport,unknow,unknow,unknow,unknow,upflow,downflow,city,imsi,unknow,unknow,business_bearer_type,country_id,unknow,unknow,rac,service_name,unknow,upspeed,downspeed,label,apply_classify,apply_name,web_classify,web_name,search_keyword,urlmd5,parser_tag");
        dfgxSave.put("HTTP", "msisdn,appbigclass,appsmallclass,lac,cid,imei,unknow,start_time,end_time,serverip,serverport,user_agent,uri,host,http_content_type,upflow,downflow,city,imsi,unknow,unknow,unknow,country_id,refer_uri,unknow,rac,service_name,target_behavior,upspeed,downspeed,label,apply_classify,apply_name,web_classify,web_name,search_keyword,urlmd5,parser_tag");
        dfgxSave.put("RTSP", "msisdn,appbigclass,appsmallclass,lac,cid,imei,unknow,start_time,end_time,serverip,serverport,user_agent,url,unknow,unknow,upflow,downflow,city,imsi,unknow,unknow,unknow,country_id,unknow,unknow,rac,service_name,unknow,upspeed,downspeed,label,apply_classify,apply_name,web_classify,web_name,search_keyword,urlmd5,parser_tag");
    }

    private YamlParser yamlParser = YamlParser.builder();
    private AppConst appConst;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void parserConf() throws Exception {
        String[] args = new String[]{"--conf", CONF + "gn.config.yaml",
                "--type", "submit"};
        // 解析参数
        ArgsParser argsParser = ArgsParser.builder();
        argsParser.addParam("--conf");
        argsParser.addParam("--type");
        argsParser.perser(args);
        AgentBean agentBean = new AgentBean();
        agentBean.setConf(argsParser.getParamValue("--conf"));
        agentBean.setType(argsParser.getParamValue("--type"));
        appConst = yamlParser.parserConf(agentBean.getConf());
        Map param = appConst.getParamBean();
        logger.info("param：{}", param);
        for (TypeDef typeDef : TypeDef.parser(param.get("typedefs"))) {
            logger.info("KeyWord：{}，sinkField：{}", typeDef.getKeyWord(), typeDef.getSinkField());
            // 原输出字段
            Map<String, String> sinkField = parser(typeDef.getSinkField());
            if (dfgxSave.get(typeDef.getKeyWord()) == null) continue;
            // 东方国信要求的输出字段
            Map<String, String> save2 = parser(dfgxSave.get(typeDef.getKeyWord()));
            // 2者比较
            diff(sinkField, save2);
            int s1 = typeDef.getSourceFields().length;
            int s2 = typeDef.getHwFields().length;
            String[] s1array = Arrays.copyOf(typeDef.getSourceFields(), s1 + s2);
            System.arraycopy(typeDef.getHwFields(), 0, s1array, s1, s2);
            // 打印excel
            printExcel(s1array, typeDef.getSinkFields(), typeDef.getRtmFields());
        }
    }

    @Test
    public void test() {
        Map<String, String> values = new HashMap<>();
        System.out.println(values.get(null) == null);
    }

    private Map<String, String> parser(String value) {
        if (value == null) throw new NullPointerException("value is null.");
        String[] fields = value.split(",", -1);
        Map<String, String> fieldMap = new LinkedHashMap<>();
        for (String field : fields) {
            fieldMap.put(field, "1");
        }
        return fieldMap;
    }

    private void diff(Map<String, String> sink, Map<String, String> save2) {
        for (Map.Entry<String, String> entry : save2.entrySet()) {
            String key = entry.getKey();
            String value = sink.get(key);
            if (value == null && !key.equals("unknow")) {
                System.out.print(key + ",");
            }
        }
        System.out.println();
    }

    private Map<String, Integer> arrayToLinkedMap(String[] arr) {
        Map<String, Integer> result = new LinkedHashMap<>();
        int index = 1;
        for (String v : arr) {
            result.put(v, index++);
        }
        return result;
    }

    /**
     * STREAM字段名称	是否落地	落地字段顺序	是否推送消息中心	消息字段顺序
     */
    private void printExcel(String[] values, String[] sinkFields, String[] rtmFields) {
        System.out.println("STREAM字段名称\t是否落地\t落地字段顺序\t是否推送消息中心\t消息字段顺序");
        Map<String, Integer> sinkFieldsMap = arrayToLinkedMap(sinkFields);
        Map<String, Integer> rtmFieldsMap = null;
        if (rtmFields != null) rtmFieldsMap = arrayToLinkedMap(rtmFields);
        for (String value : values) {
            Integer sinkIndex = sinkFieldsMap.get(value);
            String isSink = sinkIndex != null ? "是" : "否";
            String sinkIndexs = sinkIndex == null ? "" : sinkIndex.toString();
            System.out.print(value + EXCEL_SPLIT + isSink + EXCEL_SPLIT + sinkIndexs + EXCEL_SPLIT);
            if (rtmFields != null) {
                Integer rtmIndex = rtmFieldsMap.get(value);
                String isRtm = rtmIndex != null ? "是" : "否";
                String rtmIndexs = rtmIndex == null ? "" : rtmIndex.toString();
                System.out.print(isRtm + EXCEL_SPLIT + rtmIndexs);
            }
            System.out.println();
        }
        System.out.println("##############################");
    }
}