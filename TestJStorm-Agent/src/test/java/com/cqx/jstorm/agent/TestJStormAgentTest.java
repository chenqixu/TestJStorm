package com.cqx.jstorm.agent;

import com.cqx.jstorm.agent.TestJStormAgent;
import org.junit.Test;

public class TestJStormAgentTest {

    @Test
    public void localsubmit() throws Exception {
        String[] _args = new String[]{"--conf", "I:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\config.local.kafka.yaml",
                "--type", "localsubmit",
                "--jarpath", "I:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
        TestJStormAgent.builder().run(_args);
    }

    @Test
    public void submit() throws Exception {
        String conf_file;
//        conf_file = "config.yaml";
//        conf_file = "config.time.yaml";
//        conf_file = "config.time1.yaml";
//        conf_file = "config.getandsend.yaml";
//        conf_file = "random.remoute.yaml";
        conf_file = "order.yaml";
        String[] _args = new String[]{"--conf", "I:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\" + conf_file,
                "--type", "submit",
                "--jarpath", "I:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
        TestJStormAgent.builder().run(_args);
    }

    @Test
    public void kill() throws Exception {
        String conf_file;
//        conf_file = "config.yaml";
//        conf_file = "config.getandsend.yaml";
        conf_file = "random.remoute.yaml";
        String[] _args = new String[]{"--conf", "I:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\" + conf_file,
                "--type", "kill",
                "--jarpath", "I:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
        TestJStormAgent.builder().run(_args);
    }

    @Test
    public void killDpiParserLTE() throws Exception {
        String[] _args = new String[]{"--conf", "I:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\config.dpi.yaml",
                "--type", "kill",
                "--jarpath", "I:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
        TestJStormAgent.builder().run(_args);
    }

    @Test
    public void sqlRun() throws Exception {
        String source = "create table nmc_tb_lte_s1mme_new(" +
                "city string ," +
                "xdr_id string ," +
                "imsi string ," +
                "imei string ," +
                "msisdn string ," +
                "procedure_type int ," +
                "subprocedure_type string ," +
                "procedure_start_time string ," +
                "procedure_delay_time int ," +
                "procedure_end_time string ," +
                "procedure_status int ," +
                "old_mme_group_id string ," +
                "old_mme_code string ," +
                "lac int ," +
                "tac int ," +
                "cell_id int ," +
                "other_tac string ," +
                "other_eci string ," +
                "home_code int ," +
                "msisdn_home_code int ," +
                "old_mme_group_id_1 string ," +
                "old_mme_code_1 string ," +
                "old_m_tmsi string ," +
                "old_tac string ," +
                "old_eci string ," +
                "cause string ," +
                "keyword string ," +
                "mme_ue_s1ap_id string ," +
                "request_cause string ," +
                "keyword_2 string ," +
                "keyword_3 string ," +
                "keyword_4 string ," +
                "bearer_qci1 int ," +
                "bearer_status1 int ," +
                "bearer_qci2 int ," +
                "bearer_status2 int ," +
                "bearer_qci3 int ," +
                "bearer_status3 int ," +
                "bearer_qci4 int ," +
                "bearer_status4 int ," +
                "bearer_qci5 int ," +
                "bearer_status5 int ," +
                "bearer_qci6 int ," +
                "bearer_status6 int ," +
                "bearer_1_request_cause string ," +
                "bearer_1_failure_cause string ," +
                "bearer_2_request_cause string ," +
                "bearer_2_failure_cause string ," +
                "bearer_3_request_cause string ," +
                "bearer_3_failure_cause string ," +
                "bearer_4_request_cause string ," +
                "bearer_4_failure_cause string ," +
                "bearer_5_request_cause string ," +
                "bearer_5_failure_cause string ," +
                "bearer_6_request_cause string ," +
                "bearer_6_failure_cause string" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'connector.type' = 'source'," +
                "'topic' = 'nmc_tb_lte_s1mme_new'," +
                "'properties.bootstrap.servers' = '10.1.8.200:9092,10.1.8.201:9092,10.1.8.202:9092'," +
                "'properties.group.id' = 'flinkthroughput'," +
                "'properties.security.protocol' = 'SASL_PLAINTEXT'," +
                "'properties.sasl.mechanism' = 'PLAIN'," +
                "'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";'," +
                "'format' = 'avro'," +
                "'scan.startup.mode' = 'fromBeginning'" +
                ")";
        source = "create table nmc_app_nl_loc_mix_v1(" +
                "imsi string ," +
                "imei string ," +
                "msisdn string ," +
                "eventid int ," +
                "btime string ," +
                "lac int ," +
                "ci int ," +
                "msc int " +
                ") with (" +
                "'connector' = 'kafka'," +
                "'connector.type' = 'source'," +
                "'topic' = 'nmc_app_nl_loc_mix_v1'," +
                "'properties.bootstrap.servers' = '10.1.8.200:9092,10.1.8.201:9092,10.1.8.202:9092'," +
                "'properties.group.id' = 'flinkthroughput'," +
                "'properties.security.protocol' = 'SASL_PLAINTEXT'," +
                "'properties.sasl.mechanism' = 'PLAIN'," +
                "'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";'," +
                "'format' = 'avro'," +
                "'scan.startup.mode' = 'fromBeginning'" +
                ")";
        String sink = "create table print_sink(" +
                "xdr_id string," +
                "msisdn string" +
                ") with (" +
                "'connector' = 'print'," +
                "'connector.type' = 'sink'" +
                ")";
        sink = "create table print_sink(" +
                "imsi string ," +
                "imei string ," +
                "msisdn string ," +
                "eventid int ," +
                "btime string ," +
                "lac int ," +
                "ci int ," +
                "msc int " +
                ") with (" +
                "'connector' = 'print'," +
                "'connector.type' = 'sink'" +
                ")";
        String exec_sql = "insert into print_sink select xdr_id,msisdn from nmc_tb_lte_s1mme_new";
        exec_sql = "insert into print_sink select imsi,imei,msisdn,eventid,btime,lac,ci,msc from nmc_app_nl_loc_mix_v1";
        TestJStormAgent.builder().sqlRun(source, sink, exec_sql);
    }
}