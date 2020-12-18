package com.cqx.jstorm.sql.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParserUtilTest {

    private ParserUtil parserUtil;
    private String create_source_table_sql = "create table nmc_etl_nokia_mc_mc_v1(" +
            "BTIME string," +
            "ETIME string," +
            "GLOBALID BIGINT," +
            "PROTOCOLID int," +
            "EVENTID int," +
            "MSCCODE int," +
            "LAC int," +
            "CI int," +
            "OLAC int," +
            "OCI int," +
            "DLAC int," +
            "DCI int," +
            "FIRSTLAC int," +
            "FIRSTCI int," +
            "LASTLAC int," +
            "LASTCI int," +
            "CALLINGNUM string," +
            "CALLEDNUM string," +
            "CALLINGIMSI string," +
            "CALLEDIMSI string," +
            "CALLINGIMEI string," +
            "CALLEDIMEI string," +
            "CALLINGTMSI string," +
            "CALLEDTMSI string," +
            "EVENTRESULT int," +
            "ALERTOFFSET int," +
            "CONNOFFSET int," +
            "DISCONDIRECT int," +
            "DISCONNOFFSET int," +
            "ANSWERDUR int," +
            "PAGINGRESPTYPE int," +
            "ALERTSTATUS int," +
            "CONSTATUS int," +
            "DISCONNSTATUS int," +
            "DISCONNCAUSE int," +
            "RELCAUSE int," +
            "HOFLAG int," +
            "Callingnumnature string," +
            "Callednumnature string," +
            "CALLING_CITY int," +
            "CALLING_COUNTY int," +
            "CALLED_CITY int," +
            "CALLED_COUNTY int," +
            "CALL_COUNTY int," +
            "FIRST_CALL_COUNTY int," +
            "LAST_CALL_COUNTY int," +
            "CDRID string," +
            "SESSIONID string," +
            "SPCKIND string" +
            ") with (" +
            "'connector' = 'kafka'," +
            "'connector.type' = 'source'," +
            "'topic' = 'nmc_etl_nokia_mc_mc_v1'," +
            "'properties.bootstrap.servers' = '10.48.137.217:9092'," +
            "'properties.group.id' = 'flinkthroughput'," +
            "'properties.security.protocol' = 'SASL_PLAINTEXT'," +
            "'properties.sasl.mechanism' = 'PLAIN'," +
            "'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";'," +
            "'format' = 'avro'," +
            "'scan.startup.mode' = 'earliest-offset'" +
            ")";
    private String create_source_table_s1mme_sql = "create table nmc_tb_lte_s1mme_new(" +
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
            "'properties.bootstrap.servers' = '10.48.137.217:9092'," +
            "'properties.group.id' = 'flinkthroughput'," +
            "'properties.security.protocol' = 'SASL_PLAINTEXT'," +
            "'properties.sasl.mechanism' = 'PLAIN'," +
            "'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";'," +
            "'format' = 'avro'," +
            "'scan.startup.mode' = 'fromBeginning'" +
            ")";
    private String create_sink_table_sql = "create table print_sink(" +
            "xdr_id string," +
            "msisdn string" +
            ") with (" +
            "'connector' = 'print'," +
            "'connector.type' = 'sink'" +
            ")";
    private String exec_sql = "insert into print_sink select xdr_id,msisdn from nmc_tb_lte_s1mme_new";

    @Before
    public void setUp() throws Exception {
        parserUtil = new ParserUtil();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void exec() throws Exception {
//        parserUtil.exec(create_source_table_sql);
        parserUtil.exec(create_source_table_s1mme_sql);
        parserUtil.exec(create_sink_table_sql);
        parserUtil.exec(exec_sql);
    }
}