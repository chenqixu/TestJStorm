package com.cqx.jstorm.sql.bean;

import org.junit.Test;

public class TableTest {

    @Test
    public void parser() {
        Table table = new Table();
        table.setName("t1");
        table.addColumn("a", "string");
        table.addColumn("b", "string");
        System.out.println(table.toJSON());
        Table t2 = Table.JSONToBean(table.toJSON());
        System.out.println(t2.getName());
        System.out.println(t2.getColumnList());
        System.out.println(t2.getColumnMap());

        Table nmc_app_nl_loc_mix_v1 = new Table();
        nmc_app_nl_loc_mix_v1.setName("nmc_app_nl_loc_mix_v1");
        nmc_app_nl_loc_mix_v1.addColumn("imsi", "string");
        nmc_app_nl_loc_mix_v1.addColumn("imei", "string");
        nmc_app_nl_loc_mix_v1.addColumn("msisdn", "string");
        nmc_app_nl_loc_mix_v1.addColumn("eventid", "int");
        nmc_app_nl_loc_mix_v1.addColumn("btime", "string");
        nmc_app_nl_loc_mix_v1.addColumn("lac", "int");
        nmc_app_nl_loc_mix_v1.addColumn("ci", "int");
        nmc_app_nl_loc_mix_v1.addColumn("msc", "int");
        System.out.println(nmc_app_nl_loc_mix_v1.toAvro());
    }
}