package com.cqx.jstorm.sql.ddl;

import com.cqx.jstorm.sql.bean.Table;

import java.util.Map;

/**
 * IDDL
 *
 * @author chenqixu
 */
public interface IDDL {
    boolean check();

    void parser();

    void setSql(String sql);

    String getTableName();

    Map<String, String> getWithMap();

    Map<String, String> getFields_map();

    Table getTable();
}
