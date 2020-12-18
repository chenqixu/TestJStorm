package com.cqx.jstorm.sql.ddl;

import com.cqx.jstorm.sql.bean.Table;
import com.cqx.jstorm.sql.util.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * create table table_name (<br>
 * fields1 type1,<br>
 * fields2 type2,<br>
 * ...)<br>
 * with<br>
 * (<br>
 * 'connector'='xxx',<br>
 * 'connector.type'='xxx'<br>
 * );<br>
 * 大小写不敏感<br>
 *
 * @author chenqixu
 */
public class CreateTable implements IDDL {
    private static final Logger logger = LoggerFactory.getLogger(CreateTable.class);

    static {
        ParserUtil.registerParser(CreateTable.class);
    }

    private boolean isThis = false;
    private String sql;
    private String table_name;
    private Map<String, String> fields_map = new LinkedHashMap<>();
    private Map<String, String> with_map = new HashMap<>();
    private Table table = new Table();

    /**
     * 第一遍语法检查，初略判断是否是create table
     *
     * @return
     */
    public boolean check() {
        //先判断是否create开头
        if (this.sql.startsWith("create ")) {
            parser();
        }
        return isThis;
    }

    public void parser() {
        try {
            String tmp = this.sql;
            //去掉回车换行(\r\n)、换行(\n)
            tmp = tmp.replaceAll("\r\n", "");
            tmp = tmp.replaceAll("\n", "");
            //----------------------------
            //表名解析
            //----------------------------
            //找到第一个左括号
            int table_name_v1 = tmp.indexOf("(");
            String tmp_table_name = tmp.substring(0, table_name_v1).trim();
            //去掉create table
            tmp_table_name = tmp_table_name.replace("create ", "");
            tmp_table_name = tmp_table_name.replace("table ", "");
            table_name = tmp_table_name.trim();
            table.setName(table_name);
            logger.info("table_name：{}", table_name);
            //----------------------------
            //字段解析
            //----------------------------
            //找到第一个右括号
            int fields_v1 = tmp.indexOf(")");
            String tmp_fields = tmp.substring(table_name_v1 + 1, fields_v1).trim();
            logger.info("tmp_fields：{}", tmp_fields);
            //按逗号分割
            String[] fields_arr = tmp_fields.split(",", -1);
            for (String tmp_field : fields_arr) {
                String[] tmp_field_arr = tmp_field.split(" ", -1);
                logger.info("field：{}，type：{}", tmp_field_arr[0], tmp_field_arr[1]);
                fields_map.put(tmp_field_arr[0], tmp_field_arr[1]);
                table.addColumn(tmp_field_arr[0], tmp_field_arr[1]);
            }
            //----------------------------
            //解析with内的参数
            //----------------------------
            //找到第二个左括号
            int with_v1 = tmp.indexOf("(", table_name_v1 + 1);
            logger.info("table_name_v1：{}，with_v1：{}", table_name_v1, with_v1);
            //找到第二个右括号
            int with_v2 = tmp.indexOf(")", fields_v1 + 1);
            logger.info("fields_v1：{}，with_v2：{}", fields_v1, with_v2);
            if (with_v1 > 0 && with_v2 > 0) {
                String tmp_with = tmp.substring(with_v1 + 1, with_v2).trim();
                logger.info("tmp_with：{}", tmp_with);
                String[] tmp_with_properties = tmp_with.split(",", -1);
                for (String tmp_with_p : tmp_with_properties) {
                    String[] tmp_with_p_s = tmp_with_p.split("=", -1);
                    if (tmp_with_p_s.length == 2) {
                        String key = tmp_with_p_s[0].replaceAll("'", "").trim();
                        String value = tmp_with_p_s[1].replaceAll("'", "").trim();
                        logger.info("tmp_with_p：{}，{}，{}", tmp_with_p, key, value);
                        with_map.put(key, value);
                    } else if (tmp_with_p_s.length > 2) {
                        String key = tmp_with_p_s[0].replaceAll("'", "").trim();
                        //找到第一个等号
                        int tmp_with_p_v1 = tmp_with_p.indexOf("=");
                        String value = tmp_with_p.substring(tmp_with_p_v1 + 1).replaceAll("'", "").trim();
                        logger.info("tmp_with_p：{}，{}，{}", tmp_with_p, key, value);
                        with_map.put(key, value);
                    } else {
                        logger.warn("{} 无法使用=切割，请确认。", tmp_with_p);
                    }
                }
            }
            //解析完成
            isThis = true;
        } catch (Exception e) {
            logger.error("解析异常，内容：" + e.getMessage(), e);
        }
    }

    public void setSql(String sql) {
        this.sql = sql.toLowerCase().trim();//大小写不敏感
    }

    @Override
    public String getTableName() {
        return table_name;
    }

    @Override
    public Map<String, String> getWithMap() {
        return with_map;
    }

    @Override
    public Map<String, String> getFields_map() {
        return fields_map;
    }

    @Override
    public Table getTable() {
        return table;
    }
}
