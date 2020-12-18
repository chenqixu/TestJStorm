package com.cqx.jstorm.sql.util;

import com.cqx.jstorm.sql.ddl.CreateTable;
import com.cqx.jstorm.sql.ddl.IDDL;
import com.cqx.jstorm.sql.dml.ExecTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * sql解析
 *
 * @author chenqixu
 */
public class ParserUtil {
    private static final Logger logger = LoggerFactory.getLogger(ParserUtil.class);
    private static List<Class<? extends IDDL>> iddlList = new ArrayList<>();

    static {
        try {
            Class.forName(CreateTable.class.getName());
        } catch (ClassNotFoundException e) {
            logger.error("解析类加载失败，" + e.getMessage(), e);
        }
    }

    private Map<Object, Object> yaml;
    private Map<String, IDDL> iddlMap = new HashMap<>();
    private ExecTable execTable = new ExecTable();

    public static void registerParser(Class<? extends IDDL> iddl) {
        iddlList.add(iddl);
    }

    public void exec(String sql) throws Exception {
        boolean is_ddl = false;
        for (Class<? extends IDDL> cls : iddlList) {
            IDDL iddl = cls.newInstance();
            iddl.setSql(sql);
            if (iddl.check()) {
                iddlMap.put(iddl.getTableName(), iddl);
                is_ddl = true;
                break;
            }
        }
        if (!is_ddl) {
            execTable.setSql(sql);
            if (execTable.check()) {
//                execTable.submit(iddlMap);
                yaml = execTable.buildYaml(iddlMap);
            }
        }
    }

    public Map<Object, Object> getYaml() {
        return yaml;
    }
}
