package com.cqx.jstorm.comm.util;

import java.util.HashMap;
import java.util.Map;

/**
 * ArgsParser
 *
 * @author chenqixu
 */
public class ArgsParser {

    private Map<String, Args> argsMap;

    private ArgsParser() {
        argsMap = new HashMap<>();
    }

    public static ArgsParser builder() {
        return new ArgsParser();
    }

    public void perser(String[] args) {
        if (args == null || args.length == 0) throw new NullPointerException("args is null.");
        // 每次消灭两个参数
        for (int i = 0; i < args.length; i = i + 2) {
            String full_name = args[i];
            String value = null;
            if (i + 1 < args.length)
                value = args[i + 1];
            if (value == null || value.length() == 0) throw new NullPointerException(full_name + " is Unrecognized.");
            mapSetValue(full_name, value);
        }
    }

    public void addParam(String full_name) {
        argsMap.put(full_name, new Args(full_name));
    }

    public String getParamValue(String full_name) {
        return argsMap.get(full_name).getValue();
    }

    private void mapSetValue(String full_name, String value) {
        argsMap.get(full_name).setValue(value);
    }

    class Args {
        String full_name;
        String simple_name;
        String value;

        Args(String full_name) {
            this.full_name = full_name;
        }

        public String getFull_name() {
            return full_name;
        }

        public void setFull_name(String full_name) {
            this.full_name = full_name;
        }

        public String getSimple_name() {
            return simple_name;
        }

        public void setSimple_name(String simple_name) {
            this.simple_name = simple_name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
