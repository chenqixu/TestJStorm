package com.cqx.jstorm.sql.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table
 *
 * @author chenqixu
 */
public class Table {
    private String name;
    @JSONField(serialize = false, deserialize = false)
    private Map<String, Column> columnMap = new LinkedHashMap<>();

    public static Table parser(String avro) {
        //从字段拼接出schema
        Schema schema = new Schema.Parser().parse(avro);
        Table table = new Table();
        table.setName(schema.getName());
        for (Schema.Field field : schema.getFields()) {
            // 字段名称
            String field_name = field.name();
            /**
             * 字段类型
             * RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES,
             * INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL;
             */
            Schema.Type field_type = field.schema().getType();
            // 仅处理有字段名称的数据
            if (field_name != null && field_name.length() > 0) {
                // 判断字段类型
                switch (field_type) {
                    // 组合类型需要映射出真正的类型
                    case UNION:
                        // 获取组合类型中的所有类型
                        List<Schema> types = field.schema().getTypes();
                        // 循环判断
                        for (Schema schema1 : types) {
                            Schema.Type type1 = schema1.getType();
                            if (type1.equals(Schema.Type.INT) ||
                                    type1.equals(Schema.Type.STRING) ||
                                    type1.equals(Schema.Type.LONG) ||
                                    type1.equals(Schema.Type.FLOAT) ||
                                    type1.equals(Schema.Type.DOUBLE) ||
                                    type1.equals(Schema.Type.BOOLEAN)
                            ) {
                                Column column = new Column(field_name, type1.getName());
                                table.addColumn(column);
                                break;
                            }
                        }
                        break;
                    default:
                        Column column = new Column(field_name, field_type.getName());
                        table.addColumn(column);
                        break;
                }
            }
        }
        return table;
    }

    public static Table JSONToBean(String json) {
        return JSON.parseObject(json, Table.class);
    }

    public String toAvro() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"namespace\":\"com.cqx.jstorm.sql.bean.avro\",");
        sb.append("\"type\":\"record\",");
        sb.append("\"name\":\"").append(getName()).append("\",");
        sb.append("\"fields\":[");
        List<Column> columnList = getColumnList();
        for (int i = 0; i < columnList.size(); i++) {
            Column column = columnList.get(i);
            String type = column.getType();
            if (type.equals("bigint")) type = "long";
            sb.append("{\"name\": \"")
                    .append(column.getName())
                    .append("\",  \"type\": [\"")
                    .append(type)
                    .append("\", \"null\"]}");
            if (i == 0 || i < columnList.size() - 1) sb.append(",");
        }
        sb.append("]");
        sb.append("}");
        return sb.toString();
    }

    public void addColumn(Column column) {
        addColumn(column.getName(), column.getType());
    }

    public void addColumn(String name, String type) {
        Column column = new Column(name, type);
        columnMap.put(name, column);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Column> getColumnList() {
        List<Column> columnList = new ArrayList<>();
        for (Map.Entry<String, Column> entry : columnMap.entrySet()) {
            columnList.add(entry.getValue());
        }
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        for (Column column : columnList) {
            addColumn(column);
        }
    }

    public Map<String, Column> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(Map<String, Column> columnMap) {
        this.columnMap = columnMap;
    }

    public String toJSON() {
        return JSON.toJSONString(this);
    }
}
