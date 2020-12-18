package com.cqx.jstorm.sql.bean;

/**
 * Column {name , type}
 * <br>
 * <pre>
 *  type：
 *      null：no value
 *      boolean：a binary value
 *      int：32-bit signed integer
 *      long：64-bit signed integer
 *      float：single precision (32-bit) IEEE 754 floating-point number
 *      double：double precision (64-bit) IEEE 754 floating-point number
 *      bytes：sequence of 8-bit unsigned bytes
 *      string：unicode character sequence
 * </pre>
 *
 * @author chenqixu
 */
public class Column {
    private String name;
    private String type;
    private Object value;

    public Column() {
    }

    public Column(Column column) {
        this.name = column.getName();
        this.type = column.getType();
    }

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public Column(String name, String type, Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String toString() {
        return "name：" + name + "，type：" + type + "，value：" + value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
