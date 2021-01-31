package com.cqx.jstorm.connector.operator.util;


import com.cqx.jstorm.sql.bean.Column;

/**
 * count
 *
 * @author chenqixu
 */
public class CountOperator implements IOperator {
    private Long val = 0L;

    @Override
    public void exec(Column column) {
        Long count = (Long) column.getValue();
        val += count;
    }

    @Override
    public Object get() {
        return val;
    }
}
