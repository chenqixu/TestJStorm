package com.cqx.jstorm.connector.operator.util;

import com.cqx.jstorm.sql.bean.Column;

/**
 * IOperator
 *
 * @author chenqixu
 */
public interface IOperator<T> {
    void exec(Column column);
    T get();
}
