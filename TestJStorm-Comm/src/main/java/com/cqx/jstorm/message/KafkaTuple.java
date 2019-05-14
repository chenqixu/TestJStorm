package com.cqx.jstorm.message;

import java.util.ArrayList;
import java.util.List;

/**
 * KafkaTuple
 *
 * @author chenqixu
 */
public class KafkaTuple<T> {
    private List<T> values = new ArrayList<>();

    public void add(T e) {
        values.add(e);
    }

    public List<T> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
