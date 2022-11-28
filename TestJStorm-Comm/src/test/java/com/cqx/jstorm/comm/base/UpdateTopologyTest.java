package com.cqx.jstorm.comm.base;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class UpdateTopologyTest {

    @Test
    public void main() {
        UpdateTopology.main(null);
    }

    @Test
    public void hashTest() {
        Object a1 = "13500009986";
        Object a2 = "13500009987";
        Object a3 = "13500009985";
        Object a4 = "13500009990";
        getMod(a1, 3);
        getMod(a2, 3);
        getMod(a3, 3);
        getMod(a4, 3);
    }

    private int getMod(Object value, int ComponentTaskSize) {
        List<Object> ret = new ArrayList<Object>(1);
        ret.add(value);
        int hashcode = ret.hashCode();
        int hs = value.hashCode();
        int group = Math.abs(hashcode % ComponentTaskSize);
        int hs_group = Math.abs(hs % ComponentTaskSize);
        System.out.println(String.format("value: %s, hashcode: %s, group: %s, value.hs: %s, hs_group: %s"
                , value, hashcode, group, hs, hs_group));
        return group;
    }
}