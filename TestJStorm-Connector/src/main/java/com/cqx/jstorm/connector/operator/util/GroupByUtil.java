package com.cqx.jstorm.connector.operator.util;

import com.cqx.jstorm.connector.operator.bean.OperatorBean;
import com.cqx.jstorm.sql.bean.Column;
import com.cqx.jstorm.sql.bean.Table;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GroupByUtil
 *
 * @author chenqixu
 */
public class GroupByUtil {
    private static final String SEPARATOR = "-#GROUPBY#-";
    private List<String> groupByFields;
    private List<OperatorBean> operatorBeans;
    private Table table;
    private Map<String, Map<String, Column>> groupMap;
    private Map<String, Column> columnMap;
    private Map<String, IOperator> iOperatorMap;

    public GroupByUtil(List<String> groupByFields, List<OperatorBean> operatorBeans) {
        this.groupByFields = groupByFields;
        this.operatorBeans = operatorBeans;
    }

    public void addValue(String field_name, Object value) {

    }

    private void calculation() {
        for (OperatorBean operatorBean : operatorBeans) {
            columnMap.get(operatorBean.getField_name());
//            iOperatorMap.get(operatorBean.getRule()).exec();
        }
    }

    public void build() {
        Map<String, Column> columnMap = new LinkedHashMap<>();
        groupMap.put(getGroupByValue(), columnMap);
    }

    public String getGroupByValue() {
        StringBuilder sb = new StringBuilder();
        for (String groupByField : groupByFields) {
            sb.append(table.getColumnByName(groupByField).getValue())
                    .append(SEPARATOR);
        }
        return sb.toString();
    }
}
