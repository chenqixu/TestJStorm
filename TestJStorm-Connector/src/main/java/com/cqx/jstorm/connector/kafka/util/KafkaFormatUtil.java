package com.cqx.jstorm.connector.kafka.util;

import com.cqx.jstorm.comm.bean.SendBean;
import com.cqx.jstorm.sql.bean.Column;
import com.cqx.jstorm.sql.bean.Table;
import com.cqx.jstorm.comm.util.kafka.RecordConvertor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

/**
 * kafka格式化工具
 *
 * @author chenqixu
 */
public class KafkaFormatUtil {
    private RecordConvertor recordConvertor;
    private Table table;

    public void init(String format, String format_content) {
        switch (format) {
            case "avro":
                avro(format_content);
                break;
            default:
                break;
        }
    }

    private void avro(String format_content) {
        //从字段拼接出schema
        Schema schema = new Schema.Parser().parse(format_content);
        //记录转换工具类
        recordConvertor = new RecordConvertor(schema);
        //解析Table
        table = Table.parser(format_content);
    }

    public List<Column> read(byte[] value) {
        List<Column> columnList = table.getColumnList();
        GenericRecord genericRecord = recordConvertor.binaryToRecord(value);
        for (Column column : columnList) {
            column.setValue(genericRecord.get(column.getName()));
        }
        return columnList;
    }

    public Column[] read(byte[] value, SendBean sendBean) {
        Map<String, Column> columnObjMap = table.getColumnMap();
        Column[] outputList = new Column[sendBean.getOutput_fields().size()];
        GenericRecord genericRecord = recordConvertor.binaryToRecord(value);
        for (int i = 0; i < sendBean.getOutput_fields().size(); i++) {
            String columnName = sendBean.getOutput_fields().get(i);
            Column column = new Column(columnObjMap.get(columnName));
            column.setValue(genericRecord.get(columnName));
            outputList[i] = column;
        }
        return outputList;
    }
}
