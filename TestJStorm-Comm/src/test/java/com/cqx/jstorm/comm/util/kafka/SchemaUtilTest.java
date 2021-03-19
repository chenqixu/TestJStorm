package com.cqx.jstorm.comm.util.kafka;

import org.junit.Before;
import org.junit.Test;

public class SchemaUtilTest {
    private SchemaUtil schemaUtil;

    @Before
    public void setUp() throws Exception {
        String urlStr = "http://127.0.0.1:19090/nl-edc-cct-sys-ms-dev/SchemaService/";
        boolean isUseHttpUtil = true;
        schemaUtil = new SchemaUtil(urlStr, isUseHttpUtil);
    }

    @Test
    public void readUrlContent() {
        System.out.println(schemaUtil.readUrlContentByHttpUtil("ogg"));
    }

    @Test
    public void updateSchema() {
        schemaUtil.updateSchemaByHttpUtil("ogg", "{\"type\":\"record\",\"name\":\"ORA_TO_KAFKA\",\"namespace\":\"TEST_OGG\",\"fields\":[{\"name\":\"table\",\"type\":\"string\"},{\"name\":\"op_type\",\"type\":\"string\"},{\"name\":\"op_ts\",\"type\":\"string\"},{\"name\":\"current_ts\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"string\"},{\"name\":\"primary_keys\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"tokens\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"default\":{}},{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"columns\",\"fields\":[{\"name\":\"ID\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"ID_isMissing\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"NAME\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"NAME_isMissing\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"ADDR\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADDR_isMissing\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"MSISDN\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"MSISDN_isMissing\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"columns\"],\"default\":null}]}");
    }
}