/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ColumnInfo;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CsvBulkImportUtilTest {

    @Test
    public void testInitCsvImportJob() {
        Configuration conf = new Configuration();

        String tableName = "SCHEMANAME.TABLENAME";
        char delimiter = '!';
        char quote = '"';
        char escape = '\\';

        List<ColumnInfo> columnInfoList = ImmutableList.of(
                new ColumnInfo("MYCOL", PInteger.INSTANCE.getSqlType()));

        CsvBulkImportUtil.initCsvImportJob(
                conf, tableName, delimiter, quote, escape, null, columnInfoList, true);

        assertEquals(tableName, conf.get(CsvToKeyValueMapper.TABLE_NAME_CONFKEY));
        assertEquals("!", conf.get(CsvToKeyValueMapper.FIELD_DELIMITER_CONFKEY));
        assertNull(conf.get(CsvToKeyValueMapper.ARRAY_DELIMITER_CONFKEY));
        assertEquals(columnInfoList, CsvToKeyValueMapper.buildColumnInfoList(conf));
        assertEquals(true, conf.getBoolean(CsvToKeyValueMapper.IGNORE_INVALID_ROW_CONFKEY, false));
    }

    @Test
    public void testConfigurePreUpsertProcessor() {
        Configuration conf = new Configuration();
        CsvBulkImportUtil.configurePreUpsertProcessor(conf, MockProcessor.class);
        ImportPreUpsertKeyValueProcessor processor = CsvToKeyValueMapper.loadPreUpsertProcessor(conf);
        assertEquals(MockProcessor.class, processor.getClass());
    }


    public static class MockProcessor implements ImportPreUpsertKeyValueProcessor {

        @Override
        public List<KeyValue> preUpsert(byte[] rowKey, List<KeyValue> keyValues) {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
