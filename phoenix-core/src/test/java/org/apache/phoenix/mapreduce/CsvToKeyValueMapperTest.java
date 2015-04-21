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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.util.ColumnInfo;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class CsvToKeyValueMapperTest {

    @Test
    public void testCsvLineParser() throws IOException {
        CsvToKeyValueMapper.CsvLineParser lineParser =
                new CsvToKeyValueMapper.CsvLineParser(';', '"', '\\');
        CSVRecord parsed = lineParser.parse("one;two");

        assertEquals("one", parsed.get(0));
        assertEquals("two", parsed.get(1));
        assertTrue(parsed.isConsistent());
        assertEquals(1, parsed.getRecordNumber());
    }

    @Test
    public void testCsvLineParserWithQuoting() throws IOException {
        CsvToKeyValueMapper.CsvLineParser lineParser =
                new CsvToKeyValueMapper.CsvLineParser(';', '"', '\\');
        CSVRecord parsed = lineParser.parse("\"\\\"one\";\"\\;two\\\\\"");

        assertEquals("\"one", parsed.get(0));
        assertEquals(";two\\", parsed.get(1));
        assertTrue(parsed.isConsistent());
        assertEquals(1, parsed.getRecordNumber());
    }


    @Test
    public void testBuildColumnInfoList() {
        List<ColumnInfo> columnInfoList = ImmutableList.of(
                new ColumnInfo("idCol", PInteger.INSTANCE.getSqlType()),
                new ColumnInfo("unsignedIntCol", PUnsignedInt.INSTANCE.getSqlType()),
                new ColumnInfo("stringArrayCol", PIntegerArray.INSTANCE.getSqlType()));

        Configuration conf = new Configuration();
        CsvToKeyValueMapper.configureColumnInfoList(conf, columnInfoList);
        List<ColumnInfo> fromConfig = CsvToKeyValueMapper.buildColumnInfoList(conf);

        assertEquals(columnInfoList, fromConfig);
    }

    @Test
    public void testBuildColumnInfoList_ContainingNulls() {
        // A null value in the column info list means "skip that column in the input"
        List<ColumnInfo> columnInfoListWithNull = Lists.newArrayList(
                new ColumnInfo("idCol", PInteger.INSTANCE.getSqlType()),
                null,
                new ColumnInfo("unsignedIntCol", PUnsignedInt.INSTANCE.getSqlType()),
                new ColumnInfo("stringArrayCol", PIntegerArray.INSTANCE.getSqlType()));

        Configuration conf = new Configuration();
        CsvToKeyValueMapper.configureColumnInfoList(conf, columnInfoListWithNull);
        List<ColumnInfo> fromConfig = CsvToKeyValueMapper.buildColumnInfoList(conf);

        assertEquals(columnInfoListWithNull, fromConfig);
    }

    @Test
    public void testGetJdbcUrl() {
        Configuration conf = new Configuration();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "myzkclient:2181");
        String jdbcUrl = CsvToKeyValueMapper.getJdbcUrl(conf);

        assertEquals("jdbc:phoenix:myzkclient:2181", jdbcUrl);
    }

    @Test(expected=IllegalStateException.class)
    public void testGetJdbcUrl_NotConfigured() {
        Configuration conf = new Configuration();
        CsvToKeyValueMapper.getJdbcUrl(conf);
    }

    @Test
    public void testLoadPreUpdateProcessor() {
        Configuration conf = new Configuration();
        conf.setClass(PhoenixConfigurationUtil.UPSERT_HOOK_CLASS_CONFKEY, MockUpsertProcessor.class,
                ImportPreUpsertKeyValueProcessor.class);

        ImportPreUpsertKeyValueProcessor processor = PhoenixConfigurationUtil.loadPreUpsertProcessor(conf);
        assertEquals(MockUpsertProcessor.class, processor.getClass());
    }

    @Test
    public void testLoadPreUpdateProcessor_NotConfigured() {

        Configuration conf = new Configuration();
        ImportPreUpsertKeyValueProcessor processor = PhoenixConfigurationUtil.loadPreUpsertProcessor(conf);

        assertEquals(CsvToKeyValueMapper.DefaultImportPreUpsertKeyValueProcessor.class,
                processor.getClass());
    }

    @Test(expected=IllegalStateException.class)
    public void testLoadPreUpdateProcessor_ClassNotFound() {
        Configuration conf = new Configuration();
        conf.set(PhoenixConfigurationUtil.UPSERT_HOOK_CLASS_CONFKEY, "MyUndefinedClass");

        PhoenixConfigurationUtil.loadPreUpsertProcessor(conf);
    }


    static class MockUpsertProcessor implements ImportPreUpsertKeyValueProcessor {

        @Override
        public List<KeyValue> preUpsert(byte[] rowKey, List<KeyValue> keyValues) {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
