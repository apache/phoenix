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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.util.ColumnInfo;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;

public class FormatToBytesWritableMapperTest {

    @Test
    public void testBuildColumnInfoList() {
        List<ColumnInfo> columnInfoList = ImmutableList.of(
                new ColumnInfo("idCol", PInteger.INSTANCE.getSqlType()),
                new ColumnInfo("unsignedIntCol", PUnsignedInt.INSTANCE.getSqlType()),
                new ColumnInfo("stringArrayCol", PIntegerArray.INSTANCE.getSqlType()));

        Configuration conf = new Configuration();
        FormatToBytesWritableMapper.configureColumnInfoList(conf, columnInfoList);
        List<ColumnInfo> fromConfig = FormatToBytesWritableMapper.buildColumnInfoList(conf);

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
        FormatToBytesWritableMapper.configureColumnInfoList(conf, columnInfoListWithNull);
        List<ColumnInfo> fromConfig = FormatToBytesWritableMapper.buildColumnInfoList(conf);

        assertEquals(columnInfoListWithNull, fromConfig);
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

        assertEquals(FormatToBytesWritableMapper.DefaultImportPreUpsertKeyValueProcessor.class,
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
