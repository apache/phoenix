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
package org.apache.phoenix.index;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.mapreduce.index.IndexScrutinyMapper;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class IndexScrutinyMapperTest extends BaseConnectionlessQueryTest {
    String schema, tableName, indexName;
    boolean isNamespaceEnabled;
    PTable inputTable;

    @Before
    public void setup() {
        schema = "S_" + generateUniqueName();
        tableName = "T_" + generateUniqueName();
        indexName = "I_" + generateUniqueName();
        inputTable = Mockito.mock(PTable.class);

    }

    @Parameterized.Parameters(name ="IndexUpgradeToolTest_isNamespaceEnabled={0}")
    public static synchronized Collection<Boolean> data() {
        return Arrays.asList( false, true);
    }

    public IndexScrutinyMapperTest(boolean isNamespaceEnabled) {
        this.isNamespaceEnabled = isNamespaceEnabled;
    }
    @Test
    public void testGetSourceTableName_table() {
        String fullTableName = SchemaUtil.getQualifiedTableName(schema, tableName);
        PName sourcePhysicalName = SchemaUtil.getPhysicalHBaseTableName(schema, tableName,
                isNamespaceEnabled);
        String expectedName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(fullTableName),
                isNamespaceEnabled).toString();
        //setup
        Mockito.when(inputTable.getType()).thenReturn(PTableType.TABLE);
        Mockito.when(inputTable.getPhysicalName()).thenReturn(sourcePhysicalName);
        Mockito.when(inputTable.getTableName()).thenReturn(PNameFactory.newName(tableName));
        Mockito.when(inputTable.getSchemaName()).thenReturn(PNameFactory.newName(schema));
        //test
        String output = IndexScrutinyMapper.getSourceTableName(inputTable, isNamespaceEnabled);
        //assert
        Assert.assertEquals(expectedName, output);
    }

    @Test
    public void testGetSourceTableName_view() {
        String fullTableName = SchemaUtil.getQualifiedTableName(schema, tableName);
        PName sourcePhysicalName = SchemaUtil.getPhysicalHBaseTableName(schema, tableName,
                isNamespaceEnabled);
        String expectedName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(fullTableName),
                isNamespaceEnabled).toString();
        //setup
        Mockito.when(inputTable.getType()).thenReturn(PTableType.VIEW);
        Mockito.when(inputTable.getPhysicalName()).thenReturn(sourcePhysicalName);
        //test
        String output = IndexScrutinyMapper.getSourceTableName(inputTable, isNamespaceEnabled);
        //assert
        Assert.assertEquals(expectedName, output);
    }

    @Test
    public void testGetSourceTableName_index() {
        String fullTableName = SchemaUtil.getQualifiedTableName(schema, indexName);
        PName sourcePhysicalName = SchemaUtil.getPhysicalHBaseTableName(schema, indexName,
                isNamespaceEnabled);
        String expectedName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(fullTableName),
                isNamespaceEnabled).toString();

        //setup
        Mockito.when(inputTable.getType()).thenReturn(PTableType.INDEX);
        Mockito.when(inputTable.getPhysicalName()).thenReturn(sourcePhysicalName);
        Mockito.when(inputTable.getTableName()).thenReturn(PNameFactory.newName(indexName));
        Mockito.when(inputTable.getSchemaName()).thenReturn(PNameFactory.newName(schema));

        //test
        String output = IndexScrutinyMapper.getSourceTableName(inputTable, isNamespaceEnabled);
        //assert
        Assert.assertEquals(expectedName, output);
    }

    @Test
    public void testGetSourceTableName_viewIndex() {
        PName physicalTableName = SchemaUtil.getPhysicalHBaseTableName(schema, tableName,
                isNamespaceEnabled);
        String expectedName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName.getString());
        PName physicalIndexTableName = PNameFactory
                .newName(MetaDataUtil.getViewIndexPhysicalName(physicalTableName.getString()));

        PTable pSourceTable = Mockito.mock(PTable.class);
        //setup
        Mockito.when(pSourceTable.getPhysicalName()).thenReturn(physicalIndexTableName);
        //test
        String output = IndexScrutinyMapper.getSourceTableName(pSourceTable, isNamespaceEnabled);
        //assert
        Assert.assertEquals(expectedName, output);
    }
}
