/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.apache.phoenix.query;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GuidePostsCacheWrapperTest {

    @Mock
    GuidePostsCache cache;

    GuidePostsCacheWrapper wrapper;

    byte[] table = org.apache.hadoop.hbase.util.Bytes.toBytes("tableName");
    byte[] columnFamily1 = Bytes.toBytesBinary("cf1");
    byte[] columnFamily2 = Bytes.toBytesBinary("cf2");

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        wrapper = new GuidePostsCacheWrapper(cache);
    }

    @Test
    public void invalidateAllTableDescriptor() {
        Set<byte[]> cfSet = new HashSet<>();
        cfSet.add(columnFamily1);
        cfSet.add(columnFamily2);



        TableDescriptor tableDesc = Mockito.mock(TableDescriptor.class);
        TableName tableName = TableName.valueOf(table);

        Mockito.when(tableDesc.getColumnFamilyNames()).thenReturn(cfSet);
        Mockito.when(tableDesc.getTableName()).thenReturn(tableName);

        wrapper.invalidateAll(tableDesc);
        Mockito.verify(cache,Mockito.times(1)).invalidate(new GuidePostsKey(table,columnFamily1));
        Mockito.verify(cache,Mockito.times(1)).invalidate(new GuidePostsKey(table,columnFamily2));
    }

    @Test
    public void invalidateAllPTable(){
        PTable ptable = Mockito.mock(PTable.class);
        PName pname = Mockito.mock(PName.class);
        PName pnamecf1 = Mockito.mock(PName.class);
        PName pnamecf2 = Mockito.mock(PName.class);

        Mockito.when(ptable.getPhysicalName()).thenReturn(pname);
        Mockito.when(pname.getBytes()).thenReturn(table);

        PColumnFamily cf1 = Mockito.mock(PColumnFamily.class);
        PColumnFamily cf2 = Mockito.mock(PColumnFamily.class);
        Mockito.when(cf1.getName()).thenReturn(pnamecf1);
        Mockito.when(cf2.getName()).thenReturn(pnamecf2);
        Mockito.when(pnamecf1.getBytes()).thenReturn(columnFamily1);
        Mockito.when(pnamecf2.getBytes()).thenReturn(columnFamily2);

        List<PColumnFamily> cfList = Lists.newArrayList(cf1,cf2);
        Mockito.when(ptable.getColumnFamilies()).thenReturn(cfList);

        wrapper.invalidateAll(ptable);

        Mockito.verify(cache,Mockito.times(1)).invalidate(new GuidePostsKey(table,columnFamily1));
        Mockito.verify(cache,Mockito.times(1)).invalidate(new GuidePostsKey(table,columnFamily2));
    }

    @Test(expected = NullPointerException.class)
    public void invalidateAllTableDescriptorNull() {
        TableDescriptor tableDesc = null;
        wrapper.invalidateAll(tableDesc);
    }

    @Test(expected = NullPointerException.class)
    public void invalidateAllPTableNull(){
        PTable ptable = null;
        wrapper.invalidateAll(ptable);
    }

}
