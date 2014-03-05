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
package org.apache.phoenix.hbase.index.write;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.phoenix.hbase.index.table.CachingHTableFactory;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

public class TestCachingHTableFactory {

  @Test
  public void testCacheCorrectlyExpiresTable() throws Exception {
    // setup the mocks for the tables we will request
    HTableFactory delegate = Mockito.mock(HTableFactory.class);
    ImmutableBytesPtr t1 = new ImmutableBytesPtr(Bytes.toBytes("t1"));
    ImmutableBytesPtr t2 = new ImmutableBytesPtr(Bytes.toBytes("t2"));
    ImmutableBytesPtr t3 = new ImmutableBytesPtr(Bytes.toBytes("t3"));
    HTableInterface table1 = Mockito.mock(HTableInterface.class);
    HTableInterface table2 = Mockito.mock(HTableInterface.class);
    HTableInterface table3 = Mockito.mock(HTableInterface.class);
    Mockito.when(delegate.getTable(t1)).thenReturn(table1);
    Mockito.when(delegate.getTable(t2)).thenReturn(table2);
    Mockito.when(delegate.getTable(t3)).thenReturn(table3);
    
    // setup our factory with a cache size of 2
    CachingHTableFactory factory = new CachingHTableFactory(delegate, 2);
    factory.getTable(t1);
    factory.getTable(t2);
    factory.getTable(t3);
    // get the same table a second time, after it has gone out of cache
    factory.getTable(t1);
    
    Mockito.verify(delegate, Mockito.times(2)).getTable(t1);
    Mockito.verify(delegate, Mockito.times(1)).getTable(t2);
    Mockito.verify(delegate, Mockito.times(1)).getTable(t3);
    Mockito.verify(table1).close();
  }
}