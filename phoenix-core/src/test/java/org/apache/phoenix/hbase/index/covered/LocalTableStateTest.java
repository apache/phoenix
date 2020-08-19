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
package org.apache.phoenix.hbase.index.covered;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver.ReplayWrite;
import org.apache.phoenix.hbase.index.covered.data.CachedLocalTable;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.scanner.Scanner;
import org.apache.phoenix.hbase.index.scanner.ScannerBuilder.CoveredDeleteScanner;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;
import org.mockito.Mockito;


public class LocalTableStateTest {

  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] fam = Bytes.toBytes("fam");
  private static final byte[] qual = Bytes.toBytes("qual");
  private static final byte[] val = Bytes.toBytes("val");
  private static final long ts = 10;
  private static final IndexMetaData indexMetaData = new IndexMetaData() {

    @Override
    public ReplayWrite getReplayWrite() {
        return null;
    }

    @Override
    public boolean requiresPriorRowState(Mutation m) {
        return true;
    }
      
    @Override
    public int getClientVersion() {
        return ScanUtil.UNKNOWN_CLIENT_VERSION;
    }

  };

  @SuppressWarnings("unchecked")
  @Test
  public void testCorrectOrderingWithLazyLoadingColumns() throws Exception {
    Put m = new Put(row);
    m.addColumn(fam, qual, ts, val);
    // setup mocks
    Configuration conf = new Configuration(false);
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    Mockito.when(env.getConfiguration()).thenReturn(conf);

    Region region = Mockito.mock(Region.class);
    Mockito.when(env.getRegion()).thenReturn(region);
    final byte[] stored = Bytes.toBytes("stored-value");


    KeyValue kv = new KeyValue(row, fam, qual, ts, Type.Put, stored);
    kv.setSequenceId(0);
    HashMap<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells =
            new  HashMap<ImmutableBytesPtr, List<Cell>>();
    rowKeyPtrToCells.put(new ImmutableBytesPtr(row), Collections.singletonList((Cell)kv));
    CachedLocalTable cachedLocalTable = CachedLocalTable.build(rowKeyPtrToCells);
    LocalTableState table = new LocalTableState(cachedLocalTable, m);
    //add the kvs from the mutation
    table.addPendingUpdates(m.get(fam, qual));

    // setup the lookup
    ColumnReference col = new ColumnReference(fam, qual);
    table.setCurrentTimestamp(ts);
    //check that our value still shows up first on scan, even though this is a lazy load
    Pair<CoveredDeleteScanner, IndexUpdate> p = table.getIndexedColumnsTableState(Arrays.asList(col), false, false, indexMetaData);
    Scanner s = p.getFirst();
    assertEquals("Didn't get the pending mutation's value first", m.get(fam, qual).get(0), s.next());
  }

  public static final class ScannerCreatedException extends RuntimeException {
      ScannerCreatedException(String msg) {
          super(msg);
      }
  }

  @Test
  public void testNoScannerForImmutableRows() throws Exception {
      IndexMetaData indexMetaData = new IndexMetaData() {

          @Override
          public ReplayWrite getReplayWrite() {
              return null;
          }

        @Override
        public boolean requiresPriorRowState(Mutation m) {
            return false;
        }
            
        @Override
        public int getClientVersion() {
            return ScanUtil.UNKNOWN_CLIENT_VERSION;
        }

    };
    Put m = new Put(row);
    m.addColumn(fam, qual, ts, val);
    // setup mocks
    Configuration conf = new Configuration(false);
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    Mockito.when(env.getConfiguration()).thenReturn(conf);

    Region region = Mockito.mock(Region.class);
    Mockito.when(env.getRegion()).thenReturn(region);

    CachedLocalTable cachedLocalTable = CachedLocalTable.build(null);
    LocalTableState table = new LocalTableState(cachedLocalTable, m);
    //add the kvs from the mutation
    table.addPendingUpdates(m.get(fam, qual));

    // setup the lookup
    ColumnReference col = new ColumnReference(fam, qual);
    table.setCurrentTimestamp(ts);
    //check that our value still shows up first on scan, even though this is a lazy load
    Pair<CoveredDeleteScanner, IndexUpdate> p = table.getIndexedColumnsTableState(Arrays.asList(col), false, false, indexMetaData);
    Scanner s = p.getFirst();
    assertEquals("Didn't get the pending mutation's value first", m.get(fam, qual).get(0), s.next());
  }

  /**
   * Test that we correctly rollback the state of keyvalue
   * @throws Exception
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testCorrectRollback() throws Exception {
    Put m = new Put(row);
    m.addColumn(fam, qual, ts, val);
    // setup mocks
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);

    Region region = Mockito.mock(Region.class);
    Mockito.when(env.getRegion()).thenReturn(region);
    final byte[] stored = Bytes.toBytes("stored-value");
    final KeyValue storedKv = new KeyValue(row, fam, qual, ts, Type.Put, stored);
    storedKv.setSequenceId(2);

    HashMap<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells =
            new  HashMap<ImmutableBytesPtr, List<Cell>>();
    rowKeyPtrToCells.put(new ImmutableBytesPtr(row), Collections.singletonList((Cell)storedKv));
    CachedLocalTable cachedLocalTable = CachedLocalTable.build(rowKeyPtrToCells);
    LocalTableState table = new LocalTableState(cachedLocalTable, m);

    // add the kvs from the mutation
    KeyValue kv = KeyValueUtil.ensureKeyValue(m.get(fam, qual).get(0));
    kv.setSequenceId(0);
    table.addPendingUpdates(kv);

    // setup the lookup
    ColumnReference col = new ColumnReference(fam, qual);
    table.setCurrentTimestamp(ts);
    // check that the value is there
    Pair<CoveredDeleteScanner, IndexUpdate> p = table.getIndexedColumnsTableState(Arrays.asList(col), false, false, indexMetaData);
    Scanner s = p.getFirst();
    assertEquals("Didn't get the pending mutation's value first", kv, s.next());

    // rollback that value
    table.rollback(Arrays.asList(kv));
    p = table.getIndexedColumnsTableState(Arrays.asList(col), false, false, indexMetaData);
    s = p.getFirst();
    assertEquals("Didn't correctly rollback the row - still found it!", null, s.next());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnlyLoadsRequestedColumns() throws Exception {
    // setup mocks
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);

    Region region = Mockito.mock(Region.class);
    Mockito.when(env.getRegion()).thenReturn(region);
    final KeyValue storedKv =
        new KeyValue(row, fam, qual, ts, Type.Put, Bytes.toBytes("stored-value"));
    storedKv.setSequenceId(2);


    Put pendingUpdate = new Put(row);
    pendingUpdate.addColumn(fam, qual, ts, val);
    HashMap<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells =
            new  HashMap<ImmutableBytesPtr, List<Cell>>();
    rowKeyPtrToCells.put(new ImmutableBytesPtr(row), Collections.singletonList((Cell)storedKv));
    CachedLocalTable cachedLocalTable = CachedLocalTable.build(rowKeyPtrToCells);
    LocalTableState table = new LocalTableState(cachedLocalTable, pendingUpdate);


    // do the lookup for the given column
    ColumnReference col = new ColumnReference(fam, qual);
    table.setCurrentTimestamp(ts);
    // check that the value is there
    Pair<CoveredDeleteScanner, IndexUpdate> p = table.getIndexedColumnsTableState(Arrays.asList(col), false, false, indexMetaData);
    Scanner s = p.getFirst();
    // make sure it read the table the one time
    assertEquals("Didn't get the stored keyvalue!", storedKv, s.next());

    // on the second lookup it shouldn't access the underlying table again - the cached columns
    // should know they are done
    p = table.getIndexedColumnsTableState(Arrays.asList(col), false, false, indexMetaData);
    s = p.getFirst();
    assertEquals("Lost already loaded update!", storedKv, s.next());
  }

  // TODO add test here for making sure multiple column references with the same column family don't
  // cause an infinite loop
}
