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
package org.apache.phoenix.hbase.index.write.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;

public class TestPerRegionIndexWriteCache {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(); 
  private static TableName tableName = TableName.valueOf("t1");;
  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] family = Bytes.toBytes("family");
  private static final byte[] qual = Bytes.toBytes("qual");
  private static final byte[] val = Bytes.toBytes("val");

  private static MiniDFSCluster miniDfs = null;

  Put p = new Put(row);
  Put p2 = new Put(Bytes.toBytes("other row"));
  {
    p.addColumn(family, qual, val);
    p2.addColumn(family, qual, val);
  }

  HRegion r1; // FIXME: Uses private type
  HRegion r2; // FIXME: Uses private type
  WAL wal;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static synchronized void startDfs() throws Exception {
      miniDfs = TEST_UTIL.startMiniDFSCluster(1);
  }

  @AfterClass
  public static synchronized void stopDfs() throws Exception {
      if (miniDfs != null) {
          miniDfs.shutdown();
          miniDfs = null;
      }
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
      Path hbaseRootDir = new Path(getClass().getSimpleName() + "_" + testName.getMethodName());
      TEST_UTIL.getConfiguration().set("hbase.rootdir", hbaseRootDir.toString());

      FileSystem newFS = miniDfs.getFileSystem();
      RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).setStartKey(null).setEndKey(null).setSplit(false).build();
      Path basedir = CommonFSUtils.getTableDir(hbaseRootDir, tableName);
      Random rn = new Random();
      tableName = TableName.valueOf("TestPerRegion" + rn.nextInt());
      WALFactory walFactory = new WALFactory(TEST_UTIL.getConfiguration(), getClass().getSimpleName());
      wal = walFactory.getWAL(RegionInfoBuilder.newBuilder(TableName.valueOf("logs")).build());
        TableDescriptor htd =
                TableDescriptorBuilder
                        .newBuilder(tableName)
                        .addColumnFamily(
                            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("a")).build())
                        .build();
      
      r1 = new HRegion(basedir, wal, newFS, TEST_UTIL.getConfiguration(), hri, htd, null) {
          @Override
          public int hashCode() {
            return 1;
          }

          @Override
          public String toString() {
            return "testRegion1";
          }
        };
        
      r2 = new HRegion(basedir, wal, newFS, TEST_UTIL.getConfiguration(), hri, htd, null) {
          @Override
          public int hashCode() {
            return 2;
          }

          @Override
          public String toString() {
            return "testRegion1";
          }
        };
  }
  
  @After
  public void cleanUp() throws Exception {
      try{
          r1.close();
          r2.close();
          wal.close();
      } catch (Exception ignored) {}
      FileSystem newFS = FileSystem.get(TEST_UTIL.getConfiguration());
      newFS.delete(TEST_UTIL.getDataTestDir(), true);
  }

  
  @Test
  public void testAddRemoveSingleRegion() {
    PerRegionIndexWriteCache cache = new PerRegionIndexWriteCache();
    HTableInterfaceReference t1 = new HTableInterfaceReference(new ImmutableBytesPtr(Bytes.toBytes("t1")));
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(p);
    cache.addEdits(r1, t1, mutations);
    Multimap<HTableInterfaceReference, Mutation> edits = cache.getEdits(r1);
    Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = edits.asMap().entrySet();
    assertEquals("Got more than one table in the the edit map!", 1, entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
     //ensure that we are still storing a list here - otherwise it breaks the parallel writer implementation
      final List<Mutation> stored = (List<Mutation>) entry.getValue();
      assertEquals("Got an unexpected amount of mutations in the entry", 1, stored.size());
      assertEquals("Got an unexpected mutation in the entry", p, stored.get(0));
    }

    // ensure that a second get doesn't have any more edits. This ensures that we don't keep
    // references around to these edits and have a memory leak
    assertNull("Got an entry for a region we removed", cache.getEdits(r1));
  }

  @Test
  public void testMultipleAddsForSingleRegion() {
    PerRegionIndexWriteCache cache = new PerRegionIndexWriteCache();
    HTableInterfaceReference t1 =
        new HTableInterfaceReference(new ImmutableBytesPtr(Bytes.toBytes("t1")));
    List<Mutation> mutations = Lists.<Mutation> newArrayList(p);
    cache.addEdits(r1, t1, mutations);

    // add a second set
    mutations = Lists.<Mutation> newArrayList(p2);
    cache.addEdits(r1, t1, mutations);

    Multimap<HTableInterfaceReference, Mutation> edits = cache.getEdits(r1);
    Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = edits.asMap().entrySet();
    assertEquals("Got more than one table in the the edit map!", 1, entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      // ensure that we are still storing a list here - otherwise it breaks the parallel writer
      // implementation
      final List<Mutation> stored = (List<Mutation>) entry.getValue();
      assertEquals("Got an unexpected amount of mutations in the entry", 2, stored.size());
      assertEquals("Got an unexpected mutation in the entry", p, stored.get(0));
      assertEquals("Got an unexpected mutation in the entry", p2, stored.get(1));
    }
  }

  @Test
  public void testMultipleRegions() {
    PerRegionIndexWriteCache cache = new PerRegionIndexWriteCache();
    HTableInterfaceReference t1 =
        new HTableInterfaceReference(new ImmutableBytesPtr(Bytes.toBytes("t1")));
    List<Mutation> mutations = Lists.<Mutation> newArrayList(p);
    List<Mutation> m2 = Lists.<Mutation> newArrayList(p2);
    // add each region
    cache.addEdits(r1, t1, mutations);
    cache.addEdits(r2, t1, m2);

    // check region1
    Multimap<HTableInterfaceReference, Mutation> edits = cache.getEdits(r1);
    Set<Entry<HTableInterfaceReference, Collection<Mutation>>> entries = edits.asMap().entrySet();
    assertEquals("Got more than one table in the the edit map!", 1, entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      // ensure that we are still storing a list here - otherwise it breaks the parallel writer
      // implementation
      final List<Mutation> stored = (List<Mutation>) entry.getValue();
      assertEquals("Got an unexpected amount of mutations in the entry for region1", 1,
        stored.size());
      assertEquals("Got an unexpected mutation in the entry for region2", p, stored.get(0));
    }

    // check region2
    edits = cache.getEdits(r2);
    entries = edits.asMap().entrySet();
    assertEquals("Got more than one table in the the edit map!", 1, entries.size());
    for (Entry<HTableInterfaceReference, Collection<Mutation>> entry : entries) {
      // ensure that we are still storing a list here - otherwise it breaks the parallel writer
      // implementation
      final List<Mutation> stored = (List<Mutation>) entry.getValue();
      assertEquals("Got an unexpected amount of mutations in the entry for region2", 1,
        stored.size());
      assertEquals("Got an unexpected mutation in the entry for region2", p2, stored.get(0));
    }


    // ensure that a second get doesn't have any more edits. This ensures that we don't keep
    // references around to these edits and have a memory leak
    assertNull("Got an entry for a region we removed", cache.getEdits(r1));
  }
}
