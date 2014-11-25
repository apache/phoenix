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
package org.apache.phoenix.hbase.index.covered.example;


import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexTestingUtils;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.TableName;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test Covered Column indexing in an 'end-to-end' manner on a minicluster. This covers cases where
 * we manage custom timestamped updates that arrive in and out of order as well as just using the
 * generically timestamped updates.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class EndToEndCoveredIndexingIT {
  private static final Log LOG = LogFactory.getLog(EndToEndCoveredIndexingIT.class);
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String FAM_STRING = "FAMILY";
  private static final byte[] FAM = Bytes.toBytes(FAM_STRING);
  private static final String FAM2_STRING = "FAMILY2";
  private static final byte[] FAM2 = Bytes.toBytes(FAM2_STRING);
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final byte[] indexed_qualifer = Bytes.toBytes("indexed_qual");
  private static final byte[] regular_qualifer = Bytes.toBytes("reg_qual");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] value1 = Bytes.toBytes("val1");
  private static final byte[] value2 = Bytes.toBytes("val2");
  private static final byte[] value3 = Bytes.toBytes("val3");
  // match a single family:qualifier pair
  private static final CoveredColumn col1 = new CoveredColumn(FAM_STRING, indexed_qualifer);
  // matches the family2:* columns
  private static final CoveredColumn col2 = new CoveredColumn(FAM2_STRING, null);
  private static final CoveredColumn col3 = new CoveredColumn(FAM2_STRING, indexed_qualifer);
  
  @Rule
  public TableName TestTable = new TableName();
  
  private ColumnGroup fam1;
  private ColumnGroup fam2;

  // setup a couple of index columns
  private void setupColumns() {
    fam1 = new ColumnGroup(getIndexTableName());
    fam2 = new ColumnGroup(getIndexTableName() + "2");
    // values are [col1][col2_1]...[col2_n]
    fam1.add(col1);
    fam1.add(col2);
    // value is [col2]
    fam2.add(col3);
  }

  private String getIndexTableName() {
    return Bytes.toString(TestTable.getTableName()) + "_index";
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    setUpConfigForMiniCluster(conf);
    IndexTestingUtils.setupConfig(conf);
    // disable version checking, so we can test against whatever version of HBase happens to be
    // installed (right now, its generally going to be SNAPSHOT versions).
    conf.setBoolean(Indexer.CHECK_VERSION_CONF_KEY, false);
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    setupColumns();
  }

  /**
   * Test that a bunch of puts with a single timestamp across all the puts builds and inserts index
   * entries as expected
   * @throws Exception on failure
   */
  @Test
  public void testSimpleTimestampedUpdates() throws Exception {
    HTable primary = createSetupTables(fam1);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts = 10;
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), getIndexTableName());

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);

    // verify that the index matches
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Test that the multiple timestamps in a single put build the correct index updates.
   * @throws Exception on failure
   */
  @Test
  public void testMultipleTimestampsInSinglePut() throws Exception {
    HTable primary = createSetupTables(fam1);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts1 = 10;
    long ts2 = 11;
    p.add(FAM, indexed_qualifer, ts1, value1);
    p.add(FAM, regular_qualifer, ts1, value2);
    // our group indexes all columns in the this family, so any qualifier here is ok
    p.add(FAM2, regular_qualifer, ts2, value3);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), getIndexTableName());

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts1
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts1, value1);

    // check the second entry at ts2
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts2, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Test that we make updates to multiple {@link ColumnGroup}s across a single put/delete 
   * @throws Exception on failure
   */
  @Test
  public void testMultipleConcurrentGroupsUpdated() throws Exception {
    HTable primary = createSetupTables(fam1, fam2);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts = 10;
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    p.add(FAM2, indexed_qualifer, ts, value3);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), fam1.getTable());
    HTable index2 = new HTable(UTIL.getConfiguration(), fam2.getTable());

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // and check the second index as well
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col3));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index2, expected, ts, value3);

    // cleanup
    closeAndCleanupTables(primary, index1, index2);
  }

  /**
   * HBase has a 'fun' property wherein you can completely clobber an existing row if you make a
   * {@link Put} at the exact same dimension (row, cf, cq, ts) as an existing row. The old row
   * disappears and the new value (since the rest of the row is the same) completely subsumes it.
   * This test ensures that we remove the old entry and put a new entry in its place.
   * @throws Exception on failure
   */
  @Test
  public void testOverwritingPutsCorrectlyGetIndexed() throws Exception {
    HTable primary = createSetupTables(fam1);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts = 10;
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), fam1.getTable());

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // now overwrite the put in the primary table with a new value
    p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts, value3);
    primary.put(p);
    primary.flushCommits();

    pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, value3);
    // and verify that a scan at the first entry returns nothing (ignore the updated row)
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, Collections.<KeyValue> emptyList(), ts,
      value1, value2);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }
  
  @Test
  public void testSimpleDeletes() throws Exception {
    HTable primary = createSetupTables(fam1);

    // do a simple Put
    long ts = 10;
    Put p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    primary.put(p);
    primary.flushCommits();

    Delete d = new Delete(row1);
    primary.delete(d);

    HTable index = new HTable(UTIL.getConfiguration(), fam1.getTable());
    List<KeyValue> expected = Collections.<KeyValue> emptyList();
    // scan over all time should cause the delete to be covered
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, expected, 0, Long.MAX_VALUE, value1,
      HConstants.EMPTY_END_ROW);

    // scan at the older timestamp should still show the older value
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, expected, ts, value1);

    // cleanup
    closeAndCleanupTables(index, primary);
  }

  /**
   * If we don't have any updates to make to the index, we don't take a lock on the WAL. However, we
   * need to make sure that we don't try to unlock the WAL on write time when we don't write
   * anything, since that will cause an java.lang.IllegalMonitorStateException
   * @throws Exception on failure
   */
  @Test
  public void testDeletesWithoutPreviousState() throws Exception {
    HTable primary = createSetupTables(fam1);

    // do a delete on the primary table (no data, so no index updates...hopefully).
    long ts = 10;
    Delete d = new Delete(row1);
    primary.delete(d);

    HTable index1 = new HTable(UTIL.getConfiguration(), fam1.getTable());
    List<KeyValue> expected = Collections.<KeyValue> emptyList();
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // a delete of a specific family/column should also not show any index updates
    d = new Delete(row1);
    d.deleteColumn(FAM, indexed_qualifer);
    primary.delete(d);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // also just a family marker should have the same effect
    d = new Delete(row1);
    d.deleteFamily(FAM);
    primary.delete(d);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // also just a family marker should have the same effect
    d = new Delete(row1);
    d.deleteColumns(FAM, indexed_qualifer);
    primary.delete(d);
    primary.flushCommits();
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Similar to the {@link #testMultipleTimestampsInSinglePut()}, this check the same with deletes
   * @throws Exception on failure
   */
  @Test
  public void testMultipleTimestampsInSingleDelete() throws Exception {
    HTable primary = createSetupTables(fam1);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts1 = 10, ts2 = 11, ts3 = 12;
    p.add(FAM, indexed_qualifer, ts1, value1);
    // our group indexes all columns in the this family, so any qualifier here is ok
    p.add(FAM2, regular_qualifer, ts2, value3);
    primary.put(p);
    primary.flushCommits();

    // check to make sure everything we expect is there
    HTable index1 = new HTable(UTIL.getConfiguration(), fam1.getTable());

    // ts1, we just have v1
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts1, value1);

    // at ts2, don't have the above anymore
    pairs.clear();
    expected = Collections.emptyList();
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts2, ts2 + 1, value1, value1);
    // but we do have the new entry at ts2
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts2, value1);

    // now build up a delete with a couple different timestamps
    Delete d = new Delete(row1);
    // these deletes have to match the exact ts since we are doing an exact match (deleteColumn).
    d.deleteColumn(FAM, indexed_qualifer, ts1);
    // since this doesn't match exactly, we actually shouldn't see a change in table state
    d.deleteColumn(FAM2, regular_qualifer, ts3);
    primary.delete(d);

    // at ts1, we should have the put covered exactly by the delete and into the entire future
    expected = Collections.emptyList();
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts1, Long.MAX_VALUE, value1,
      value1);

    // at ts2, we should just see value3
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts2, value1);

    // the later delete is a point delete, so we shouldn't see any change at ts3
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts2, ts3, value1,
      HConstants.EMPTY_END_ROW);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Covering deletes (via {@link Delete#deleteColumns}) cover everything back in time from the
   * given time. If its modifying the latest state, we don't need to do anything but add deletes. If
   * its modifying back in time state, we need to just fix up the surrounding elements as anything
   * else ahead of it will be fixed up by later updates.
   * <p>
   * similar to {@link #testMultipleTimestampsInSingleDelete()}, but with covering deletes.
   * @throws Exception on failure
   */
  @Test
  public void testDeleteColumnsInThePast() throws Exception {
    HTable primary = createSetupTables(fam1);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts1 = 10, ts2 = 11, ts3 = 12;
    p.add(FAM, indexed_qualifer, ts1, value1);
    p.add(FAM2, regular_qualifer, ts2, value3);
    primary.put(p);
    primary.flushCommits();

    // now build up a delete with a couple different timestamps
    Delete d = new Delete(row1);
    // these deletes don't need to match the exact ts because they cover everything earlier
    d.deleteColumns(FAM, indexed_qualifer, ts2);
    d.deleteColumns(FAM2, regular_qualifer, ts3);
    primary.delete(d);

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), fam1.getTable());

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts1
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts1, value1);

    // delete at ts2 changes what the put would insert
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts2, value1);

    // final delete clears out everything
    expected = Collections.emptyList();
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts3, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }
  
  /**
   * If the client is using custom timestamps is possible that the updates come out-of-order (i.e.
   * update to ts 10 comes after the update to ts 12). In the case, we need to be sure that the
   * index is correctly updated when the out of order put arrives.
   * @throws Exception
   */
  @Test
  public void testOutOfOrderUpdates() throws Exception {
    HTable primary = createSetupTables(fam1);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts = 12;
    p.add(FAM, indexed_qualifer, ts, value1);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), fam1.getTable());

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts
    List<KeyValue> expectedTs1 = CoveredColumnIndexCodec
        .getIndexKeyValueForTesting(row1, ts, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expectedTs1, ts, value1);

    // now make a put back in time
    long ts2 = ts - 2;
    p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts2, value2);
    primary.put(p);
    primary.flushCommits();

    pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value2, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check to make sure the back in time entry exists
    List<KeyValue> expectedTs2 = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2,
      pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expectedTs2, ts2, value2);
    // then it should be gone at the newer ts (because it deletes itself)
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, Collections.<KeyValue> emptyList(), ts2,
      ts + 1, value2, HConstants.EMPTY_END_ROW);

    // but that the original index entry is still visible at ts, just fine
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expectedTs1, ts, value1);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Its possible (i.e. from a fast, frequently writing client) that they put more than the
   * 'visible' number of versions in a row before a client make a put 'back in time' on that row. If
   * we don't scan the current table properly, we won't see an index update for that 'back in time'
   * update since the usual lookup will only see the regular number of versions. This ability to see
   * back in time depends on running HBase 0.94.9
   * @throws Exception on failure
   */
  @Test
  public void testExceedVersionsOutOfOrderPut() throws Exception {
    // setup the index
    HTable primary = createSetupTables(fam2);

    // do a put to the primary table
    Put p = new Put(row1);
    long ts1 = 1, ts2 = 2, ts3 = 3, ts4 = 4, ts5 = 5;
    byte[] value4 = Bytes.toBytes("val4");
    byte[] value5 = Bytes.toBytes("val5");
    p.add(FAM2, indexed_qualifer, ts1, value1);
    primary.put(p);
    primary.flushCommits();

    p = new Put(row1);
    p.add(FAM2, indexed_qualifer, ts3, value3);
    primary.put(p);
    primary.flushCommits();

    p = new Put(row1);
    p.add(FAM2, indexed_qualifer, ts4, value4);
    primary.put(p);
    primary.flushCommits();

    p = new Put(row1);
    p.add(FAM2, indexed_qualifer, ts5, value5);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index = new HTable(UTIL.getConfiguration(), fam2.getTable());

    // do a raw scan of everything in the table
    if (LOG.isDebugEnabled()) {
      // the whole table, all the keys
      Scan s = new Scan();
      s.setRaw(true);
      ResultScanner scanner = index.getScanner(s);
      for (Result r : scanner) {
        LOG.debug("Found row:" + r);
      }
      scanner.close();
    }

    /*
     * now we have definitely exceeded the number of versions visible to a usual client of the
     * primary table, so we should try doing a put 'back in time' an make sure that has the correct
     * index values and cleanup
     */
    p = new Put(row1);
    p.add(FAM2, indexed_qualifer, ts2, value2);
    primary.put(p);
    primary.flushCommits();

    // // read the index for the expected values
    // HTable index = new HTable(UTIL.getConfiguration(), fam2.getTable());
    //
    // do a raw scan of everything in the table
    if (LOG.isDebugEnabled()) {
      // the whole table, all the keys
      Scan s = new Scan();
      s.setRaw(true);
      ResultScanner scanner = index.getScanner(s);
      for (Result r : scanner) {
        LOG.debug("Found row:" + r);
      }
      scanner.close();
    }

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col3));

    // check the value1 should be present at the earliest timestamp
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, expected, ts1, value1, value2);

    // and value1 should be removed at ts2 (even though it came later)
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, Collections.<KeyValue> emptyList(), ts1,
      ts2 + 1, value1, value2); // timestamp + 1 since its exclusive end timestamp

    // late added column should be there just fine at its timestamp
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value2, col3));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, expected, ts2, value2);

    // and check that the late entry also removes its self at the next timestamp up
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, Collections.<KeyValue> emptyList(), ts3,
      value2, value3);

    // then we should have the rest of the inserts at their appropriate timestamps. Everything else
    // should be exactly the same, except we shouldn't see ts0 anymore at ts2

    // check the third entry
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col3));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts3, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, expected, ts3, value3);

    // check the fourth entry
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value4, col3));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts4, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, expected, ts4, value4);

    // check the first entry at ts4
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value2, col3));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, expected, ts2, value2);
    // verify that we remove the entry, even though its too far 'back in time'
    IndexTestingUtils.verifyIndexTableAtTimestamp(index, Collections.<KeyValue> emptyList(), ts3,
      value4);

    // cleanup
    closeAndCleanupTables(primary, index);
  }

  /**
   * Similar to {@link #testExceedVersionsOutOfOrderPut()}, but mingles deletes and puts.
   * @throws Exception on failure
   */
  @Test
  public void testExceedVersionsOutOfOrderUpdates() throws Exception {
    HTable primary = createSetupTables(fam1);

    // setup the data to store
    long ts1 = 1, ts2 = 2, ts3 = 3, ts4 = 4, ts5 = 5, ts6 = 6;
    byte[] value4 = Bytes.toBytes("val4"), value5 = Bytes.toBytes("val5"), value6 =
        Bytes.toBytes("val6");
    // values for the other column to index
    byte[] v1_1 = ArrayUtils.addAll(value1, Bytes.toBytes("_otherCol")), v3_1 =
        ArrayUtils.addAll(value3, Bytes.toBytes("_otherCol")), v5_1 =
        ArrayUtils.addAll(value5, Bytes.toBytes("_otherCol")), v6_1 =
        ArrayUtils.addAll(value6, Bytes.toBytes("_otherCol"));

    // make some puts to the primary table
    Put p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts1, value1);
    p.add(FAM2, indexed_qualifer, ts1, v1_1);
    primary.put(p);
    primary.flushCommits();

    p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts3, value3);
    p.add(FAM2, indexed_qualifer, ts3, v3_1);
    primary.put(p);
    primary.flushCommits();

    p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts5, value5);
    p.add(FAM2, indexed_qualifer, ts5, v5_1);
    primary.put(p);
    primary.flushCommits();

    p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts6, value6);
    p.add(FAM2, indexed_qualifer, ts6, v6_1);
    primary.put(p);
    primary.flushCommits();

    /*
     * now we have definitely exceeded the number of versions visible to a usual client of the
     * primary table, so we should try doing a put 'back in time' an make sure that has the correct
     * index values and cleanup
     */
    p = new Put(row1);
    p.add(FAM, indexed_qualifer, ts2, value2);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), fam1.getTable());

    // do a raw scan of everything in the table
    if (LOG.isDebugEnabled()) {
      Scan s = new Scan();
      s.setRaw(true);
      ResultScanner scanner = index1.getScanner(s);
      for (Result r : scanner) {
        LOG.debug("Found row:" + r);
      }
      scanner.close();
    }

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(v1_1, col2));

    // check the value1 should be present at the earliest timestamp
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts1, value1, value2);

    // and value1 should be removed at ts2 (even though it came later)
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, Collections.<KeyValue> emptyList(), ts1,
      ts2 + 1, value1, value2); // timestamp + 1 since its exclusive end timestamp

    // late added column should be there just fine at its timestamp
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value2, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(v1_1, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts2, value2);

    // and check that the late entry also removes its self at the next timestamp up
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, Collections.<KeyValue> emptyList(), ts3,
      value2, value3);

    // -----------------------------------------------
    // Check Delete intermingled
    // -----------------------------------------------

    // verify that the old row is there
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(v3_1, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts3, pairs);
    // scan from the start key forward (should only include [value3][v3_3])
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts3, expected.get(0).getKey(),
      value4);

    // then do a delete of just one of the indexed columns. This should insert a delete for all just
    // the single value, then a put & a later corresponding in the past for the new value
    Delete d = new Delete(row1);
    d.deleteColumn(FAM2, indexed_qualifer, ts3);
    primary.delete(d);

    // we shouldn't find that entry, but we should find [value3][v1_1] since that is next entry back
    // in time from the current
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(v1_1, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts3, pairs);
    // it should be re-written at 3
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts3, value3, value4);

    // but we shouldn't find it at ts5 since it should be covered again
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, Collections.<KeyValue> emptyList(), ts5,
      value3, value4);

    // now remove all the older columns in FAM2 at 4
    d = new Delete(row1);
    d.deleteColumns(FAM2, indexed_qualifer, ts4);
    primary.delete(d);

    // we shouldn't find that entry, but we should find [value3][null] since deleteColumns removes
    // all the entries for that column
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts4, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts4, value3, value4);

    // same as above, but now do it at 3 (on earlier)
    d = new Delete(row1);
    d.deleteColumns(FAM2, indexed_qualifer, ts3);
    primary.delete(d);

    // we shouldn't find that entry, but we should find [value3][null] since deleteColumns removes
    // all the entries for that column
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts3, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts3, value3, value4);

    // -----------------------------------------------
    // then we should have the rest of the inserts at their appropriate timestamps. Everything else
    // should be exactly the same, except we shouldn't see ts0 anymore at ts2
    // -----------------------------------------------

    // check the entry at 5
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value5, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(v5_1, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts5, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts5, value5);

    // check the entry at 6
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value6, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(v6_1, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts6, pairs);
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts6, value5);

    // cleanup
    closeAndCleanupTables(primary, index1);
  }

  /**
   * Create the primary table (to which you should write), setup properly for indexing the given
   * {@link ColumnGroup}s. Also creates the necessary index tables to match the passes groups.
   * @param groups {@link ColumnGroup}s to index, creating one index table per column group.
   * @return reference to the primary table
   * @throws IOException if there is an issue communicating with HBase
   */
  private HTable createSetupTables(ColumnGroup... groups) throws IOException {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    for (ColumnGroup group : groups) {
      builder.addIndexGroup(group);
      // create the index tables
      CoveredColumnIndexer.createIndexTable(admin, group.getTable());
    }

    // setup the primary table
    String indexedTableName = Bytes.toString(TestTable.getTableName());
    @SuppressWarnings("deprecation")
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);
    primary.setAutoFlush(false);
    return primary;
  }

  private void closeAndCleanupTables(HTable... tables) throws IOException {
    if (tables == null) {
      return;
    }

    for (HTable table : tables) {
      table.close();
      UTIL.deleteTable(table.getTableName());
    }
  }
}
