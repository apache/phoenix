/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test for RegionServer-level split coalescing in {@link PhoenixInputFormat} on a
 * multi-RegionServer mini-cluster. Exercises what the {@code RegionServerSplitCoalescerTest} unit
 * tests cannot: real region locations resolved from a live cluster. Verifies that enabling
 * {@link PhoenixInputFormat#SPLIT_COALESCING_ENABLED} collapses the region-granular splits into one
 * split per distinct RegionServer ({@code host:port}) while preserving every underlying region scan
 * (no rows dropped or duplicated), and that the default (disabled) path leaves the region-granular
 * splits untouched.
 * <p>
 * Note: a mini-cluster's RegionServers all share the same hostname and differ only by port, so
 * grouping by {@code host:port} ({@link PhoenixInputSplit#getRegionServerName()}) — rather than by
 * hostname — is exactly what keeps them in separate splits here. The test asserts against the
 * dynamically-computed count of distinct RegionServer identities rather than a hard-coded count.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixInputFormatSplitCoalescingIT extends BaseTest {

  private static final int SALT_BUCKETS = 4;
  private static String tableName;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    NUM_SLAVES_BASE = 2;
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS, ReadOnlyProps.EMPTY_PROPS);
    createAndPopulateTable();
  }

  /**
   * Salting pre-splits the table into {@code SALT_BUCKETS} regions at creation, giving more than
   * one region-granular split (spread across the RegionServers) for coalescing to collapse.
   */
  private static void createAndPopulateTable() throws Exception {
    tableName = generateUniqueName();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (PK INTEGER NOT NULL PRIMARY KEY, V VARCHAR) SALT_BUCKETS=" + SALT_BUCKETS);
      try (PreparedStatement upsert =
        conn.prepareStatement("UPSERT INTO " + tableName + " (PK, V) VALUES (?, ?)")) {
        for (int i = 0; i < 100; i++) {
          upsert.setInt(1, i);
          upsert.setString(2, "v" + i);
          upsert.executeUpdate();
        }
      }
      conn.commit();
    }
  }

  @Test
  public void coalescingCollapsesRegionSplitsToOnePerRegionServer() throws Exception {
    List<InputSplit> baseline = getSplits(false);
    List<InputSplit> coalesced = getSplits(true);

    assertTrue(
      "Salted table should generate more than one region-granular split, got " + baseline.size(),
      baseline.size() > 1);

    // Every region-granular split carries the host:port identity of the RegionServer hosting it.
    Set<String> servers = new HashSet<>();
    for (InputSplit split : baseline) {
      String server = ((PhoenixInputSplit) split).getRegionServerName();
      assertNotNull("Region split should have a RegionServer identity", server);
      servers.add(server);
    }

    assertEquals("Coalescing must produce exactly one split per distinct RegionServer (host:port)",
      servers.size(), coalesced.size());
    assertTrue("Coalescing must reduce the split count (coalesced " + coalesced.size()
      + " < baseline " + baseline.size() + ")", coalesced.size() < baseline.size());

    // With stats-splitting off each region is one scan, so a coalesced split holding more than one
    // scan proves multiple regions were actually merged (not just that the split count dropped).
    boolean merged =
      coalesced.stream().anyMatch(s -> ((PhoenixInputSplit) s).getScans().size() > 1);
    assertTrue("At least one coalesced split must merge multiple regions' scans", merged);

    for (InputSplit split : coalesced) {
      assertNotEquals("Coalesced split must resolve to a real RegionServer",
        RegionServerSplitCoalescer.UNKNOWN_SERVER,
        ((PhoenixInputSplit) split).getRegionServerName());
    }

    // The count/location asserts above would still pass if a whole region's scan went missing, so
    // assert coalescing preserved every region scan (this is what protects a downstream delete
    // job).
    assertEquals("Coalescing must preserve the total underlying scan count (no rows dropped)",
      countScans(baseline), countScans(coalesced));
    assertEquals("Coalescing must preserve the exact set of region scan ranges",
      scanRanges(baseline), scanRanges(coalesced));
  }

  @Test
  public void coalescingDisabledYieldsOneSplitPerRegion() throws Exception {
    // The feature is opt-in: with the flag off, getSplits must return the raw region-granular
    // splits, i.e. exactly one split per HBase region. Asserting against the live region count is a
    // direct expression of "no coalescing happened" that does not depend on how many scans a region
    // maps to (unlike PhoenixInputSplit#isCoalesced, which is really "this split has > 1 scan").
    int regionCount = getUtility().getAdmin().getRegions(TableName.valueOf(tableName)).size();
    assertTrue("Salted table should have more than one region", regionCount > 1);

    List<InputSplit> baseline = getSplits(false);
    assertEquals("With coalescing disabled, getSplits must produce one split per region",
      regionCount, baseline.size());
  }

  /**
   * Runs {@link PhoenixInputFormat#getSplits} against the live cluster with coalescing on/off.
   * Stats splitting is set to track {@code coalescingEnabled}: off for the baseline (so it yields
   * raw one-scan-per-region splits to compare against), on for the coalescing path (so
   * {@code getSplits}'s own auto-disable of stats splitting is exercised).
   */
  private List<InputSplit> getSplits(boolean coalescingEnabled) throws Exception {
    Configuration conf = new Configuration(getUtility().getConfiguration());
    Job job = Job.getInstance(conf);
    PhoenixMapReduceUtil.setInput(job, DummyDBWritable.class, tableName, null, "PK");
    PhoenixConfigurationUtil.setSplitByStats(job.getConfiguration(), coalescingEnabled);
    job.getConfiguration().setBoolean(PhoenixInputFormat.SPLIT_COALESCING_ENABLED,
      coalescingEnabled);
    return new PhoenixInputFormat<DummyDBWritable>().getSplits(job);
  }

  private static int countScans(List<InputSplit> splits) {
    int count = 0;
    for (InputSplit split : splits) {
      count += ((PhoenixInputSplit) split).getScans().size();
    }
    return count;
  }

  private static Set<String> scanRanges(List<InputSplit> splits) {
    Set<String> ranges = new HashSet<>();
    for (InputSplit split : splits) {
      for (Scan scan : ((PhoenixInputSplit) split).getScans()) {
        ranges.add(
          Bytes.toStringBinary(scan.getStartRow()) + "-" + Bytes.toStringBinary(scan.getStopRow()));
      }
    }
    return ranges;
  }

  /**
   * Minimal {@link DBWritable}; never instantiated by {@code getSplits}, only needed by setInput.
   */
  public static class DummyDBWritable implements DBWritable {
    @Override
    public void write(PreparedStatement statement) {
    }

    @Override
    public void readFields(ResultSet resultSet) {
    }
  }
}
