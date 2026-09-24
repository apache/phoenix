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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.query.KeyRange;
import org.junit.Test;

/**
 * Unit tests for {@link RegionServerSplitCoalescer}, the RegionServer-level split coalescing shared
 * by any {@link PhoenixInputFormat} consumer that opts in (the sync tool, and the base
 * {@link PhoenixInputFormat#getSplits} path via {@code SPLIT_COALESCING_ENABLED}). Coalescing
 * groups region-boundary splits by the {@code host:port} RegionServer identity stamped on each
 * split, so these tests exercise the logic with plain located / unlocated
 * {@link PhoenixInputSplit}s and no cluster.
 */
public class RegionServerSplitCoalescerTest {

  /**
   * Located split: {@code [start, end)} on the given RegionServer. {@code server} is the
   * {@code host:port} identity; the hostname portion (before {@code :}) is used as the split's
   * data-locality location, mirroring how split generation stamps a split.
   */
  private PhoenixInputSplit createSplit(byte[] start, byte[] end, String server) {
    String hostname = server == null ? null : server.split(":")[0];
    return new PhoenixInputSplit(Collections.singletonList(scan(start, end)), 100L, hostname,
      server);
  }

  /** Unlocated split: {@code [start, end)} with no server (e.g. region-in-transition). */
  private PhoenixInputSplit createSplit(byte[] start, byte[] end) {
    return new PhoenixInputSplit(Collections.singletonList(scan(start, end)));
  }

  private Scan scan(byte[] start, byte[] end) {
    Scan scan = new Scan();
    scan.withStartRow(start, true);
    scan.withStopRow(end, false);
    return scan;
  }

  private static int totalScans(List<InputSplit> splits) {
    int count = 0;
    for (InputSplit split : splits) {
      count += ((PhoenixInputSplit) split).getScans().size();
    }
    return count;
  }

  @Test
  public void testCoalesceSingleServerMergesAllRegions() throws Exception {
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "server1"));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j"), "server1"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals("All regions on one server collapse to one split", 1, result.size());
    PhoenixInputSplit coalesced = (PhoenixInputSplit) result.get(0);
    assertTrue("Split should be coalesced", coalesced.isCoalesced());
    assertEquals("Should carry all 3 key ranges", 3, coalesced.getKeyRanges().size());
    assertEquals("server1", coalesced.getLocations()[0]);
    // KeyRanges sorted by start key.
    List<KeyRange> ranges = coalesced.getKeyRanges();
    assertTrue(Bytes.equals(Bytes.toBytes("a"), ranges.get(0).getLowerRange()));
    assertTrue(Bytes.equals(Bytes.toBytes("d"), ranges.get(1).getLowerRange()));
    assertTrue(Bytes.equals(Bytes.toBytes("g"), ranges.get(2).getLowerRange()));
    assertEquals("Scan count preserved", totalScans(splits), totalScans(result));
  }

  @Test
  public void testCoalesceGroupsByServer() throws Exception {
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("c"), "server1"));
    splits.add(createSplit(Bytes.toBytes("c"), Bytes.toBytes("e"), "server1"));
    splits.add(createSplit(Bytes.toBytes("e"), Bytes.toBytes("g"), "server1"));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("i"), "server2"));
    splits.add(createSplit(Bytes.toBytes("i"), Bytes.toBytes("k"), "server2"));
    splits.add(createSplit(Bytes.toBytes("k"), Bytes.toBytes("m"), "server2"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals("One coalesced split per server", 2, result.size());
    for (InputSplit s : result) {
      PhoenixInputSplit ps = (PhoenixInputSplit) s;
      assertTrue("Each server split should be coalesced", ps.isCoalesced());
      assertEquals("Each server hosts 3 regions", 3, ps.getKeyRanges().size());
      // Ranges within a server group are sorted by start key.
      List<KeyRange> ranges = ps.getKeyRanges();
      for (int i = 0; i < ranges.size() - 1; i++) {
        assertTrue("KeyRanges should be sorted within a server group",
          Bytes.compareTo(ranges.get(i).getLowerRange(), ranges.get(i + 1).getLowerRange()) < 0);
      }
    }
    assertEquals("Scan count preserved", totalScans(splits), totalScans(result));
  }

  @Test
  public void testCoalesceSortsUnorderedInput() throws Exception {
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j"), "server1"));
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "server1"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals(1, result.size());
    List<KeyRange> ranges = ((PhoenixInputSplit) result.get(0)).getKeyRanges();
    assertTrue(Bytes.equals(Bytes.toBytes("a"), ranges.get(0).getLowerRange()));
    assertTrue(Bytes.equals(Bytes.toBytes("d"), ranges.get(1).getLowerRange()));
    assertTrue(Bytes.equals(Bytes.toBytes("g"), ranges.get(2).getLowerRange()));
  }

  @Test
  public void testCoalesceEmptyList() throws Exception {
    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(new ArrayList<>());
    assertEquals(0, result.size());
  }

  @Test
  public void testCoalesceSingleSplitNotMarkedCoalesced() throws Exception {
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals(1, result.size());
    assertFalse("Single region should not be marked coalesced",
      ((PhoenixInputSplit) result.get(0)).isCoalesced());
  }

  @Test
  public void testCoalesceUnlocatedSplitGoesToUnknownServer() throws Exception {
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals(1, result.size());
    assertEquals(RegionServerSplitCoalescer.UNKNOWN_SERVER,
      ((PhoenixInputSplit) result.get(0)).getRegionServerName());
  }

  @Test
  public void testCoalesceMultipleUnlocatedSplitsGroupIntoOneUnknownServer() throws Exception {
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d")));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g")));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j")));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals("All unlocated splits collapse into one UNKNOWN_SERVER split", 1, result.size());
    PhoenixInputSplit coalesced = (PhoenixInputSplit) result.get(0);
    assertEquals(RegionServerSplitCoalescer.UNKNOWN_SERVER, coalesced.getRegionServerName());
    assertEquals(3, coalesced.getKeyRanges().size());
  }

  @Test
  public void testCoalesceMixedLocatedAndUnlocated() throws Exception {
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "server1"));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j")));
    splits.add(createSplit(Bytes.toBytes("j"), Bytes.toBytes("m")));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals("server1 bucket + UNKNOWN_SERVER bucket", 2, result.size());
    List<String> servers = new ArrayList<>();
    for (InputSplit s : result) {
      servers.add(((PhoenixInputSplit) s).getRegionServerName());
    }
    assertTrue(servers.contains("server1"));
    assertTrue(servers.contains(RegionServerSplitCoalescer.UNKNOWN_SERVER));
    assertEquals("Scan count preserved", totalScans(splits), totalScans(result));
  }

  @Test
  public void testCoalescePreservesMultiScanSplits() throws Exception {
    // A single region split can already hold more than one scan; coalescing must keep every scan.
    PhoenixInputSplit multiScan =
      new PhoenixInputSplit(Arrays.asList(scan(Bytes.toBytes("a"), Bytes.toBytes("b")),
        scan(Bytes.toBytes("b"), Bytes.toBytes("d"))), 100L, "server1", "server1");
    List<InputSplit> splits = new ArrayList<>();
    splits.add(multiScan);
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "server1"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals(1, result.size());
    assertEquals("All 3 scans retained", 3, ((PhoenixInputSplit) result.get(0)).getScans().size());
    assertEquals("Scan count preserved", totalScans(splits), totalScans(result));
  }

  @Test
  public void testCoalescedSplitSizeIsSumOfMembers() throws Exception {
    // The coalesced split's length is the sum of its members' lengths (each createSplit uses 100L),
    // so YARN sees the combined mapper's true size when ordering splits.
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "server1"));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j"), "server1"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals(1, result.size());
    assertEquals("Coalesced split length is the sum of member lengths", 300L,
      result.get(0).getLength());
  }

  @Test
  public void testCoalesceWithGuardSingleSplitReturnsBaseUnchanged() throws Exception {
    List<InputSplit> base = new ArrayList<>();
    base.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesceWithGuard(base);

    assertSame("Nothing to coalesce (<= 1 split) returns base unchanged", base, result);
  }

  @Test
  public void testCoalesceWithGuardNullReturnsNull() throws Exception {
    assertSame(null, RegionServerSplitCoalescer.coalesceWithGuard(null));
  }

  @Test
  public void testCoalesceWithGuardCoalesces() throws Exception {
    List<InputSplit> base = new ArrayList<>();
    base.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));
    base.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "server1"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesceWithGuard(base);

    assertEquals("Both regions on server1 coalesce into one split", 1, result.size());
    assertEquals("Scan count preserved", totalScans(base), totalScans(result));
  }

  @Test
  public void testScanCountPreservedGuardDecision() throws Exception {
    // The guard that coalesceWithGuard uses to reject a coalesced result that lost or gained a
    // scan.
    // Dropping a scan would silently skip rows in a delete job, so a mismatch must be rejected.
    List<InputSplit> base = new ArrayList<>();
    base.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));
    base.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "server1"));

    List<InputSplit> faithful = RegionServerSplitCoalescer.coalesce(base);
    assertTrue("A correct coalesce preserves the scan count",
      RegionServerSplitCoalescer.scanCountPreserved(base, faithful));

    // A "coalesced" result that dropped a scan (only the first region) must be rejected.
    List<InputSplit> dropped = new ArrayList<>(Collections.singletonList(base.get(0)));
    assertFalse("A coalesce that drops a scan must be rejected",
      RegionServerSplitCoalescer.scanCountPreserved(base, dropped));
  }

  @Test
  public void testCoalesceGroupsByHostPortNotHostname() throws Exception {
    // Two RegionServer processes on the same host share a hostname but differ by port. They must be
    // kept in separate splits (one split per RegionServer, not per host).
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "host1:16020"));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "host1:16020"));
    splits.add(createSplit(Bytes.toBytes("g"), Bytes.toBytes("j"), "host1:16030"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals("Same host, different port must not be merged", 2, result.size());
    List<String> servers = new ArrayList<>();
    for (InputSplit s : result) {
      servers.add(((PhoenixInputSplit) s).getRegionServerName());
    }
    assertTrue(servers.contains("host1:16020"));
    assertTrue(servers.contains("host1:16030"));
    assertEquals("Scan count preserved", totalScans(splits), totalScans(result));
  }

  @Test
  public void testCoalescedSplitKeepsHostnameForLocality() throws Exception {
    // The coalesced split must expose the bare hostname (not host:port) via getLocations() so the
    // MapReduce framework can still schedule the mapper data-locally, while getRegionServerName()
    // carries the full host:port identity it was grouped on.
    List<InputSplit> splits = new ArrayList<>();
    splits.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "host1:16020"));
    splits.add(createSplit(Bytes.toBytes("d"), Bytes.toBytes("g"), "host1:16020"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals(1, result.size());
    PhoenixInputSplit coalesced = (PhoenixInputSplit) result.get(0);
    assertEquals("Locality location is the bare hostname", "host1", coalesced.getLocations()[0]);
    assertEquals("Server identity is the full host:port", "host1:16020",
      coalesced.getRegionServerName());
  }

  @Test
  public void testCoalesceFallsBackToHostnameWhenNoServerIdentity() throws Exception {
    // A split with a hostname but no host:port identity (e.g. built by a path that does not stamp
    // the RegionServer) groups by hostname rather than collapsing into UNKNOWN_SERVER.
    List<InputSplit> splits = new ArrayList<>();
    splits.add(new PhoenixInputSplit(
      Collections.singletonList(scan(Bytes.toBytes("a"), Bytes.toBytes("d"))), 100L, "host1"));
    splits.add(new PhoenixInputSplit(
      Collections.singletonList(scan(Bytes.toBytes("d"), Bytes.toBytes("g"))), 100L, "host1"));

    List<InputSplit> result = RegionServerSplitCoalescer.coalesce(splits);

    assertEquals("Both group by hostname into one split", 1, result.size());
    assertEquals("host1", ((PhoenixInputSplit) result.get(0)).getRegionServerName());
    assertEquals("Scan count preserved", totalScans(splits), totalScans(result));
  }

  @Test
  public void testCoalesceWithGuardFallsBackToBaseOnError() throws Exception {
    // A split that is not a PhoenixInputSplit triggers a ClassCastException inside coalescing. The
    // guard must swallow it and return the base splits unchanged rather than fail the job -- the
    // graceful degradation the sync tool relies on in production.
    List<InputSplit> base = new ArrayList<>();
    base.add(createSplit(Bytes.toBytes("a"), Bytes.toBytes("d"), "server1"));
    base.add(new NonPhoenixInputSplit());

    List<InputSplit> result = RegionServerSplitCoalescer.coalesceWithGuard(base);

    assertSame("On a coalescing error the base splits are returned unchanged", base, result);
  }

  /** A non-{@link PhoenixInputSplit} used to force a coalescing failure (ClassCastException). */
  private static class NonPhoenixInputSplit extends InputSplit {
    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return new String[0];
    }
  }
}
