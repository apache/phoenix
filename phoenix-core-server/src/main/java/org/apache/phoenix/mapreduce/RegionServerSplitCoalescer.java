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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coalesces region-boundary {@link PhoenixInputSplit}s into one split per RegionServer, reducing
 * mapper fan-out and hot spotting on large (e.g. salted) tables. Grouping uses the
 * {@code host:port} identity already stamped on each split
 * ({@link PhoenixInputSplit#getRegionServerName()}) — no region-location RPCs, and the port keeps
 * two RegionServers on one host separate. A split with no identity falls back to its hostname
 * ({@link InputSplit#getLocations()}), else {@link #UNKNOWN_SERVER}.
 */
final class RegionServerSplitCoalescer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionServerSplitCoalescer.class);

  /**
   * Sentinel key for splits with no location (empty {@link InputSplit#getLocations()}, e.g. a
   * region-in-transition). They are coalesced together rather than failing the job, since
   * coalescing is an optimisation, not a correctness requirement.
   */
  static final String UNKNOWN_SERVER = "UNKNOWN_SERVER";

  private RegionServerSplitCoalescer() {
  }

  /**
   * Coalesces the given splits by RegionServer, guarding correctness. Since coalescing must never
   * change which rows are processed, this returns the original splits unchanged when there is
   * nothing to coalesce ({@code <= 1} split or {@code null}), when the scan count is not preserved,
   * or when coalescing throws. {@link InterruptedException} is propagated (interrupt flag
   * restored).
   * @param splits region-granular splits to coalesce; may be {@code null}
   * @return the coalesced splits, or {@code splits} unchanged when coalescing is skipped or
   *         rejected
   */
  static List<InputSplit> coalesceWithGuard(List<InputSplit> splits) throws InterruptedException {
    if (splits == null || splits.size() <= 1) {
      return splits;
    }
    try {
      List<InputSplit> coalesced = coalesce(splits);
      if (!scanCountPreserved(splits, coalesced)) {
        LOGGER.error(
          "Split coalescing changed the scan count ({} -> {}); falling back to base splits to "
            + "preserve correctness",
          countScans(splits), countScans(coalesced));
        return splits;
      }
      LOGGER.info("Split coalescing: {} base splits coalesced into {} splits", splits.size(),
        coalesced.size());
      return coalesced;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (Exception e) {
      LOGGER.error("Split coalescing failed; falling back to base splits", e);
      return splits;
    }
  }

  /**
   * Coalesces region-boundary splits so all regions on the same RegionServer become one
   * {@link PhoenixInputSplit}, with each group's splits sorted by start key and their scans
   * concatenated. Performs no correctness guard, unlike {@link #coalesceWithGuard(List)}; prefer
   * the guarded entry point.
   * @param splits region-granular splits to coalesce
   * @return one coalesced split per distinct server location
   */
  static List<InputSplit> coalesce(List<InputSplit> splits)
    throws IOException, InterruptedException {
    Map<String, List<PhoenixInputSplit>> splitsByServer = groupSplitsByServer(splits);
    List<InputSplit> coalescedSplits = new ArrayList<>(splitsByServer.size());
    for (Map.Entry<String, List<PhoenixInputSplit>> entry : splitsByServer.entrySet()) {
      List<PhoenixInputSplit> serverSplits = entry.getValue();
      // Sort by start key so each mapper scans its server's regions in key order.
      serverSplits.sort((s1, s2) -> Bytes.compareTo(s1.getKeyRange().getLowerRange(),
        s2.getKeyRange().getLowerRange()));
      coalescedSplits.add(createCoalescedSplit(serverSplits, entry.getKey()));
    }
    return coalescedSplits;
  }

  /**
   * Groups splits by RegionServer identity: the {@code host:port} stamped on each split
   * ({@link PhoenixInputSplit#getRegionServerName()}), else the bare hostname
   * ({@link InputSplit#getLocations()}), else {@link #UNKNOWN_SERVER}. Uses a {@link LinkedHashMap}
   * so grouping order is deterministic (first-seen first) given a fixed input order.
   */
  private static Map<String, List<PhoenixInputSplit>> groupSplitsByServer(List<InputSplit> splits)
    throws IOException, InterruptedException {
    Map<String, List<PhoenixInputSplit>> splitsByServer = new LinkedHashMap<>();
    for (InputSplit split : splits) {
      PhoenixInputSplit pSplit = (PhoenixInputSplit) split;
      String serverName = pSplit.getRegionServerName();
      String serverKey;
      if (serverName != null && !serverName.isEmpty()) {
        serverKey = serverName;
      } else {
        String[] locations = pSplit.getLocations();
        if (locations != null && locations.length > 0 && locations[0] != null) {
          serverKey = locations[0];
          LOGGER.warn("Split {} has no RegionServer identity; grouping by hostname {} instead",
            Bytes.toStringBinary(pSplit.getKeyRange().getLowerRange()), serverKey);
        } else {
          serverKey = UNKNOWN_SERVER;
          LOGGER.warn("Split {} has no location (region may be in transition); assigning to {}",
            Bytes.toStringBinary(pSplit.getKeyRange().getLowerRange()), UNKNOWN_SERVER);
        }
      }
      splitsByServer.computeIfAbsent(serverKey, k -> new ArrayList<>()).add(pSplit);
    }
    return splitsByServer;
  }

  /**
   * Creates one coalesced {@link PhoenixInputSplit} by concatenating the scans of the given
   * per-region splits (already sorted by start key) and summing their sizes. It carries
   * {@code serverKey} as its RegionServer identity and the group's shared hostname (taken from the
   * first member) as its data-locality location.
   */
  private static PhoenixInputSplit createCoalescedSplit(List<PhoenixInputSplit> splits,
    String serverKey) throws IOException, InterruptedException {
    List<Scan> allScans = new ArrayList<>();
    long totalSize = 0;
    for (PhoenixInputSplit split : splits) {
      allScans.addAll(split.getScans());
      totalSize += split.getLength();
    }
    String[] firstLocations = splits.get(0).getLocations();
    String hostname = firstLocations.length > 0 ? firstLocations[0] : null;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Created coalesced split with {} regions from server {}", splits.size(),
        serverKey);
    }
    return new PhoenixInputSplit(allScans, totalSize, hostname, serverKey);
  }

  /**
   * Whether coalescing preserved every region scan (none dropped or duplicated). The guard
   * {@link #coalesceWithGuard(List)} uses to decide whether the coalesced result is safe.
   */
  static boolean scanCountPreserved(List<InputSplit> base, List<InputSplit> coalesced) {
    return countScans(base) == countScans(coalesced);
  }

  /**
   * Total number of scans across all splits.
   */
  private static int countScans(List<InputSplit> splits) {
    int count = 0;
    for (InputSplit split : splits) {
      count += ((PhoenixInputSplit) split).getScans().size();
    }
    return count;
  }
}
