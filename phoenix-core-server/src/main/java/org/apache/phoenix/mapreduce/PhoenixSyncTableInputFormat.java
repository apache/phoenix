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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * InputFormat designed for PhoenixSyncTableTool that generates splits based on HBase region
 * boundaries. Filters out already-processed mapper regions using checkpoint data, enabling
 * resumable sync jobs. Uses {@link PhoenixNoOpSingleRecordReader} to invoke the mapper once per
 * split (region).
 */
public class PhoenixSyncTableInputFormat extends PhoenixInputFormat {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableInputFormat.class);

  public PhoenixSyncTableInputFormat() {
    super();
  }

  /**
   * Returns a {@link PhoenixNoOpSingleRecordReader} that emits exactly one dummy record per split.
   * <p>
   * PhoenixSyncTableMapper doesn't need actual row data from the RecordReader - it extracts region
   * boundaries from the InputSplit and delegates all scanning to the PhoenixSyncTableRegionScanner
   * coprocessor. Using PhoenixNoOpSingleRecordReader ensures that {@code map()} is called exactly
   * once per region no matter what scan looks like, avoiding the overhead of the default
   * PhoenixRecordReader which would call {@code map()} for every row of scan.
   * @param split Input Split
   * @return A PhoenixNoOpSingleRecordReader instance
   */
  @SuppressWarnings("rawtypes")
  @Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new PhoenixNoOpSingleRecordReader();
  }

  /**
   * Generates InputSplits for the Phoenix sync table job, splits are done based on region boundary
   * and then filter out already-completed regions using sync table checkpoint table.
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String tableName = PhoenixConfigurationUtil.getPhoenixSyncTableName(conf);
    String targetZkQuorum = PhoenixConfigurationUtil.getPhoenixSyncTableTargetZkQuorum(conf);
    Long fromTime = PhoenixConfigurationUtil.getPhoenixSyncTableFromTime(conf);
    Long toTime = PhoenixConfigurationUtil.getPhoenixSyncTableToTime(conf);
    List<InputSplit> allSplits = super.getSplits(context);
    if (allSplits == null || allSplits.isEmpty()) {
      throw new IOException(String.format(
        "PhoenixInputFormat generated no splits for table %s. Check table exists and has regions.",
        tableName));
    }
    LOGGER.info("Total splits generated {} of table {} for PhoenixSyncTable ", allSplits.size(),
      tableName);
    List<KeyRange> completedRegions;
    try {
      completedRegions =
        queryCompletedMapperRegions(conf, tableName, targetZkQuorum, fromTime, toTime);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    if (completedRegions.isEmpty()) {
      LOGGER.info("No completed regions for table {} - processing all {} splits", tableName,
        allSplits.size());
      return allSplits;
    }

    List<InputSplit> unprocessedSplits = filterCompletedSplits(allSplits, completedRegions);
    LOGGER.info("Found {} completed mapper regions for table {}, {} unprocessed splits remaining",
      completedRegions.size(), tableName, unprocessedSplits.size());

    // Coalesce splits to reduce mapper count and avoid hotspotting
    boolean enableCoalescing = conf.getBoolean(
      PhoenixConfigurationUtil.PHOENIX_SYNC_TABLE_ENABLE_SPLIT_COALESCING,
      PhoenixConfigurationUtil.DEFAULT_PHOENIX_SYNC_TABLE_ENABLE_SPLIT_COALESCING);

    if (enableCoalescing && unprocessedSplits.size() > 1) {
      try {
        List<InputSplit> coalescedSplits = coalesceSplits(context, unprocessedSplits);
        LOGGER.info("Split coalescing: {} unprocessed splits -> {} coalesced splits for table {}",
          unprocessedSplits.size(), coalescedSplits.size(), tableName);
        return coalescedSplits;
      } catch (Exception e) {
        LOGGER.warn("Failed to coalesce splits for table {}, falling back to uncoalesced splits: {}",
          tableName, e.getMessage(), e);
        return unprocessedSplits;
      }
    }

    return unprocessedSplits;
  }

  /**
   * Queries Sync checkpoint table for completed mapper regions
   */
  private List<KeyRange> queryCompletedMapperRegions(Configuration conf, String tableName,
    String targetZkQuorum, Long fromTime, Long toTime) throws SQLException {
    List<KeyRange> completedRegions = new ArrayList<>();
    try (Connection conn = ConnectionUtil.getInputConnection(conf)) {
      PhoenixSyncTableOutputRepository repository = new PhoenixSyncTableOutputRepository(conn);
      List<PhoenixSyncTableOutputRow> completedRows =
        repository.getProcessedMapperRegions(tableName, targetZkQuorum, fromTime, toTime);
      for (PhoenixSyncTableOutputRow row : completedRows) {
        KeyRange keyRange = KeyRange.getKeyRange(row.getStartRowKey(), row.getEndRowKey());
        completedRegions.add(keyRange);
      }
    }
    return completedRegions;
  }

  /**
   * Filters out splits that are fully contained within already completed mapper region boundary.
   * @param allSplits        All splits generated from region boundaries
   * @param completedRegions Regions already verified (from checkpoint table)
   * @return Splits that need processing
   */
  private List<InputSplit> filterCompletedSplits(List<InputSplit> allSplits,
    List<KeyRange> completedRegions) {
    allSplits.sort((s1, s2) -> {
      PhoenixInputSplit ps1 = (PhoenixInputSplit) s1;
      PhoenixInputSplit ps2 = (PhoenixInputSplit) s2;
      return KeyRange.COMPARATOR.compare(ps1.getKeyRange(), ps2.getKeyRange());
    });
    List<InputSplit> unprocessedSplits = new ArrayList<>();
    int splitIdx = 0;
    int completedIdx = 0;

    // Two pointer comparison across splitRange and completedRange
    while (splitIdx < allSplits.size() && completedIdx < completedRegions.size()) {
      PhoenixInputSplit split = (PhoenixInputSplit) allSplits.get(splitIdx);
      KeyRange splitRange = split.getKeyRange();
      KeyRange completedRange = completedRegions.get(completedIdx);
      byte[] splitStart = splitRange.getLowerRange();
      byte[] splitEnd = splitRange.getUpperRange();
      byte[] completedStart = completedRange.getLowerRange();
      byte[] completedEnd = completedRange.getUpperRange();

      // No overlap b/w completedRange/splitRange.
      // completedEnd is before splitStart, increment completed pointer to catch up. For scenario
      // like below
      // [----splitRange-----)
      // [----completed----)
      // If completedEnd is [], it means this is for last region, this check has no meaning.
      if (
        !Bytes.equals(completedEnd, HConstants.EMPTY_END_ROW)
          && Bytes.compareTo(completedEnd, splitStart) <= 0
      ) {
        completedIdx++;
      } else if (
        !Bytes.equals(splitEnd, HConstants.EMPTY_END_ROW)
          && Bytes.compareTo(completedStart, splitEnd) >= 0
      ) {
        // No overlap b/w completedRange/splitRange.
        // splitEnd is before completedStart, add this splitRange to unprocessed. For scenario like
        // below
        // [----splitRange-----)
        // [----completed----)
        // If splitEnd is [], it means this is for last region, this check has no meaning.
        unprocessedSplits.add(allSplits.get(splitIdx));
        splitIdx++;
      } else {
        // Some overlap detected, check if SplitRange is fullyContained within completedRange
        // [----splitRange-----)
        // [----completed----) // partialContained -- unprocessedSplits
        // OR
        // [----splitRange-----)
        // [----completed----) // partialContained -- unprocessedSplits
        // OR
        // [----splitRange-----------)
        // [----completed--) // partialContained -- unprocessedSplits
        // OR
        // [----splitRange-----)
        // [----completed----------) // fullyContained -- nothing to process
        boolean startContained = Bytes.compareTo(completedStart, splitStart) <= 0;
        // If we are at end of completedRange region, we can assume end boundary is always contained
        // wrt splitRange
        boolean endContained = Bytes.equals(completedEnd, HConstants.EMPTY_END_ROW)
          || Bytes.compareTo(splitEnd, completedEnd) <= 0;

        boolean fullyContained = startContained && endContained;
        if (!fullyContained) {
          unprocessedSplits.add(allSplits.get(splitIdx));
        }
        splitIdx++;
      }
    }

    // Add any remaining splits (if completed regions exhausted)
    // These splits cannot be contained since no completed regions left to check
    while (splitIdx < allSplits.size()) {
      unprocessedSplits.add(allSplits.get(splitIdx));
      splitIdx++;
    }
    return unprocessedSplits;
  }

  /**
   * Coalesces multiple region splits from the same RegionServer into single InputSplits.
   * This reduces mapper count and avoids hotspotting when many mappers hit the same server.
   * @param context JobContext for configuration access
   * @param unprocessedSplits Splits remaining after filtering completed regions
   * @return Coalesced splits with multiple regions per split
   */
  private List<InputSplit> coalesceSplits(JobContext context, List<InputSplit> unprocessedSplits)
    throws IOException, SQLException {
    Configuration conf = context.getConfiguration();
    String tableName = PhoenixConfigurationUtil.getPhoenixSyncTableName(conf);

    // Get configuration parameters
    int maxRegionsPerSplit = conf.getInt(
      PhoenixConfigurationUtil.PHOENIX_SYNC_TABLE_MAX_REGIONS_PER_SPLIT,
      PhoenixConfigurationUtil.DEFAULT_PHOENIX_SYNC_TABLE_MAX_REGIONS_PER_SPLIT);
    long maxSizePerSplit = conf.getLong(
      PhoenixConfigurationUtil.PHOENIX_SYNC_TABLE_MAX_SIZE_PER_SPLIT_BYTES,
      PhoenixConfigurationUtil.DEFAULT_PHOENIX_SYNC_TABLE_MAX_SIZE_PER_SPLIT_BYTES);

    // Get physical table name and RegionLocator
    Connection conn = ConnectionUtil.getInputConnection(conf);
    PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
    byte[] physicalTableName = pConn.getTable(tableName).getPhysicalName().getBytes();
    Admin admin = pConn.getQueryServices().getAdmin();
    RegionLocator regionLocator = admin.getRegionLocator(TableName.valueOf(physicalTableName));

    try {
      // Group splits by RegionServer location
      Map<String, List<PhoenixInputSplit>> splitsByServer =
        groupSplitsByServer(unprocessedSplits, regionLocator);

      List<InputSplit> coalescedSplits = new ArrayList<>();

      // For each RegionServer, create coalesced splits
      for (Map.Entry<String, List<PhoenixInputSplit>> entry : splitsByServer.entrySet()) {
        String serverName = entry.getKey();
        List<PhoenixInputSplit> serverSplits = entry.getValue();

        LOGGER.info("Coalescing {} splits from server {} for table {}",
          serverSplits.size(), serverName, tableName);

        // Sort splits by start key for sequential processing
        serverSplits.sort((s1, s2) ->
          Bytes.compareTo(s1.getKeyRange().getLowerRange(), s2.getKeyRange().getLowerRange()));

        // Create batches based on size and count limits
        List<PhoenixInputSplit> currentBatch = new ArrayList<>();
        long currentBatchSize = 0;

        for (PhoenixInputSplit split : serverSplits) {
          long splitSize = split.getLength();

          // Check if adding this split would exceed limits
          boolean wouldExceedCount = currentBatch.size() >= maxRegionsPerSplit;
          boolean wouldExceedSize = (currentBatchSize + splitSize) > maxSizePerSplit;

          if (currentBatch.isEmpty()) {
            // Always add first split to avoid empty batch
            currentBatch.add(split);
            currentBatchSize += splitSize;
          } else if (wouldExceedCount || wouldExceedSize) {
            // Finalize current batch and start new one
            coalescedSplits.add(createCoalescedSplit(currentBatch, serverName));
            LOGGER.debug("Created coalesced split with {} regions, {} MB from server {}",
              currentBatch.size(), currentBatchSize / (1024 * 1024), serverName);

            currentBatch = new ArrayList<>();
            currentBatch.add(split);
            currentBatchSize = splitSize;
          } else {
            // Add to current batch
            currentBatch.add(split);
            currentBatchSize += splitSize;
          }
        }

        // Don't forget the last batch for this server
        if (!currentBatch.isEmpty()) {
          coalescedSplits.add(createCoalescedSplit(currentBatch, serverName));
          LOGGER.debug("Created final coalesced split with {} regions, {} MB from server {}",
            currentBatch.size(), currentBatchSize / (1024 * 1024), serverName);
        }
      }

      return coalescedSplits;
    } finally {
      if (regionLocator != null) {
        regionLocator.close();
      }
      if (admin != null) {
        admin.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }

  /**
   * Groups splits by RegionServer location for locality-aware coalescing.
   * Uses HBase RegionLocator API to determine which server hosts each region.
   * @param splits List of splits to group
   * @param regionLocator HBase RegionLocator for querying region locations
   * @return Map of server name to list of splits hosted on that server
   */
  private Map<String, List<PhoenixInputSplit>> groupSplitsByServer(
    List<InputSplit> splits, RegionLocator regionLocator) throws IOException {

    Map<String, List<PhoenixInputSplit>> splitsByServer = new LinkedHashMap<>();

    for (InputSplit split : splits) {
      PhoenixInputSplit pSplit = (PhoenixInputSplit) split;
      KeyRange keyRange = pSplit.getKeyRange();

      // Get region location for this key range using HBase API
      HRegionLocation regionLocation = regionLocator.getRegionLocation(
        keyRange.getLowerRange(),
        false // useCache
      );

      // Get RegionServer hostname:port
      String serverName = regionLocation.getServerName().getHostAndPort();

      // Group splits by server
      splitsByServer.computeIfAbsent(serverName, k -> new ArrayList<>()).add(pSplit);

      LOGGER.debug("Split {} assigned to server {}",
        Bytes.toStringBinary(keyRange.getLowerRange()), serverName);
    }

    return splitsByServer;
  }

  /**
   * Creates a coalesced PhoenixInputSplit containing multiple regions.
   * Combines scans and KeyRanges from individual splits into a single split.
   * @param splits List of splits to coalesce (from same RegionServer)
   * @param serverLocation RegionServer location for data locality
   * @return Coalesced PhoenixInputSplit
   */
  private PhoenixInputSplit createCoalescedSplit(
    List<PhoenixInputSplit> splits, String serverLocation) throws IOException {

    if (splits.isEmpty()) {
      throw new IllegalArgumentException("Cannot create coalesced split from empty list");
    }

    if (splits.size() == 1) {
      // No coalescing needed, return original split
      return splits.get(0);
    }

    // Extract all scans and KeyRanges from individual splits
    List<Scan> allScans = new ArrayList<>();
    List<KeyRange> allKeyRanges = new ArrayList<>();
    long totalSize = 0;

    for (PhoenixInputSplit split : splits) {
      allScans.addAll(split.getScans());
      allKeyRanges.add(split.getKeyRange());
      totalSize += split.getLength();
    }

    // Create a new PhoenixInputSplit containing multiple KeyRanges
    PhoenixInputSplit coalescedSplit = new PhoenixInputSplit(
      allScans,           // Combined scans
      allKeyRanges,       // Multiple KeyRanges (one per region)
      totalSize,          // Combined size
      serverLocation      // Preferred location for mapper scheduling
    );

    LOGGER.debug("Created coalesced split: {} regions, total size {} MB, location {}",
      allKeyRanges.size(), totalSize / (1024 * 1024), serverLocation);

    return coalescedSplit;
  }
}
