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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InputFormat designed for PhoenixSyncTableTool that generates splits based on HBase region
 * boundaries. Filters out already-processed mapper regions using checkpoint data, enabling
 * resumable sync jobs. Uses {@link PhoenixNoOpSingleRecordReader} to invoke the mapper once per
 * split (region).
 */
public class PhoenixSyncTableInputFormat extends PhoenixInputFormat {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableInputFormat.class);

  /**
   * Instantiated by MapReduce framework
   */
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
    List<KeyRange> completedRegions = null;
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
      // completedEnd is before splitStart, increment completed pointer to catch up
      if (
        !Bytes.equals(completedEnd, HConstants.EMPTY_END_ROW)
          && Bytes.compareTo(completedEnd, splitStart) <= 0
      ) {
        completedIdx++;
      } else if (
        !Bytes.equals(splitEnd, HConstants.EMPTY_END_ROW)
          && Bytes.compareTo(completedStart, splitEnd) >= 0
      ) {
        // No overlap. completedStart is after splitEnd, splitRange needs to be processed,
        // add to unprocessed list and increment
        unprocessedSplits.add(allSplits.get(splitIdx));
        splitIdx++;
      } else {
        // Some overlap detected, check if SplitRange is fullyContained within completedRange
        // Fully contained if: completedStart <= splitStart AND splitEnd <= completedEnd

        boolean startContained = Bytes.compareTo(completedStart, splitStart) <= 0;
        boolean endContained = Bytes.equals(completedEnd, HConstants.EMPTY_END_ROW)
          || Bytes.compareTo(splitEnd, completedEnd) <= 0;

        boolean fullyContained = startContained && endContained;
        if (!fullyContained) {
          // Not fully contained, keep the split
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
}
