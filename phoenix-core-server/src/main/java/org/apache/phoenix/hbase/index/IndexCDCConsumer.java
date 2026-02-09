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
package org.apache.phoenix.hbase.index;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.DelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.generated.IndexMutationsProtos;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.IndexConsistency;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * A single-threaded background consumer that processes CDC mutations for eventually consistent
 * indexes. This consumer reads mutations from the CDC index table and applies them to the
 * appropriate secondary indexes.
 * <p>
 * The consumer tracks its progress in the SYSTEM.IDX_CDC_TRACKER table, allowing for proper
 * handling of region splits and merges.
 * </p>
 * <p>
 * <b>Lifecycle:</b>
 * <ol>
 * <li>On startup, the consumer first replays any remaining mutations from parent regions (if this
 * region was created from a split or merge). A region will have multiple parent regions in case of
 * region merge.</li>
 * <li>After replaying all parent mutations, the consumer marks each parent region as COMPLETE in
 * SYSTEM.IDX_CDC_TRACKER.</li>
 * <li>The consumer then begins processing mutations for the current region. With every batch of
 * rows consumed (e.g., 1000 rows), it updates LAST_TIMESTAMP in SYSTEM.IDX_CDC_TRACKER. The tracker
 * entry with status IN_PROGRESS is created on the first batch update.</li>
 * <li>When the region is closed (due to split, merge, or shutdown), the consumer stops but does NOT
 * mark itself as COMPLETE. The COMPLETE status is only set by child regions after they have
 * replayed all parent mutations.</li>
 * </ol>
 * </p>
 */
public class IndexCDCConsumer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(IndexCDCConsumer.class);

  public static final String INDEX_CDC_CONSUMER_BATCH_SIZE =
    "phoenix.index.cdc.consumer.batch.size";
  private static final int DEFAULT_CDC_BATCH_SIZE = 1000;

  public static final String INDEX_CDC_CONSUMER_STARTUP_DELAY_MS =
    "phoenix.index.cdc.consumer.startup.delay.ms";
  private static final long DEFAULT_STARTUP_DELAY_MS = 10000;

  /**
   * The interval in milliseconds between processing batches when mutations are found.
   */
  public static final String INDEX_CDC_CONSUMER_POLL_INTERVAL_MS =
    "phoenix.index.cdc.consumer.poll.interval.ms";
  private static final long DEFAULT_POLL_INTERVAL_MS = 1000;

  /**
   * The time buffer in milliseconds subtracted from current time when querying CDC mutations. This
   * buffer helps avoid reading mutations that are too recent.
   */
  public static final String INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS =
    "phoenix.index.cdc.consumer.timestamp.buffer.ms";
  private static final long DEFAULT_TIMESTAMP_BUFFER_MS = 1000;

  private final RegionCoprocessorEnvironment env;
  private final String dataTableName;
  private final String encodedRegionName;
  private final IndexWriter indexWriter;
  private final long pause;
  private final long startupDelayMs;
  private final int batchSize;
  private final long pollIntervalMs;
  private final long timestampBufferMs;
  private final Configuration config;
  private volatile boolean stopped = false;
  private Thread consumerThread;
  private boolean hasParentPartitions = false;
  private PTable cachedDataTable;

  /**
   * Creates a new IndexCDCConsumer for the given region.
   * @param env           region coprocessor environment.
   * @param dataTableName name of the data table.
   * @param serverName    server name.
   * @throws IOException if the IndexWriter cannot be created.
   */
  public IndexCDCConsumer(RegionCoprocessorEnvironment env, String dataTableName, String serverName)
    throws IOException {
    this.env = env;
    this.dataTableName = dataTableName;
    this.encodedRegionName = env.getRegion().getRegionInfo().getEncodedName();
    this.config = env.getConfiguration();
    this.pause = config.getLong(HConstants.HBASE_CLIENT_PAUSE, 300);
    this.startupDelayMs =
      config.getLong(INDEX_CDC_CONSUMER_STARTUP_DELAY_MS, DEFAULT_STARTUP_DELAY_MS);
    this.batchSize = config.getInt(INDEX_CDC_CONSUMER_BATCH_SIZE, DEFAULT_CDC_BATCH_SIZE);
    this.pollIntervalMs =
      config.getLong(INDEX_CDC_CONSUMER_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
    this.timestampBufferMs =
      config.getLong(INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS, DEFAULT_TIMESTAMP_BUFFER_MS);
    DelegateRegionCoprocessorEnvironment indexWriterEnv =
      new DelegateRegionCoprocessorEnvironment(env, ConnectionType.INDEX_WRITER_CONNECTION);
    this.indexWriter =
      new IndexWriter(indexWriterEnv, serverName + "-index-eventual-writer", false);
  }

  /**
   * Starts the consumer thread in the background.
   */
  public void start() {
    consumerThread =
      new Thread(this, "IndexCDCConsumer-" + dataTableName + "-" + encodedRegionName);
    consumerThread.setDaemon(true);
    consumerThread.start();
  }

  /**
   * Stops the consumer thread gracefully.
   */
  public void stop() {
    stopped = true;
    if (consumerThread != null) {
      consumerThread.interrupt();
    }
    if (indexWriter != null) {
      indexWriter.stop("IndexCDCConsumer stopped for " + dataTableName);
    }
    LOG.info("Stopped IndexCDCConsumer for table {} region {}", dataTableName, encodedRegionName);
  }

  /**
   * Sleeps for the specified duration if the consumer has not been stopped.
   * @param millis the duration to sleep in milliseconds.
   * @throws InterruptedException if the thread is interrupted while sleeping.
   */
  private void sleepIfNotStopped(long millis) throws InterruptedException {
    if (!stopped) {
      Thread.sleep(millis);
    }
  }

  private PTable getDataTable(PhoenixConnection conn) throws SQLException {
    PTable dataTable = cachedDataTable;
    if (dataTable == null) {
      dataTable = conn.getTable(dataTableName);
      cachedDataTable = dataTable;
    }
    return dataTable;
  }

  private void refreshDataTableCache(PhoenixConnection conn) throws SQLException {
    cachedDataTable = conn.getTable(dataTableName);
  }

  @Override
  public void run() {
    try {
      if (startupDelayMs > 0 && getCDCStreamNumPartitions() <= 1) {
        sleepIfNotStopped(startupDelayMs);
      }
      if (stopped) {
        return;
      }
      if (!hasEventuallyConsistentIndexes()) {
        LOG.trace("No eventually consistent indexes found for table {}. Exiting consumer.",
          dataTableName);
        return;
      }
      LOG.info("IndexCDCConsumer started for table {} region {}", dataTableName, encodedRegionName);
      if (!waitForCDCStreamEntry()) {
        LOG.error(
          "IndexCDCConsumer stopped while waiting for CDC_STREAM entry for table {} region {}",
          dataTableName, encodedRegionName);
        return;
      }
      long lastProcessedTimestamp = checkTrackerStatus(encodedRegionName, encodedRegionName);
      if (lastProcessedTimestamp == -1) {
        // should never happen as COMPLETE is only set by child regions for their parent regions
        LOG.error(
          "Unexpected COMPLETE status in IDX_CDC_TRACKER for current region. "
            + "Table: {}, Partition: {}. Exiting consumer thread.",
          dataTableName, encodedRegionName);
        return;
      } else if (lastProcessedTimestamp > 0) {
        LOG.info(
          "Found existing tracker entry for table {} region {} with lastTimestamp {}. "
            + "Resuming from last position (region movement scenario).",
          dataTableName, encodedRegionName, lastProcessedTimestamp);
      } else {
        if (hasParentPartitions) {
          replayAndCompleteParentRegions(encodedRegionName);
        } else {
          LOG.info("No parent partitions for table {} region {}, skipping parent replay",
            dataTableName, encodedRegionName);
        }
      }
      int retryCount = 0;
      while (!stopped) {
        try {
          long previousTimestamp = lastProcessedTimestamp;
          lastProcessedTimestamp =
            processCDCBatch(encodedRegionName, encodedRegionName, lastProcessedTimestamp);
          if (lastProcessedTimestamp == previousTimestamp) {
            sleepIfNotStopped(ConnectionUtils.getPauseTime(pause, ++retryCount));
          } else {
            retryCount = 0;
            sleepIfNotStopped(pollIntervalMs);
          }
        } catch (Exception e) {
          if (e instanceof InterruptedException) {
            throw (InterruptedException) e;
          }
          long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
          LOG.error(
            "Error processing CDC mutations for table {} region {}. "
              + "Retry #{}, sleeping {} ms before retrying...",
            dataTableName, encodedRegionName, retryCount, sleepTime, e);
          sleepIfNotStopped(sleepTime);
        }
      }
    } catch (InterruptedException e) {
      if (!stopped) {
        LOG.warn("IndexCDCConsumer interrupted unexpectedly for table {} region {}", dataTableName,
          encodedRegionName, e);
      }
      Thread.currentThread().interrupt();
    }
    LOG.info("IndexCDCConsumer exiting for table {} region {}", dataTableName, encodedRegionName);
  }

  private boolean hasEventuallyConsistentIndexes() throws InterruptedException {
    int retryCount = 0;
    while (!stopped) {
      try (PhoenixConnection conn =
        QueryUtil.getConnectionOnServer(config).unwrap(PhoenixConnection.class)) {
        refreshDataTableCache(conn);
        PTable dataTable = getDataTable(conn);
        String cdcObjectName = CDCUtil.getCDCObjectName(dataTable, false);
        if (cdcObjectName == null) {
          LOG.debug("No CDC index found for table {}. Exiting consumer.", dataTableName);
          return false;
        }
        for (PTable index : dataTable.getIndexes()) {
          IndexConsistency consistency = index.getIndexConsistency();
          if (consistency != null && consistency.isAsynchronous()) {
            LOG.debug("Found eventually consistent index {} for table {}",
              index.getName().getString(), dataTableName);
            return true;
          }
        }
        return false;
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error checking for eventually consistent indexes for table {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          dataTableName, retryCount, sleepTime, e);
        sleepIfNotStopped(sleepTime);
      }
    }
    return false;
  }

  /**
   * Retrieves the count of partitions for the given table.
   * @return the count of CDC_STREAM rows for this table.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  private long getCDCStreamNumPartitions() throws InterruptedException {
    int retryCount = 0;
    String query = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME
      + " WHERE TABLE_NAME = ?";
    while (!stopped) {
      try (
        PhoenixConnection conn =
          QueryUtil.getConnectionOnServer(config).unwrap(PhoenixConnection.class);
        PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, dataTableName);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return rs.getLong(1);
          }
        }
        return 0;
      } catch (SQLException e) {
        if (e instanceof TableNotFoundException) {
          TableNotFoundException tnfe = (TableNotFoundException) e;
          // 5.3.0+ server with old metadata tables (i.e. EXECUTE_UPGRADE is not yet run)
          if (PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_TABLE.equals(tnfe.getTableName())) {
            stopped = true;
            return -1;
          }
        }
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error getting CDC_STREAM row count for table {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          dataTableName, retryCount, sleepTime, e);
        sleepIfNotStopped(sleepTime);
      }
    }
    return -1;
  }

  /**
   * Waits for the CDC_STREAM entry for the given table and partition to be available.
   * @return true if the entry was found, false if the consumer was stopped before finding the
   *         entry.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  private boolean waitForCDCStreamEntry() throws InterruptedException {
    int retryCount = 0;
    String query =
      "SELECT PARENT_PARTITION_ID FROM " + PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME
        + " WHERE TABLE_NAME = ? AND PARTITION_ID = ? LIMIT 1";
    while (!stopped) {
      try (
        PhoenixConnection conn =
          QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
        PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, dataTableName);
        ps.setString(2, encodedRegionName);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            String parentPartitionId = rs.getString(1);
            hasParentPartitions = parentPartitionId != null;
            LOG.debug("Found CDC_STREAM entry for table {} partition {}, hasParentPartitions={}",
              dataTableName, encodedRegionName, hasParentPartitions);
            return true;
          }
        }
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.info(
          "CDC_STREAM entry not found for table {} partition {}. "
            + "Attempt #{}, sleeping {} ms before retrying...",
          dataTableName, encodedRegionName, retryCount, sleepTime);
        sleepIfNotStopped(sleepTime);
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error checking CDC_STREAM for table {} partition {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          dataTableName, encodedRegionName, retryCount, sleepTime, e);
        sleepIfNotStopped(sleepTime);
      }
    }
    return false;
  }

  /**
   * Checks for an existing entry for the given partition and owner.
   * @param partitionId      the partition ID to check.
   * @param ownerPartitionId the owner partition ID.
   * @return the last processed timestamp if an entry exists with status IN_PROGRESS, -1 if the
   *         entry exists with status COMPLETE, or 0 if no entry exists.
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  private long checkTrackerStatus(String partitionId, String ownerPartitionId)
    throws InterruptedException {
    int retryCount = 0;
    String query =
      "SELECT LAST_TIMESTAMP, STATUS FROM " + PhoenixDatabaseMetaData.SYSTEM_IDX_CDC_TRACKER_NAME
        + " WHERE TABLE_NAME = ? AND PARTITION_ID = ? AND OWNER_PARTITION_ID = ?";
    while (!stopped) {
      try (
        PhoenixConnection conn =
          QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
        PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, dataTableName);
        ps.setString(2, partitionId);
        ps.setString(3, ownerPartitionId);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            long lastTimestamp = rs.getLong(1);
            String status = rs.getString(2);
            LOG.debug(
              "Found IDX_CDC_TRACKER entry for table {} partition {} owner {} "
                + "with status={}, lastTimestamp={}",
              dataTableName, partitionId, ownerPartitionId, status, lastTimestamp);
            if (PhoenixDatabaseMetaData.TRACKER_STATUS_IN_PROGRESS.equals(status)) {
              return lastTimestamp;
            } else if (PhoenixDatabaseMetaData.TRACKER_STATUS_COMPLETE.equals(status)) {
              return -1;
            } else {
              // Unknown status - should not happen
              throw new IllegalStateException(
                String.format("Unknown tracker status '%s' for table %s partition %s owner %s.",
                  status, dataTableName, partitionId, ownerPartitionId));
            }
          } else {
            LOG.debug("No IDX_CDC_TRACKER entry found for table {} partition {} owner {}",
              dataTableName, partitionId, ownerPartitionId);
            return 0;
          }
        }
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error checking IDX_CDC_TRACKER for table {} partition {} owner {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          dataTableName, partitionId, ownerPartitionId, retryCount, sleepTime, e);
        sleepIfNotStopped(sleepTime);
      }
    }
    return 0;
  }

  /**
   * Checks if any consumer has already completed processing the given partition.
   * @param partitionId the partition ID to check
   * @return true if the partition is done processing.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  private boolean isPartitionCompleted(String partitionId) throws InterruptedException {
    int retryCount = 0;
    String query = "SELECT * FROM " + PhoenixDatabaseMetaData.SYSTEM_IDX_CDC_TRACKER_NAME
      + " WHERE TABLE_NAME = ? AND PARTITION_ID = ? AND STATUS = ?";
    while (!stopped) {
      try (
        PhoenixConnection conn =
          QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
        PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, dataTableName);
        ps.setString(2, partitionId);
        ps.setString(3, PhoenixDatabaseMetaData.TRACKER_STATUS_COMPLETE);
        try (ResultSet rs = ps.executeQuery()) {
          return rs.next();
        }
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error checking if partition {} is completed for table {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          partitionId, dataTableName, retryCount, sleepTime, e);
        sleepIfNotStopped(sleepTime);
      }
    }
    return false;
  }

  /**
   * Gets the maximum last processed timestamp from any consumer's tracker record for the given
   * partition. This is used when a child region needs to continue processing from where any
   * previous consumer left off.
   * @param partitionId the partition ID to get progress for.
   * @return the max last processed timestamp across all owners, or 0 if not found.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  private long getParentProgress(String partitionId) throws InterruptedException {
    int retryCount = 0;
    String query =
      "SELECT MAX(LAST_TIMESTAMP) FROM " + PhoenixDatabaseMetaData.SYSTEM_IDX_CDC_TRACKER_NAME
        + " WHERE TABLE_NAME = ? AND PARTITION_ID = ? AND STATUS = ?";
    while (!stopped) {
      try (
        PhoenixConnection conn =
          QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
        PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, dataTableName);
        ps.setString(2, partitionId);
        ps.setString(3, PhoenixDatabaseMetaData.TRACKER_STATUS_IN_PROGRESS);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            long maxTimestamp = rs.getLong(1);
            if (maxTimestamp > 0) {
              LOG.debug("Found max progress {} for partition {} from previous consumers",
                maxTimestamp, partitionId);
              return maxTimestamp;
            }
          }
          return 0;
        }
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error getting parent progress for partition {} table {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          partitionId, dataTableName, retryCount, sleepTime, e);
        sleepIfNotStopped(sleepTime);
      }
    }
    throw new InterruptedException("IndexCDCConsumer stopped while getting parent progress.");
  }

  /**
   * Retrieves all parent partition IDs for the given partition.
   * @param partitionId the partition ID to find parents for.
   * @return list of parent partition IDs.
   * @throws InterruptedException if the thread is interrupted.
   */
  private List<String> getParentPartitionIds(String partitionId) throws InterruptedException {
    int retryCount = 0;
    String query =
      "SELECT PARENT_PARTITION_ID FROM " + PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME
        + " WHERE TABLE_NAME = ? AND PARTITION_ID = ? AND PARENT_PARTITION_ID IS NOT NULL";
    while (!stopped) {
      try (
        PhoenixConnection conn =
          QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
        PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, dataTableName);
        ps.setString(2, partitionId);
        List<String> parentIds = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            parentIds.add(rs.getString(1));
          }
        }
        LOG.debug("Found {} parent partition(s) for table {} partition {}: {}", parentIds.size(),
          dataTableName, partitionId, parentIds);
        return parentIds;
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error querying parent partitions from CDC_STREAM for table {} partition {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          dataTableName, partitionId, retryCount, sleepTime, e);
        sleepIfNotStopped(sleepTime);
      }
    }
    return Collections.emptyList();
  }

  /**
   * Processes all remaining CDC mutations for the given partition until completion, then marks the
   * partition as COMPLETE. Used to replay ancestor partitions (regions) after they are split or
   * merged into new regions.
   * @param partitionId            the partition ID to process.
   * @param ownerPartitionId       the owner partition ID.
   * @param lastProcessedTimestamp the timestamp to start processing from.
   * @throws InterruptedException if the thread is interrupted.
   */
  private void processPartitionToCompletion(String partitionId, String ownerPartitionId,
    long lastProcessedTimestamp) throws InterruptedException {
    LOG.debug(
      "Processing partition {} owner {} to completion for table {}, starting from timestamp {}",
      partitionId, ownerPartitionId, dataTableName, lastProcessedTimestamp);
    long currentLastProcessedTimestamp = lastProcessedTimestamp;
    int retryCount = 0;
    int batchCount = 0;
    while (!stopped) {
      try {
        if (batchCount > 0 && batchCount % 5 == 0) {
          if (isPartitionCompleted(partitionId)) {
            return;
          }
          long otherProgress = getParentProgress(partitionId);
          if (otherProgress > currentLastProcessedTimestamp) {
            // other owner has already made some progress, pause for a while before resuming
            sleepIfNotStopped(10000);
            if (isPartitionCompleted(partitionId)) {
              return;
            }
            currentLastProcessedTimestamp = getParentProgress(partitionId);
          }
        }
        long newTimestamp =
          processCDCBatch(partitionId, ownerPartitionId, currentLastProcessedTimestamp);
        batchCount++;
        retryCount = 0;
        if (newTimestamp == currentLastProcessedTimestamp) {
          if (isPartitionCompleted(partitionId)) {
            LOG.info(
              "Partition {} for table {} was completed by another consumer before {} could mark it",
              partitionId, dataTableName, ownerPartitionId);
            return;
          }
          LOG.info("Partition {} owner {} for table {} fully processed, marking as COMPLETE",
            partitionId, ownerPartitionId, dataTableName);
          try (PhoenixConnection conn = QueryUtil.getConnectionOnServer(env.getConfiguration())
            .unwrap(PhoenixConnection.class)) {
            updateTrackerProgress(conn, partitionId, ownerPartitionId,
              currentLastProcessedTimestamp, PhoenixDatabaseMetaData.TRACKER_STATUS_COMPLETE);
          }
          return;
        }
        currentLastProcessedTimestamp = newTimestamp;
      } catch (SQLException | IOException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error processing CDC batch for partition {} owner {} table {} "
            + "lastProcessedTimestamp {}. Retry #{}, sleeping {} ms",
          partitionId, ownerPartitionId, dataTableName, currentLastProcessedTimestamp, retryCount,
          sleepTime, e);
        sleepIfNotStopped(sleepTime);
      }
    }
    LOG.info("Processing partition {} (owner {}) stopped before completion for table {}",
      partitionId, ownerPartitionId, dataTableName);
  }

  /**
   * Recursive helper method to replay and complete parent regions. Replays all remaining mutations
   * from parent regions and marks them as COMPLETE. This is called during initialization when a
   * region has parent regions (split/merge scenarios).
   * @param partitionId the partition (region) ID to find and process parents for.
   */
  private void replayAndCompleteParentRegions(String partitionId) throws InterruptedException {
    List<String> parentIds = getParentPartitionIds(partitionId);
    if (parentIds.isEmpty()) {
      LOG.debug("No parent partitions found for table {} partition {}", dataTableName, partitionId);
      return;
    }
    for (String parentId : parentIds) {
      if (stopped) {
        return;
      }
      if (isPartitionCompleted(parentId)) {
        LOG.debug("Parent partition {} for table {} already has a COMPLETE record, skipping",
          parentId, dataTableName);
        continue;
      }
      replayAndCompleteParentRegions(parentId);
      if (isPartitionCompleted(parentId)) {
        LOG.debug(
          "Parent partition {} for table {} was completed by sibling during ancestor processing, "
            + "skipping",
          parentId, dataTableName);
        continue;
      }
      long parentProgress = getParentProgress(parentId);
      LOG.debug("Processing/Resuming parent partition {} for table {} from timestamp {} owner: {}",
        parentId, dataTableName, parentProgress, encodedRegionName);
      processPartitionToCompletion(parentId, encodedRegionName, parentProgress);
    }
  }

  /**
   * Processes a batch of CDC mutations for the given partition starting from the specified
   * timestamp. This method reads mutations from the CDC index, applies them to the respective
   * eventually consistent index tables, and updates the progress in SYSTEM.IDX_CDC_TRACKER.
   * @param partitionId            the partition (region) ID to process CDC mutations for.
   * @param ownerPartitionId       the owner partition ID.
   * @param lastProcessedTimestamp the timestamp to start reading CDC mutations from.
   * @return the new last processed timestamp after this batch, or the same timestamp if no new
   *         records were found.
   * @throws SQLException         if SQL error occurs.
   * @throws IOException          if an I/O error occurs.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  private long processCDCBatch(String partitionId, String ownerPartitionId,
    long lastProcessedTimestamp) throws SQLException, IOException, InterruptedException {
    LOG.debug("Processing CDC batch for table {} partition {} owner {} from timestamp {}",
      dataTableName, partitionId, ownerPartitionId, lastProcessedTimestamp);
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(config).unwrap(PhoenixConnection.class)) {
      PTable dataTable = getDataTable(conn);
      String cdcObjectName = CDCUtil.getCDCObjectName(dataTable, false);
      if (cdcObjectName == null) {
        throw new SQLException("No CDC object found for table " + dataTableName);
      }
      String schemaName = dataTable.getSchemaName().getString();
      if (schemaName == null || schemaName.isEmpty()) {
        cdcObjectName = "\"" + cdcObjectName + "\"";
      } else {
        cdcObjectName =
          "\"" + schemaName + "\"" + QueryConstants.NAME_SEPARATOR + "\"" + cdcObjectName + "\"";
      }
      String cdcQuery = String
        .format("SELECT /*+ CDC_INCLUDE(IDX_MUTATIONS) */ PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" "
          + "FROM %s WHERE PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() > ? "
          + "AND PHOENIX_ROW_TIMESTAMP() < ? "
          + "ORDER BY PARTITION_ID() ASC, PHOENIX_ROW_TIMESTAMP() ASC LIMIT ?", cdcObjectName);
      List<Pair<Long, IndexMutationsProtos.IndexMutations>> batchMutations = new ArrayList<>();
      long newLastTimestamp = lastProcessedTimestamp;
      boolean hasMoreRows = true;
      int retryCount = 0;
      while (hasMoreRows && batchMutations.isEmpty()) {
        long currentTime = EnvironmentEdgeManager.currentTimeMillis() - timestampBufferMs;
        try (PreparedStatement ps = conn.prepareStatement(cdcQuery)) {
          ps.setString(1, partitionId);
          ps.setDate(2, new Date(newLastTimestamp));
          ps.setDate(3, new Date(currentTime));
          ps.setInt(4, batchSize);
          Pair<Long, Boolean> result =
            getMutationsAndTimestamp(ps, newLastTimestamp, batchMutations);
          hasMoreRows = result.getSecond();
          if (hasMoreRows) {
            newLastTimestamp = result.getFirst();
            if (batchMutations.isEmpty()) {
              sleepIfNotStopped(ConnectionUtils.getPauseTime(pause, ++retryCount));
            }
          }
        }
      }
      // With predefined LIMIT, there might be more rows with the same timestamp that were not
      // included in this batch.
      if (newLastTimestamp > lastProcessedTimestamp) {
        String sameTimestampQuery = String
          .format("SELECT /*+ CDC_INCLUDE(IDX_MUTATIONS) */ PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" "
            + "FROM %s WHERE PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() = ? "
            + "ORDER BY PARTITION_ID() ASC, PHOENIX_ROW_TIMESTAMP() ASC", cdcObjectName);
        final long timestampToRefetch = newLastTimestamp;
        batchMutations.removeIf(pair -> pair.getFirst() == timestampToRefetch);
        try (PreparedStatement ps = conn.prepareStatement(sameTimestampQuery)) {
          ps.setString(1, partitionId);
          ps.setDate(2, new Date(newLastTimestamp));
          Pair<Long, Boolean> result =
            getMutationsAndTimestamp(ps, newLastTimestamp, batchMutations);
          newLastTimestamp = result.getFirst();
          if (newLastTimestamp != timestampToRefetch) {
            throw new IOException("Unexpected timestamp mismatch: expected " + timestampToRefetch
              + " but got " + newLastTimestamp);
          }
        }
      }
      executeIndexMutations(partitionId, batchMutations, ownerPartitionId, newLastTimestamp);
      if (!batchMutations.isEmpty()) {
        updateTrackerProgress(conn, partitionId, ownerPartitionId, newLastTimestamp,
          PhoenixDatabaseMetaData.TRACKER_STATUS_IN_PROGRESS);
      }
      return newLastTimestamp;
    }
  }

  private void executeIndexMutations(String partitionId,
    List<Pair<Long, IndexMutationsProtos.IndexMutations>> batchMutations, String ownerPartitionId,
    long lastProcessedTimestamp) throws SQLException, IOException {
    if (!batchMutations.isEmpty()) {
      ListMultimap<HTableInterfaceReference, Mutation> indexUpdates = ArrayListMultimap.create();
      Map<ImmutableBytesPtr, HTableInterfaceReference> tableRefCache = new HashMap<>();
      int totalMutations = 0;
      for (Pair<Long, IndexMutationsProtos.IndexMutations> batchMutation : batchMutations) {
        IndexMutationsProtos.IndexMutations mutationsProto = batchMutation.getSecond();
        List<ByteString> tables = mutationsProto.getTablesList();
        List<ByteString> mutations = mutationsProto.getMutationsList();
        if (tables.size() != mutations.size()) {
          throw new SQLException("Tables and mutations sizes do not match. Tables size: "
            + tables.size() + ", mutations size: " + mutations.size());
        }
        for (int i = 0; i < tables.size(); i++) {
          byte[] indexTableName = tables.get(i).toByteArray();
          byte[] mutationBytes = mutations.get(i).toByteArray();
          ImmutableBytesPtr tableNamePtr = new ImmutableBytesPtr(indexTableName);
          HTableInterfaceReference tableRef =
            tableRefCache.computeIfAbsent(tableNamePtr, HTableInterfaceReference::new);
          ClientProtos.MutationProto mProto = ClientProtos.MutationProto.parseFrom(mutationBytes);
          Mutation mutation = ProtobufUtil.toMutation(mProto);
          indexUpdates.put(tableRef, mutation);
        }
        if (indexUpdates.size() >= batchSize) {
          indexWriter.write(indexUpdates, false, MetaDataProtocol.PHOENIX_VERSION);
          totalMutations += indexUpdates.size();
          indexUpdates.clear();
        }
      }
      if (!indexUpdates.isEmpty()) {
        indexWriter.write(indexUpdates, false, MetaDataProtocol.PHOENIX_VERSION);
        totalMutations += indexUpdates.size();
      }
      if (totalMutations > 0) {
        LOG.debug(
          "Applied total {} index mutations for table {} partition {} owner {} "
            + ", last processed timestamp {}",
          totalMutations, dataTableName, partitionId, ownerPartitionId, lastProcessedTimestamp);
      }
    }
  }

  private void updateTrackerProgress(PhoenixConnection conn, String partitionId,
    String ownerPartitionId, long lastTimestamp, String status) throws SQLException {
    String upsertSql = "UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_IDX_CDC_TRACKER_NAME
      + " (TABLE_NAME, PARTITION_ID, OWNER_PARTITION_ID, LAST_TIMESTAMP, STATUS) "
      + "VALUES (?, ?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
      ps.setString(1, dataTableName);
      ps.setString(2, partitionId);
      ps.setString(3, ownerPartitionId);
      ps.setLong(4, lastTimestamp);
      ps.setString(5, status);
      ps.executeUpdate();
      conn.commit();
      LOG.debug("Updated tracker for table {} partition {} owner {} to timestamp {} with status {}",
        dataTableName, partitionId, ownerPartitionId, lastTimestamp, status);
    }
  }

  /**
   * Executes the prepared statement and extracts mutations and timestamps from the result set.
   * @param ps                   the prepared statement to execute.
   * @param initialLastTimestamp the initial last timestamp to use if no rows are found.
   * @param batchMutations       list to add mutations to.
   * @return Pair of last processed timestamp and boolean to indicate if any rows were returned.
   */
  private static Pair<Long, Boolean> getMutationsAndTimestamp(PreparedStatement ps,
    long initialLastTimestamp, List<Pair<Long, IndexMutationsProtos.IndexMutations>> batchMutations)
    throws SQLException, IOException {
    boolean hasRows = false;
    long lastTimestamp = initialLastTimestamp;
    try (ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        hasRows = true;
        lastTimestamp = rs.getDate(1).getTime();
        String cdcValue = rs.getString(2);
        if (cdcValue != null && !cdcValue.isEmpty()) {
          byte[] protoBytes = Base64.getDecoder().decode(cdcValue);
          IndexMutationsProtos.IndexMutations mutationsProto =
            IndexMutationsProtos.IndexMutations.parseFrom(protoBytes);
          if (mutationsProto.getTablesCount() > 0 && mutationsProto.getMutationsCount() > 0) {
            batchMutations.add(Pair.newPair(lastTimestamp, mutationsProto));
          }
        }
      }
    }
    return Pair.newPair(lastTimestamp, hasRows);
  }

}
