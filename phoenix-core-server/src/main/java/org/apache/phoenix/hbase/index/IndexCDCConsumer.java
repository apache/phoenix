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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.DelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.generated.IndexMutationsProtos;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexCDCConsumerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.IndexConsistency;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
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
  private static final int DEFAULT_CDC_BATCH_SIZE = 500;

  public static final String INDEX_CDC_CONSUMER_STARTUP_DELAY_MS =
    "phoenix.index.cdc.consumer.startup.delay.ms";
  private static final long DEFAULT_STARTUP_DELAY_MS = 10000;

  /**
   * The interval in milliseconds between processing batches when mutations are found.
   */
  public static final String INDEX_CDC_CONSUMER_POLL_INTERVAL_MS =
    "phoenix.index.cdc.consumer.poll.interval.ms";
  private static final long DEFAULT_POLL_INTERVAL_MS = 500;

  /**
   * The interval in milliseconds between polls when the previous poll found no new events.
   */
  public static final String INDEX_CDC_CONSUMER_IDLE_POLL_INTERVAL_MS =
    "phoenix.index.cdc.consumer.idle.poll.interval.ms";
  private static final long DEFAULT_IDLE_POLL_INTERVAL_MS = 4000;

  /**
   * The time buffer in milliseconds subtracted from current time when querying CDC mutations to
   * help avoid reading mutations that are too recent.
   */
  private static final String INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS =
    "phoenix.index.cdc.consumer.timestamp.buffer.ms";
  private static final long DEFAULT_TIMESTAMP_BUFFER_MS = 1000;

  /**
   * Maximum number of retries when CDC events exist but the corresponding data table mutations are
   * not yet visible (or permanently failed). After exceeding this limit, the consumer advances past
   * the unprocessable events to avoid blocking indefinitely. This is only used for index mutation
   * generation approach (serializeCDCMutations = false).
   */
  public static final String INDEX_CDC_CONSUMER_MAX_DATA_VISIBILITY_RETRIES =
    "phoenix.index.cdc.consumer.max.data.visibility.retries";
  private static final int DEFAULT_MAX_DATA_VISIBILITY_RETRIES = 15;

  public static final String INDEX_CDC_CONSUMER_RETRY_PAUSE_MS =
    "phoenix.index.cdc.consumer.retry.pause.ms";
  private static final long DEFAULT_RETRY_PAUSE_MS = 200;

  public static final String INDEX_CDC_CONSUMER_PARENT_PROGRESS_PAUSE_MS =
    "phoenix.index.cdc.consumer.parent.progress.pause.ms";
  private static final long DEFAULT_PARENT_PROGRESS_PAUSE_MS = 15000;

  /**
   * Interval between {@code cdcIndexUpdateLag} samples emitted while the consumer is sleeping (idle
   * poll, backoff, parent-progress wait, etc.). Clamped to at least 50ms.
   */
  public static final String INDEX_CDC_CONSUMER_LAG_SAMPLE_INTERVAL_MS =
    "phoenix.index.cdc.consumer.lag.sample.interval.ms";
  private static final long DEFAULT_LAG_SAMPLE_INTERVAL_MS = 5000L;
  private static final long MIN_LAG_SAMPLE_INTERVAL_MS = 50L;

  /**
   * Minimum time between two reads of the data table definition from SYSTEM.CATALOG. The
   * UPDATE_CACHE_FREQUENCY of the data table sets this time within the minimum and the maximum. The
   * minimum applies when that value is smaller, for example ALWAYS. It limits the load that busy
   * regions put on SYSTEM.CATALOG.
   */
  private static final long MIN_METADATA_REFRESH_INTERVAL_MS = 60000L;

  /**
   * Maximum time between two reads of the data table definition from SYSTEM.CATALOG. The maximum
   * applies when the UPDATE_CACHE_FREQUENCY of the data table is larger, for example NEVER. It
   * limits the time until the consumer sees a change of index consistency. NEVER together with
   * disabled LAST_DDL_TIMESTAMP validation is not supported, because clients then never see the
   * change.
   */
  private static final long MAX_METADATA_REFRESH_INTERVAL_MS = 900000L;

  /**
   * Extra time after a switch of an index from EVENTUAL to STRONG during which the consumer still
   * applies CDC events to the index. It covers writes in flight at the switch, and clock skew
   * between the DDL client and the region servers.
   */
  private static final long CONSISTENCY_SWITCH_GRACE_MS = 60000L;

  /**
   * Minimum time between two starts of a consumer for the same region. The writers start a new
   * consumer when the old consumer has exited. This limit stops a consumer that exits at once from
   * starting again on each batch.
   */
  private static final long MIN_RESTART_INTERVAL_MS = 60000L;

  /**
   * The last startup read of each data table definition on this region server. The consumers of a
   * table share it. Many regions of a table can open at the same time, for example when the server
   * starts. The shared read then makes one SYSTEM.CATALOG read for the table, not one for each
   * region. The time limit removes old entries. A removed entry causes only one more read.
   */
  private static final Cache<String, SharedTableRead> STARTUP_TABLE_READS = CacheBuilder
    .newBuilder().expireAfterWrite(MIN_METADATA_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS).build();

  /** A data table definition and the local time just before its read from SYSTEM.CATALOG. */
  private static final class SharedTableRead {
    private PTable table;
    private long readTime = Long.MIN_VALUE;
  }

  private final RegionCoprocessorEnvironment env;
  private final String dataTableName;
  private final String encodedRegionName;
  private final String serverName;
  private final IndexWriter indexWriter;
  private final long pause;
  private final long startupDelayMs;
  private final int batchSize;
  private final long pollIntervalMs;
  private final long idlePollIntervalMs;
  private final long timestampBufferMs;
  private final int maxDataVisibilityRetries;
  private final long parentProgressPauseMs;
  private final Configuration config;
  private final boolean serializeCDCMutations;
  private final MetricsIndexCDCConsumerSource metricSource;
  private final long lagSampleIntervalMs;
  private final IndexCDCConsumerProgress progress;
  // Local time when the region opened.
  private final long openTime;
  // True when an index needs the consumer. Until then, sleepWithLagSampling emits no lag samples,
  // so that tables without such an index do not add samples to the lag histograms.
  private volatile boolean lagEmissionEnabled = false;
  private volatile boolean stopped = false;
  // True when the consumer thread has ended.
  private volatile boolean exited = false;
  private Thread consumerThread;
  private boolean hasParentPartitions = false;
  private PTable cachedDataTable;
  // Local time just before the last read of cachedDataTable from SYSTEM.CATALOG.
  private long cachedDataTableReadTime = 0L;

  private boolean tenantInit = false;
  private boolean isMultiTenant = false;
  private String tenantIdColName;
  private PDataType<?> tenantIdDataType;
  private TenantScanInfo ownRegionScanInfo;

  private final Map<String, TenantScanInfo> ancestorScanInfoCache = new HashMap<>();

  private static class TenantScanInfo {

    private static final TenantScanInfo EMPTY = new TenantScanInfo("", "", null, null, null);

    private final String filter;
    private final String orderBy;
    private final Object startValue;
    private final Object endValue;
    private final PDataType<?> dataType;

    TenantScanInfo(String filter, String orderBy, Object startValue, Object endValue,
      PDataType<?> dataType) {
      this.filter = filter;
      this.orderBy = orderBy;
      this.startValue = startValue;
      this.endValue = endValue;
      this.dataType = dataType;
    }

    int bindParams(PreparedStatement ps, int startIndex) throws SQLException {
      int idx = startIndex;
      if (startValue != null) {
        ps.setObject(idx++, startValue, dataType.getSqlType());
      }
      if (endValue != null) {
        ps.setObject(idx++, endValue, dataType.getSqlType());
      }
      return idx;
    }
  }

  /**
   * Creates a new IndexCDCConsumer for the given region with configurable serialization mode.
   * @param env                   region coprocessor environment.
   * @param dataTableName         name of the data table.
   * @param serverName            server name.
   * @param serializeCDCMutations when true, consumes pre-serialized index mutations; when false,
   *                              generates index mutations from data row states.
   * @throws IOException if the IndexWriter cannot be created.
   */
  public IndexCDCConsumer(RegionCoprocessorEnvironment env, String dataTableName, String serverName,
    boolean serializeCDCMutations) throws IOException {
    this.env = env;
    this.dataTableName = dataTableName;
    this.encodedRegionName = env.getRegion().getRegionInfo().getEncodedName();
    this.serverName = serverName;
    this.config = env.getConfiguration();
    this.serializeCDCMutations = serializeCDCMutations;
    this.pause = config.getLong(INDEX_CDC_CONSUMER_RETRY_PAUSE_MS, DEFAULT_RETRY_PAUSE_MS);
    this.startupDelayMs =
      config.getLong(INDEX_CDC_CONSUMER_STARTUP_DELAY_MS, DEFAULT_STARTUP_DELAY_MS);
    int baseBatchSize = config.getInt(INDEX_CDC_CONSUMER_BATCH_SIZE, DEFAULT_CDC_BATCH_SIZE);
    int jitter = ThreadLocalRandom.current().nextInt(baseBatchSize / 5 + 1);
    this.batchSize = baseBatchSize + jitter;
    this.pollIntervalMs =
      config.getLong(INDEX_CDC_CONSUMER_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
    long baseIdlePollInterval =
      config.getLong(INDEX_CDC_CONSUMER_IDLE_POLL_INTERVAL_MS, DEFAULT_IDLE_POLL_INTERVAL_MS);
    this.idlePollIntervalMs =
      baseIdlePollInterval + ThreadLocalRandom.current().nextLong(baseIdlePollInterval / 5 + 1);
    this.timestampBufferMs =
      config.getLong(INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS, DEFAULT_TIMESTAMP_BUFFER_MS);
    this.maxDataVisibilityRetries = config.getInt(INDEX_CDC_CONSUMER_MAX_DATA_VISIBILITY_RETRIES,
      DEFAULT_MAX_DATA_VISIBILITY_RETRIES);
    this.parentProgressPauseMs =
      config.getLong(INDEX_CDC_CONSUMER_PARENT_PROGRESS_PAUSE_MS, DEFAULT_PARENT_PROGRESS_PAUSE_MS);
    this.lagSampleIntervalMs = Math.max(MIN_LAG_SAMPLE_INTERVAL_MS,
      config.getLong(INDEX_CDC_CONSUMER_LAG_SAMPLE_INTERVAL_MS, DEFAULT_LAG_SAMPLE_INTERVAL_MS));
    this.metricSource = MetricsIndexerSourceFactory.getInstance().getIndexCDCConsumerSource();
    this.openTime = EnvironmentEdgeManager.currentTimeMillis();
    this.progress = new IndexCDCConsumerProgress(this.openTime, this.timestampBufferMs);
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
   * Returns true when the consumer has exited and enough time has passed since it started. A new
   * consumer for the region can then start.
   */
  boolean canRestart() {
    return exited && !stopped
      && EnvironmentEdgeManager.currentTimeMillis() - openTime >= MIN_RESTART_INTERVAL_MS;
  }

  /** Creates a consumer for the same region. The caller starts it. */
  IndexCDCConsumer newInstance() throws IOException {
    return new IndexCDCConsumer(env, dataTableName, serverName, serializeCDCMutations);
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
  }

  /**
   * Sleeps for up to {@code totalMillis}, emitting a {@code cdcIndexUpdateLag} sample at the start
   * of each {@code lagSampleIntervalMs} slice once {@link #lagEmissionEnabled} is set. Aborts
   * immediately when stopped. Used for all consumer-thread sleeps so the lag metric stays
   * non-silent across idle, failure, post-startup, and parent-replay phases.
   */
  private void sleepWithLagSampling(long totalMillis) throws InterruptedException {
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + totalMillis;
    while (!stopped) {
      long now = EnvironmentEdgeManager.currentTimeMillis();
      if (lagEmissionEnabled) {
        metricSource.updateCdcLag(dataTableName, progress.currentLagMs(now));
      }
      long remaining = deadline - now;
      if (remaining <= 0) {
        return;
      }
      Thread.sleep(Math.min(remaining, lagSampleIntervalMs));
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

  /**
   * Reads the data table definition from SYSTEM.CATALOG. The metadata cache of the region server
   * can keep an old definition after an ALTER, for example when the region moves to this server.
   */
  private void refreshDataTableCache(PhoenixConnection conn) throws SQLException {
    long readTime = EnvironmentEdgeManager.currentTimeMillis();
    cachedDataTable = conn.getTableNoCache(conn.getTenantId(), dataTableName);
    cachedDataTableReadTime = readTime;
  }

  /**
   * Gets the data table definition at consumer startup. The consumer uses a shared read of another
   * consumer when that read started after this region opened. That read then shows each DDL that
   * committed before the region opened, as a read of this consumer does.
   */
  private void readDataTableAtStartup(PhoenixConnection conn) throws SQLException {
    SharedTableRead shared =
      STARTUP_TABLE_READS.asMap().computeIfAbsent(dataTableName, k -> new SharedTableRead());
    synchronized (shared) {
      if (shared.table != null && shared.readTime >= openTime) {
        cachedDataTable = shared.table;
        cachedDataTableReadTime = shared.readTime;
        return;
      }
      refreshDataTableCache(conn);
      shared.table = cachedDataTable;
      shared.readTime = cachedDataTableReadTime;
    }
  }

  /**
   * Reads the data table definition again when the batch has events newer than the last read and
   * the refresh interval has expired. The UPDATE_CACHE_FREQUENCY of the data table sets that
   * interval, within fixed limits. Idle regions thus do not read SYSTEM.CATALOG.
   */
  private void refreshDataTableCacheIfDue(PhoenixConnection conn,
    List<Pair<Long, IndexMutationsProtos.DataRowStates>> batchStates) throws SQLException {
    long maxEventTimestamp = Long.MIN_VALUE;
    for (Pair<Long, IndexMutationsProtos.DataRowStates> entry : batchStates) {
      maxEventTimestamp = Math.max(maxEventTimestamp, entry.getFirst());
    }
    if (maxEventTimestamp <= cachedDataTableReadTime) {
      return;
    }
    long refreshInterval = Math.max(MIN_METADATA_REFRESH_INTERVAL_MS,
      Math.min(getDataTable(conn).getUpdateCacheFrequency(), MAX_METADATA_REFRESH_INTERVAL_MS));
    if (EnvironmentEdgeManager.currentTimeMillis() - cachedDataTableReadTime >= refreshInterval) {
      refreshDataTableCache(conn);
    }
  }

  /**
   * Returns the timestamp of the last CDC event that the consumer applies to the index. An EVENTUAL
   * index gets all events. After a switch from EVENTUAL to STRONG, a client with an old cache can
   * send EVENTUAL maintainers for up to the UPDATE_CACHE_FREQUENCY of the data table. Indexes use
   * the frequency of their data table. Thus the consumer applies events up to the index
   * LAST_DDL_TIMESTAMP, plus that time, plus a grace period. Writers also maintain the index
   * synchronously in this period, and the replay writes the same cells again.
   * <p>
   * An UPDATE_CACHE_FREQUENCY of NEVER together with disabled LAST_DDL_TIMESTAMP validation is not
   * supported. In that configuration, a client never reads the switch, so no cutoff is correct. The
   * cutoff is then not limited, and the design does not try to handle this case.
   * @return the cutoff timestamp, or {@link Long#MIN_VALUE} when the consumer does not maintain the
   *         index.
   */
  static long getApplyCutoff(PTable dataTable, PTable index) {
    IndexConsistency consistency = index.getIndexConsistency();
    if (consistency != null && consistency.isAsynchronous()) {
      return Long.MAX_VALUE;
    }
    Long lastDDLTimestamp = index.getLastDDLTimestamp();
    if (
      lastDDLTimestamp == null || index.getIndexType() == PTable.IndexType.LOCAL
        || CDCUtil.isCDCIndex(index)
    ) {
      return Long.MIN_VALUE;
    }
    long cacheLag = dataTable.getUpdateCacheFrequency();
    long allowance = cacheLag > Long.MAX_VALUE - CONSISTENCY_SWITCH_GRACE_MS
      ? Long.MAX_VALUE
      : cacheLag + CONSISTENCY_SWITCH_GRACE_MS;
    return lastDDLTimestamp > Long.MAX_VALUE - allowance
      ? Long.MAX_VALUE
      : lastDDLTimestamp + allowance;
  }

  /**
   * Returns true when no index needs the CDC events of this region after the given timestamp. This
   * is true when no index is EVENTUAL and each cutoff is at or before the timestamp. The consumer
   * then exits. If an index changes to EVENTUAL later, the writers see the change and start a new
   * consumer for the region.
   */
  private boolean noIndexNeedsEventsAfter(long timestamp) {
    PTable dataTable = cachedDataTable;
    for (PTable index : dataTable.getIndexes()) {
      if (getApplyCutoff(dataTable, index) > timestamp) {
        return false;
      }
    }
    return true;
  }

  private void initTenantInfo(PhoenixConnection conn) throws SQLException {
    if (tenantInit) {
      return;
    }
    PTable dataTable = getDataTable(conn);
    isMultiTenant = dataTable.isMultiTenant();
    if (!isMultiTenant) {
      ownRegionScanInfo = TenantScanInfo.EMPTY;
      tenantInit = true;
      return;
    }
    int tenantColIndex = dataTable.getBucketNum() != null ? 1 : 0;
    PColumn tenantCol = dataTable.getPKColumns().get(tenantColIndex);
    tenantIdColName = tenantCol.getName().getString();
    tenantIdDataType = tenantCol.getDataType();

    byte[] regionStartKey = env.getRegion().getRegionInfo().getStartKey();
    byte[] regionEndKey = env.getRegion().getRegionInfo().getEndKey();
    ownRegionScanInfo = buildTenantScanInfo(regionStartKey, regionEndKey, dataTable);
    LOG.debug(
      "Initialized multi-tenant scan for table {} region {}:"
        + " tenantCol {}, startTenant {}, endTenant {}",
      dataTableName, encodedRegionName, tenantIdColName, ownRegionScanInfo.startValue,
      ownRegionScanInfo.endValue);
    tenantInit = true;
  }

  private TenantScanInfo buildTenantScanInfo(byte[] startKey, byte[] endKey, PTable dataTable) {
    Object startVal = extractTenantIdFromRegionKey(startKey, dataTable);
    Object endVal = extractTenantIdFromRegionKey(endKey, dataTable);
    StringBuilder sb = new StringBuilder();
    if (startVal != null) {
      sb.append("\"").append(tenantIdColName).append("\" >= ? AND ");
    }
    if (endVal != null) {
      sb.append("\"").append(tenantIdColName).append("\" <= ? AND ");
    }
    String filter = sb.toString();
    String orderBy = filter.isEmpty() ? "" : "\"" + tenantIdColName + "\" ASC,";
    return new TenantScanInfo(filter, orderBy, startVal, endVal, tenantIdDataType);
  }

  private Object extractTenantIdFromRegionKey(byte[] regionKey, PTable dataTable) {
    if (regionKey == null || regionKey.length == 0) {
      return null;
    }
    final RowKeySchema schema = dataTable.getRowKeySchema();
    int pkPos = dataTable.getBucketNum() != null ? 1 : 0;
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    int maxOffset = schema.iterator(regionKey, 0, regionKey.length, ptr);
    for (int i = 0; i <= pkPos; i++) {
      Boolean hasValue = schema.next(ptr, i, maxOffset);
      if (!Boolean.TRUE.equals(hasValue)) {
        return null;
      }
    }
    byte[] tenantBytes = ByteUtil.copyKeyBytesIfNecessary(ptr);
    PColumn tenantCol = dataTable.getPKColumns().get(pkPos);
    return tenantCol.getDataType().toObject(tenantBytes, 0, tenantBytes.length,
      tenantCol.getDataType(), tenantCol.getSortOrder(), tenantCol.getMaxLength(),
      tenantCol.getScale());
  }

  private byte[][] lookupPartitionKeys(String partitionId) throws InterruptedException {
    int retryCount = 0;
    final String query = "SELECT PARTITION_START_KEY, PARTITION_END_KEY FROM "
      + PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME
      + " WHERE TABLE_NAME = ? AND PARTITION_ID = ? LIMIT 1";
    while (!stopped) {
      try (
        PhoenixConnection conn =
          QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
        PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, dataTableName);
        ps.setString(2, partitionId);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            byte[] startKey = rs.getBytes(1);
            byte[] endKey = rs.getBytes(2);
            return new byte[][] { startKey == null ? new byte[0] : startKey,
              endKey == null ? new byte[0] : endKey };
          }
        }
        LOG.error("No CDC_STREAM entry found for partition {} table {}. This should not happen.",
          partitionId, dataTableName);
        return new byte[][] { new byte[0], new byte[0] };
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error while retrieving partition keys from CDC_STREAM for partition {} table {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          partitionId, dataTableName, retryCount, sleepTime, e);
        sleepWithLagSampling(sleepTime);
      }
    }
    return null;
  }

  private TenantScanInfo getPartitionTenantScanInfo(String partitionId)
    throws InterruptedException {
    if (!isMultiTenant) {
      return TenantScanInfo.EMPTY;
    }
    if (partitionId.equals(encodedRegionName)) {
      return ownRegionScanInfo;
    }
    TenantScanInfo cached = ancestorScanInfoCache.get(partitionId);
    if (cached != null) {
      return cached;
    }
    byte[][] keys = lookupPartitionKeys(partitionId);
    if (keys == null) {
      return TenantScanInfo.EMPTY;
    }
    TenantScanInfo info = buildTenantScanInfo(keys[0], keys[1], cachedDataTable);
    ancestorScanInfoCache.put(partitionId, info);
    return info;
  }

  @Override
  public void run() {
    try {
      if (startupDelayMs > 0 && getCDCStreamNumPartitions() <= 1) {
        sleepWithLagSampling(startupDelayMs);
      }
      if (stopped) {
        return;
      }
      if (!hasCDCIndex()) {
        LOG.trace("No CDC index found for table {}. Exiting consumer.", dataTableName);
        return;
      }
      LOG.info(
        "IndexCDCConsumer started for table {} region {}"
          + " [batchSize: {}, pollIntervalMs: {}, idlePollIntervalMs: {}, timestampBufferMs: {},"
          + " startupDelayMs: {}, pause: {}, maxDataVisibilityRetries: {},"
          + " parentProgressPauseMs: {}, serializeCDCMutations: {}]",
        dataTableName, encodedRegionName, batchSize, pollIntervalMs, idlePollIntervalMs,
        timestampBufferMs, startupDelayMs, pause, maxDataVisibilityRetries, parentProgressPauseMs,
        serializeCDCMutations);
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
        if (noIndexNeedsEventsAfter(lastProcessedTimestamp)) {
          LOG.info("No index of table {} needs the CDC events of region {} after {}."
            + " Exiting consumer.", dataTableName, encodedRegionName, lastProcessedTimestamp);
          return;
        }
        progress.recordProcessed(lastProcessedTimestamp);
        LOG.info(
          "Found existing tracker entry for table {} region {} with lastTimestamp {}. "
            + "Resuming from last position (region movement scenario).",
          dataTableName, encodedRegionName, lastProcessedTimestamp);
      } else {
        if (hasParentPartitions) {
          metricSource.incrementCdcParentReplayActiveRegions(dataTableName);
          try {
            sleepWithLagSampling(timestampBufferMs + 1);
            replayAndCompleteParentRegions(encodedRegionName);
          } finally {
            metricSource.decrementCdcParentReplayActiveRegions(dataTableName);
          }
        } else {
          // A region without parents has no events older than the region.
          long regionCreateTime = env.getRegion().getRegionInfo().getRegionId();
          if (noIndexNeedsEventsAfter(regionCreateTime - 1)) {
            LOG.info("No index of table {} needs the CDC events of region {}. Exiting consumer.",
              dataTableName, encodedRegionName);
            return;
          }
          LOG.info("No parent partitions for table {} region {}, skipping parent replay",
            dataTableName, encodedRegionName);
        }
      }
      int retryCount = 0;
      metricSource.incrementCdcConsumerActiveRegions(dataTableName);
      try {
        while (!stopped) {
          try {
            long previousTimestamp = lastProcessedTimestamp;
            if (serializeCDCMutations) {
              lastProcessedTimestamp = processCDCBatch(encodedRegionName, encodedRegionName,
                lastProcessedTimestamp, false);
            } else {
              lastProcessedTimestamp = processCDCBatchGenerated(encodedRegionName,
                encodedRegionName, lastProcessedTimestamp, false);
            }
            retryCount = 0;
            // An empty poll also proves progress, so use the watermark of the region.
            long caughtUpTo = Math.max(lastProcessedTimestamp, progress.getEffectiveWatermark());
            if (noIndexNeedsEventsAfter(caughtUpTo)) {
              LOG.info("No index of table {} needs the CDC events of region {} after {}."
                + " Exiting consumer.", dataTableName, encodedRegionName, caughtUpTo);
              break;
            }
            if (lastProcessedTimestamp == previousTimestamp) {
              sleepWithLagSampling(idlePollIntervalMs);
            } else {
              sleepWithLagSampling(pollIntervalMs);
            }
          } catch (Exception e) {
            if (e instanceof InterruptedException) {
              throw (InterruptedException) e;
            }
            metricSource.incrementCdcBatchFailureCount(dataTableName);
            long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
            LOG.error(
              "Error processing CDC mutations for table {} region {}. "
                + "Retry #{}, sleeping {} ms before retrying...",
              dataTableName, encodedRegionName, retryCount, sleepTime, e);
            sleepWithLagSampling(sleepTime);
          }
        }
      } finally {
        metricSource.decrementCdcConsumerActiveRegions(dataTableName);
      }
    } catch (InterruptedException e) {
      if (!stopped) {
        LOG.warn("IndexCDCConsumer interrupted unexpectedly for table {} region {}", dataTableName,
          encodedRegionName, e);
      }
      Thread.currentThread().interrupt();
    } finally {
      exited = true;
    }
    LOG.info("IndexCDCConsumer exiting for table {} region {}", dataTableName, encodedRegionName);
  }

  /**
   * Returns true if the data table has an active CDC index. The consumer then continues also when
   * no index is EVENTUAL now. An index that changed from EVENTUAL to STRONG can still have CDC
   * events that no consumer applied, for example in the parent regions of this region. The consumer
   * exits later, when no index needs the events of the region. Lag sampling starts only when an
   * index needs the consumer, so that other tables do not add samples.
   */
  private boolean hasCDCIndex() throws InterruptedException {
    int retryCount = 0;
    while (!stopped) {
      try (PhoenixConnection conn =
        QueryUtil.getConnectionOnServer(config).unwrap(PhoenixConnection.class)) {
        readDataTableAtStartup(conn);
        PTable dataTable = getDataTable(conn);
        String cdcObjectName = CDCUtil.getCDCObjectName(dataTable, false);
        if (cdcObjectName == null) {
          LOG.debug("No CDC index found for table {}. Exiting consumer.", dataTableName);
          return false;
        }
        long now = EnvironmentEdgeManager.currentTimeMillis();
        for (PTable index : dataTable.getIndexes()) {
          if (getApplyCutoff(dataTable, index) >= now) {
            LOG.debug("Index {} of table {} needs the consumer", index.getName().getString(),
              dataTableName);
            lagEmissionEnabled = true;
            break;
          }
        }
        return true;
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error checking for eventually consistent indexes for table {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          dataTableName, retryCount, sleepTime, e);
        sleepWithLagSampling(sleepTime);
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
        sleepWithLagSampling(sleepTime);
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
        sleepWithLagSampling(sleepTime);
      } catch (SQLException e) {
        long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
        LOG.warn(
          "Error checking CDC_STREAM for table {} partition {}. "
            + "Retry #{}, sleeping {} ms before retrying...",
          dataTableName, encodedRegionName, retryCount, sleepTime, e);
        sleepWithLagSampling(sleepTime);
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
        sleepWithLagSampling(sleepTime);
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
        sleepWithLagSampling(sleepTime);
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
        sleepWithLagSampling(sleepTime);
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
        sleepWithLagSampling(sleepTime);
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
    long replayStartTime = EnvironmentEdgeManager.currentTimeMillis();
    boolean reachedTerminalState = false;
    try {
      while (!stopped) {
        try {
          if (batchCount > 0) {
            if (isPartitionCompleted(partitionId)) {
              reachedTerminalState = true;
              return;
            }
            long otherProgress = getParentProgress(partitionId);
            if (otherProgress > currentLastProcessedTimestamp) {
              long previousOtherProgress;
              do {
                previousOtherProgress = otherProgress;
                sleepWithLagSampling(parentProgressPauseMs);
                if (isPartitionCompleted(partitionId)) {
                  reachedTerminalState = true;
                  return;
                }
                otherProgress = getParentProgress(partitionId);
              } while (!stopped && otherProgress > previousOtherProgress);
              currentLastProcessedTimestamp = otherProgress;
            }
          }
          long newTimestamp;
          if (serializeCDCMutations) {
            newTimestamp =
              processCDCBatch(partitionId, ownerPartitionId, currentLastProcessedTimestamp, true);
          } else {
            newTimestamp = processCDCBatchGenerated(partitionId, ownerPartitionId,
              currentLastProcessedTimestamp, true);
          }
          batchCount++;
          retryCount = 0;
          if (newTimestamp == currentLastProcessedTimestamp) {
            if (isPartitionCompleted(partitionId)) {
              LOG.info(
                "Partition {} for table {} was completed by another consumer before {} could mark it",
                partitionId, dataTableName, ownerPartitionId);
              reachedTerminalState = true;
              return;
            }
            LOG.info("Partition {} owner {} for table {} fully processed, marking as COMPLETE",
              partitionId, ownerPartitionId, dataTableName);
            try (PhoenixConnection conn = QueryUtil.getConnectionOnServer(env.getConfiguration())
              .unwrap(PhoenixConnection.class)) {
              updateTrackerProgress(conn, partitionId, ownerPartitionId,
                currentLastProcessedTimestamp, PhoenixDatabaseMetaData.TRACKER_STATUS_COMPLETE);
            }
            reachedTerminalState = true;
            return;
          }
          currentLastProcessedTimestamp = newTimestamp;
        } catch (SQLException | IOException e) {
          metricSource.incrementCdcBatchFailureCount(dataTableName);
          long sleepTime = ConnectionUtils.getPauseTime(pause, ++retryCount);
          LOG.warn(
            "Error processing CDC batch for partition {} owner {} table {} "
              + "lastProcessedTimestamp {}. Retry #{}, sleeping {} ms",
            partitionId, ownerPartitionId, dataTableName, currentLastProcessedTimestamp, retryCount,
            sleepTime, e);
          sleepWithLagSampling(sleepTime);
        }
      }
      LOG.info("Processing partition {} (owner {}) stopped before completion for table {}",
        partitionId, ownerPartitionId, dataTableName);
    } finally {
      if (reachedTerminalState) {
        metricSource.updateCdcParentReplayDuration(dataTableName,
          EnvironmentEdgeManager.currentTimeMillis() - replayStartTime);
      }
    }
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
   * @param isParentReplay         true if replaying a closed parent partition.
   * @return the new last processed timestamp after this batch, or the same timestamp if no new
   *         records were found.
   * @throws SQLException         if SQL error occurs.
   * @throws IOException          if an I/O error occurs.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  private long processCDCBatch(String partitionId, String ownerPartitionId,
    long lastProcessedTimestamp, boolean isParentReplay)
    throws SQLException, IOException, InterruptedException {
    long batchStartTime = EnvironmentEdgeManager.currentTimeMillis();
    LOG.debug("Processing CDC batch for table {} partition {} owner {} from timestamp {}",
      dataTableName, partitionId, ownerPartitionId, lastProcessedTimestamp);
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(config).unwrap(PhoenixConnection.class)) {
      initTenantInfo(conn);
      String cdcObjectName = getCdcObjectName(conn);
      TenantScanInfo scanInfo = getPartitionTenantScanInfo(partitionId);
      String cdcQuery;
      if (isParentReplay) {
        cdcQuery = String.format(
          "SELECT /*+ CDC_INCLUDE(IDX_MUTATIONS) */ PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" "
            + "FROM %s WHERE %s PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() > ? "
            + "ORDER BY %s PARTITION_ID() ASC, PHOENIX_ROW_TIMESTAMP() ASC LIMIT ?",
          cdcObjectName, scanInfo.filter, scanInfo.orderBy);
      } else {
        cdcQuery = String.format(
          "SELECT /*+ CDC_INCLUDE(IDX_MUTATIONS) */ PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" "
            + "FROM %s WHERE %s PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() > ? "
            + "AND PHOENIX_ROW_TIMESTAMP() < ? "
            + "ORDER BY %s PARTITION_ID() ASC, PHOENIX_ROW_TIMESTAMP() ASC LIMIT ?",
          cdcObjectName, scanInfo.filter, scanInfo.orderBy);
      }
      List<Pair<Long, IndexMutationsProtos.IndexMutations>> batchMutations = new ArrayList<>();
      long newLastTimestamp = lastProcessedTimestamp;
      boolean hasMoreRows = true;
      int retryCount = 0;
      // Captured immediately before each query so the empty-poll watermark cannot over-advance
      // past what the query's own (now - timestampBufferMs) upper bound actually proved empty.
      long lastQueryStartTime = newLastTimestamp;
      while (hasMoreRows && batchMutations.isEmpty()) {
        lastQueryStartTime = EnvironmentEdgeManager.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(cdcQuery)) {
          setStatementParams(scanInfo, partitionId, isParentReplay, newLastTimestamp, ps);
          Pair<Long, Boolean> result =
            getMutationsAndTimestamp(ps, newLastTimestamp, batchMutations);
          hasMoreRows = result.getSecond();
          if (hasMoreRows) {
            newLastTimestamp = result.getFirst();
            if (batchMutations.isEmpty()) {
              if (!isParentReplay) {
                progress.recordProcessed(newLastTimestamp);
              }
              sleepWithLagSampling(ConnectionUtils.getPauseTime(pause, ++retryCount));
            }
          }
        }
      }
      // Empty own-partition poll proves we are caught up to (queryStart - timestampBufferMs).
      if (!hasMoreRows && !isParentReplay) {
        progress.recordEmptyPoll(lastQueryStartTime);
      }
      // With predefined LIMIT, there might be more rows with the same timestamp that were not
      // included in this batch.
      if (newLastTimestamp > lastProcessedTimestamp) {
        String sameTimestampQuery = String.format(
          "SELECT /*+ CDC_INCLUDE(IDX_MUTATIONS) */ PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" "
            + "FROM %s WHERE %s PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() = ? "
            + "ORDER BY %s PARTITION_ID() ASC, PHOENIX_ROW_TIMESTAMP() ASC",
          cdcObjectName, scanInfo.filter, scanInfo.orderBy);
        final long timestampToRefetch = newLastTimestamp;
        batchMutations.removeIf(pair -> pair.getFirst() == timestampToRefetch);
        try (PreparedStatement ps = conn.prepareStatement(sameTimestampQuery)) {
          int idx = scanInfo.bindParams(ps, 1);
          ps.setString(idx++, partitionId);
          ps.setDate(idx, new Date(newLastTimestamp));
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
        lagEmissionEnabled = true;
        metricSource.updateCdcBatchProcessTime(dataTableName,
          EnvironmentEdgeManager.currentTimeMillis() - batchStartTime);
        metricSource.incrementCdcBatchCount(dataTableName);
        if (!isParentReplay) {
          progress.recordProcessed(newLastTimestamp);
        }
        updateTrackerProgress(conn, partitionId, ownerPartitionId, newLastTimestamp,
          PhoenixDatabaseMetaData.TRACKER_STATUS_IN_PROGRESS);
      }
      return newLastTimestamp;
    }
  }

  private String getCdcObjectName(PhoenixConnection conn) throws SQLException {
    PTable dataTable = getDataTable(conn);
    String cdcObjectName = CDCUtil.getCDCObjectName(dataTable, false);
    if (cdcObjectName == null) {
      throw new SQLException("No CDC object found for table " + dataTableName);
    }
    return SchemaUtil.getEscapedTableName(dataTable.getSchemaName().getString(), cdcObjectName);
  }

  /**
   * Processes a batch of CDC events for the given partition starting from the specified timestamp
   * by generating index mutations from data row states. This method queries the CDC index with the
   * DATA_ROW_STATE scope, which triggers a server-side data table scan to reconstruct the
   * before-image ({@code currentDataRowState}) and after-image ({@code nextDataRowState}) for each
   * change.
   * @param partitionId            the partition (region) ID to process CDC events for.
   * @param ownerPartitionId       the owner partition ID.
   * @param lastProcessedTimestamp the timestamp to start processing CDC events from.
   * @param isParentReplay         true if replaying a closed parent partition.
   * @return the new last processed timestamp after this batch, or the same timestamp if no new
   *         records were found.
   * @throws SQLException         if a SQL error occurs.
   * @throws IOException          if an I/O error occurs.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  private long processCDCBatchGenerated(String partitionId, String ownerPartitionId,
    long lastProcessedTimestamp, boolean isParentReplay)
    throws SQLException, IOException, InterruptedException {
    long batchStartTime = EnvironmentEdgeManager.currentTimeMillis();
    LOG.debug(
      "Processing CDC batch (generated mode) for table {} partition {} owner {} from timestamp {}",
      dataTableName, partitionId, ownerPartitionId, lastProcessedTimestamp);
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(config).unwrap(PhoenixConnection.class)) {
      initTenantInfo(conn);
      String cdcObjectName = getCdcObjectName(conn);
      TenantScanInfo scanInfo = getPartitionTenantScanInfo(partitionId);
      String cdcQuery;
      if (isParentReplay) {
        cdcQuery = String.format(
          "SELECT /*+ CDC_INCLUDE(DATA_ROW_STATE) */ PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" "
            + "FROM %s WHERE %s PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() > ? "
            + "ORDER BY %s PARTITION_ID() ASC, PHOENIX_ROW_TIMESTAMP() ASC LIMIT ?",
          cdcObjectName, scanInfo.filter, scanInfo.orderBy);
      } else {
        cdcQuery = String.format(
          "SELECT /*+ CDC_INCLUDE(DATA_ROW_STATE) */ PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" "
            + "FROM %s WHERE %s PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() > ? "
            + "AND PHOENIX_ROW_TIMESTAMP() < ? "
            + "ORDER BY %s PARTITION_ID() ASC, PHOENIX_ROW_TIMESTAMP() ASC LIMIT ?",
          cdcObjectName, scanInfo.filter, scanInfo.orderBy);
      }

      List<Pair<Long, IndexMutationsProtos.DataRowStates>> batchStates = new ArrayList<>();
      long newLastTimestamp = lastProcessedTimestamp;
      long[] lastScannedTimestamp = { lastProcessedTimestamp };
      long[] scannedRowCount = { 0L };
      boolean hasMoreRows = true;
      int retryCount = 0;
      // Captured immediately before each query so the empty-poll watermark cannot over-advance
      // past what the query's own (now - timestampBufferMs) upper bound actually proved empty.
      long lastQueryStartTime = newLastTimestamp;
      while (hasMoreRows && batchStates.isEmpty()) {
        lastQueryStartTime = EnvironmentEdgeManager.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(cdcQuery)) {
          setStatementParams(scanInfo, partitionId, isParentReplay, newLastTimestamp, ps);
          Pair<Long, Boolean> result = getDataRowStatesAndTimestamp(ps, newLastTimestamp,
            batchStates, lastScannedTimestamp, scannedRowCount);
          hasMoreRows = result.getSecond();
          if (hasMoreRows) {
            if (!batchStates.isEmpty()) {
              newLastTimestamp = result.getFirst();
            } else if (retryCount >= maxDataVisibilityRetries) {
              LOG.warn(
                "Skipping {} CDC events for table {} partition {} from timestamp {}"
                  + " to {} after {} retries — data table mutations may have failed",
                scannedRowCount[0], dataTableName, partitionId, newLastTimestamp,
                lastScannedTimestamp[0], retryCount);
              metricSource.incrementCdcEventSkippedCount(dataTableName, scannedRowCount[0]);
              newLastTimestamp = lastScannedTimestamp[0];
              break;
            } else {
              // CDC index entries are written but the data is not yet visible.
              // Don't advance newLastTimestamp so the same events are re-fetched
              // once the data becomes visible.
              sleepWithLagSampling(ConnectionUtils.getPauseTime(pause, ++retryCount));
            }
          }
        }
      }
      // Empty own-partition poll proves we are caught up to (queryStart - timestampBufferMs).
      if (!hasMoreRows && !isParentReplay) {
        progress.recordEmptyPoll(lastQueryStartTime);
      }
      if (newLastTimestamp > lastProcessedTimestamp) {
        String sameTimestampQuery = String.format(
          "SELECT /*+ CDC_INCLUDE(DATA_ROW_STATE) */ PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" "
            + "FROM %s WHERE %s PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() = ? "
            + "ORDER BY %s PARTITION_ID() ASC, PHOENIX_ROW_TIMESTAMP() ASC",
          cdcObjectName, scanInfo.filter, scanInfo.orderBy);
        final long timestampToRefetch = newLastTimestamp;
        batchStates.removeIf(pair -> pair.getFirst() == timestampToRefetch);
        try (PreparedStatement ps = conn.prepareStatement(sameTimestampQuery)) {
          int idx = scanInfo.bindParams(ps, 1);
          ps.setString(idx++, partitionId);
          ps.setDate(idx, new Date(newLastTimestamp));
          Pair<Long, Boolean> result = getDataRowStatesAndTimestamp(ps, newLastTimestamp,
            batchStates, lastScannedTimestamp, scannedRowCount);
          newLastTimestamp = result.getFirst();
          if (batchStates.isEmpty()) {
            newLastTimestamp = timestampToRefetch;
          } else if (newLastTimestamp != timestampToRefetch) {
            throw new IOException("Unexpected timestamp mismatch: expected " + timestampToRefetch
              + " but got " + newLastTimestamp);
          }
        }
      }
      generateAndApplyIndexMutations(conn, batchStates, partitionId, ownerPartitionId,
        newLastTimestamp);
      if (!batchStates.isEmpty()) {
        metricSource.updateCdcBatchProcessTime(dataTableName,
          EnvironmentEdgeManager.currentTimeMillis() - batchStartTime);
        metricSource.incrementCdcBatchCount(dataTableName);
      }
      if (newLastTimestamp > lastProcessedTimestamp) {
        if (!isParentReplay) {
          progress.recordProcessed(newLastTimestamp);
        }
        updateTrackerProgress(conn, partitionId, ownerPartitionId, newLastTimestamp,
          PhoenixDatabaseMetaData.TRACKER_STATUS_IN_PROGRESS);
      }
      return newLastTimestamp;
    }
  }

  private void setStatementParams(TenantScanInfo scanInfo, String partitionId,
    boolean isParentReplay, long newLastTimestamp, PreparedStatement ps) throws SQLException {
    int idx = scanInfo.bindParams(ps, 1);
    ps.setString(idx++, partitionId);
    ps.setDate(idx++, new Date(newLastTimestamp));
    if (isParentReplay) {
      ps.setInt(idx, batchSize);
    } else {
      long currentTime = EnvironmentEdgeManager.currentTimeMillis() - timestampBufferMs;
      ps.setDate(idx++, new Date(currentTime));
      ps.setInt(idx, batchSize);
    }
  }

  private static Pair<Long, Boolean> getDataRowStatesAndTimestamp(PreparedStatement ps,
    long initialLastTimestamp, List<Pair<Long, IndexMutationsProtos.DataRowStates>> batchStates,
    long[] lastScannedTimestamp, long[] scannedRowCount) throws SQLException, IOException {
    boolean hasRows = false;
    long lastTimestamp = initialLastTimestamp;
    lastScannedTimestamp[0] = initialLastTimestamp;
    scannedRowCount[0] = 0L;
    try (ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        hasRows = true;
        scannedRowCount[0]++;
        long rowTimestamp = rs.getDate(1).getTime();
        lastScannedTimestamp[0] = rowTimestamp;
        String cdcValue = rs.getString(2);
        if (cdcValue != null && !cdcValue.isEmpty()) {
          byte[] protoBytes = Base64.getDecoder().decode(cdcValue);
          IndexMutationsProtos.DataRowStates dataRowStates =
            IndexMutationsProtos.DataRowStates.parseFrom(protoBytes);
          if (
            dataRowStates.hasDataRowKey()
              && (dataRowStates.hasCurrentDataRowState() || dataRowStates.hasNextDataRowState())
          ) {
            batchStates.add(Pair.newPair(rowTimestamp, dataRowStates));
            lastTimestamp = rowTimestamp;
          }
        }
      }
    }
    return Pair.newPair(lastTimestamp, hasRows);
  }

  private void generateAndApplyIndexMutations(PhoenixConnection conn,
    List<Pair<Long, IndexMutationsProtos.DataRowStates>> batchStates, String partitionId,
    String ownerPartitionId, long lastProcessedTimestamp) throws SQLException, IOException {
    if (batchStates.isEmpty()) {
      return;
    }
    refreshDataTableCacheIfDue(conn, batchStates);
    PTable dataTable = getDataTable(conn);
    byte[] encodedRegionNameBytes = env.getRegion().getRegionInfo().getEncodedNameAsBytes();
    List<Pair<Pair<IndexMaintainer, HTableInterfaceReference>, Long>> targets = new ArrayList<>();
    for (PTable index : dataTable.getIndexes()) {
      long cutoff = getApplyCutoff(dataTable, index);
      if (cutoff != Long.MIN_VALUE) {
        IndexMaintainer maintainer = index.getIndexMaintainer(dataTable, conn);
        HTableInterfaceReference tableRef =
          new HTableInterfaceReference(new ImmutableBytesPtr(maintainer.getIndexTableName()));
        targets.add(new Pair<>(new Pair<>(maintainer, tableRef), cutoff));
      }
    }
    if (targets.isEmpty()) {
      return;
    }
    // Each event goes to the indexes with a cutoff at or after the event timestamp. The list
    // changes only when the event timestamp crosses a cutoff.
    List<Pair<IndexMaintainer, HTableInterfaceReference>> indexTables = Collections.emptyList();
    long tablesFrom = Long.MAX_VALUE;
    long tablesUntil = Long.MIN_VALUE;
    ListMultimap<HTableInterfaceReference, Mutation> indexUpdates = ArrayListMultimap.create();
    int totalMutations = 0;
    long generateStartTime = EnvironmentEdgeManager.currentTimeMillis();
    for (Pair<Long, IndexMutationsProtos.DataRowStates> entry : batchStates) {
      long ts = entry.getFirst();
      if (ts < tablesFrom || ts > tablesUntil) {
        indexTables = new ArrayList<>(targets.size());
        tablesFrom = ts;
        tablesUntil = Long.MAX_VALUE;
        for (Pair<Pair<IndexMaintainer, HTableInterfaceReference>, Long> target : targets) {
          if (ts <= target.getSecond()) {
            indexTables.add(target.getFirst());
            tablesUntil = Math.min(tablesUntil, target.getSecond());
          }
        }
      }
      if (indexTables.isEmpty()) {
        continue;
      }
      lagEmissionEnabled = true;
      IndexMutationsProtos.DataRowStates dataRowStates = entry.getSecond();
      byte[] dataRowKey = dataRowStates.getDataRowKey().toByteArray();
      ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(dataRowKey);

      Put currentDataRowState = null;
      if (dataRowStates.hasCurrentDataRowState()) {
        ClientProtos.MutationProto currentProto = ClientProtos.MutationProto
          .parseFrom(dataRowStates.getCurrentDataRowState().toByteArray());
        Mutation currentMutation = ProtobufUtil.toMutation(currentProto);
        if (currentMutation instanceof Put) {
          currentDataRowState = (Put) currentMutation;
        }
      }
      Put nextDataRowState = null;
      if (dataRowStates.hasNextDataRowState()) {
        ClientProtos.MutationProto nextProto =
          ClientProtos.MutationProto.parseFrom(dataRowStates.getNextDataRowState().toByteArray());
        Mutation nextMutation = ProtobufUtil.toMutation(nextProto);
        if (nextMutation instanceof Put) {
          nextDataRowState = (Put) nextMutation;
        }
      }
      if (currentDataRowState == null && nextDataRowState == null) {
        continue;
      }
      IndexRegionObserver.generateIndexMutationsForRow(rowKeyPtr, currentDataRowState,
        nextDataRowState, ts, encodedRegionNameBytes, QueryConstants.VERIFIED_BYTES, indexTables,
        indexUpdates);
      if (indexUpdates.size() >= batchSize) {
        metricSource.updateCdcMutationGenerateTime(dataTableName,
          EnvironmentEdgeManager.currentTimeMillis() - generateStartTime);
        long applyStartTime = EnvironmentEdgeManager.currentTimeMillis();
        indexWriter.write(indexUpdates, false, MetaDataProtocol.PHOENIX_VERSION);
        metricSource.updateCdcMutationApplyTime(dataTableName,
          EnvironmentEdgeManager.currentTimeMillis() - applyStartTime);
        totalMutations += indexUpdates.size();
        indexUpdates.clear();
        generateStartTime = EnvironmentEdgeManager.currentTimeMillis();
      }
    }
    if (!indexUpdates.isEmpty()) {
      metricSource.updateCdcMutationGenerateTime(dataTableName,
        EnvironmentEdgeManager.currentTimeMillis() - generateStartTime);
      long applyStartTime = EnvironmentEdgeManager.currentTimeMillis();
      indexWriter.write(indexUpdates, false, MetaDataProtocol.PHOENIX_VERSION);
      metricSource.updateCdcMutationApplyTime(dataTableName,
        EnvironmentEdgeManager.currentTimeMillis() - applyStartTime);
      totalMutations += indexUpdates.size();
    }
    if (totalMutations > 0) {
      metricSource.incrementCdcMutationCount(dataTableName, totalMutations);
      LOG.debug(
        "Applied total {} index mutations for table {} partition {} owner {} "
          + ", last processed timestamp {}",
        totalMutations, dataTableName, partitionId, ownerPartitionId, lastProcessedTimestamp);
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
          long applyStartTime = EnvironmentEdgeManager.currentTimeMillis();
          indexWriter.write(indexUpdates, false, MetaDataProtocol.PHOENIX_VERSION);
          metricSource.updateCdcMutationApplyTime(dataTableName,
            EnvironmentEdgeManager.currentTimeMillis() - applyStartTime);
          totalMutations += indexUpdates.size();
          indexUpdates.clear();
        }
      }
      if (!indexUpdates.isEmpty()) {
        long applyStartTime = EnvironmentEdgeManager.currentTimeMillis();
        indexWriter.write(indexUpdates, false, MetaDataProtocol.PHOENIX_VERSION);
        metricSource.updateCdcMutationApplyTime(dataTableName,
          EnvironmentEdgeManager.currentTimeMillis() - applyStartTime);
        totalMutations += indexUpdates.size();
      }
      if (totalMutations > 0) {
        metricSource.incrementCdcMutationCount(dataTableName, totalMutations);
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
