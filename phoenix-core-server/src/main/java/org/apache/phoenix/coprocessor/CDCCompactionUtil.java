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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.query.QueryConstants.NAME_SEPARATOR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.function.PartitionIdFunction;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;

/**
 * Utility class for CDC (Change Data Capture) operations during compaction. This class contains
 * utilities for handling TTL row expiration events and generating CDC events with pre-image data
 * that are written directly to CDC index tables using batch mutations.
 */
public final class CDCCompactionUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(CDCCompactionUtil.class);

  // Shared cache for row images across all CompactionScanner instances in the JVM.
  // Entries expire after 1200 seconds (20 minutes) by default.
  // The JVM level cache helps merge the pre-image for the row with multiple CFs.
  // The key of the cache contains (regionId + data table rowkey).
  // The value contains pre-image that needs to be directly inserted in the CDC index.
  private static volatile Cache<ImmutableBytesPtr, Map<String, Object>> sharedTtlImageCache;

  private CDCCompactionUtil() {
    // empty
  }

  /**
   * Gets the shared row image cache, initializing it lazily with configuration.
   * @param config The Hadoop configuration to read cache expiry from
   * @return the shared cache instance
   */
  static Cache<ImmutableBytesPtr, Map<String, Object>>
    getSharedRowImageCache(Configuration config) {
    if (sharedTtlImageCache == null) {
      synchronized (CDCCompactionUtil.class) {
        if (sharedTtlImageCache == null) {
          int expirySeconds = config.getInt(QueryServices.CDC_TTL_SHARED_CACHE_EXPIRY_SECONDS,
            QueryServicesOptions.DEFAULT_CDC_TTL_SHARED_CACHE_EXPIRY_SECONDS);
          sharedTtlImageCache =
            CacheBuilder.newBuilder().expireAfterWrite(expirySeconds, TimeUnit.SECONDS).build();
          LOGGER.info("Initialized shared CDC row image cache with expiry of {} seconds",
            expirySeconds);
        }
      }
    }
    return sharedTtlImageCache;
  }

  /**
   * Batch processor for CDC mutations during compaction. This class manages accumulating mutations
   * and maintaining in-memory image tracking for the duration of the compaction operation.
   */
  public static class CDCBatchProcessor {

    private final Map<ImmutableBytesPtr, Put> pendingMutations;
    private final PTable cdcIndex;
    private final PTable dataTable;
    private final RegionCoprocessorEnvironment env;
    private final Region region;
    private final byte[] compactionTimeBytes;
    private final long eventTimestamp;
    private final String tableName;
    private final int cdcTtlMutationMaxRetries;
    private final int batchSize;
    private final Configuration config;

    public CDCBatchProcessor(PTable cdcIndex, PTable dataTable, RegionCoprocessorEnvironment env,
      Region region, byte[] compactionTimeBytes, long eventTimestamp, String tableName,
      int cdcTtlMutationMaxRetries, int batchSize) {
      this.pendingMutations = new HashMap<>();
      this.cdcIndex = cdcIndex;
      this.dataTable = dataTable;
      this.env = env;
      this.region = region;
      this.compactionTimeBytes = compactionTimeBytes;
      this.eventTimestamp = eventTimestamp;
      this.tableName = tableName;
      this.cdcTtlMutationMaxRetries = cdcTtlMutationMaxRetries;
      this.batchSize = batchSize;
      this.config = env.getConfiguration();
    }

    /**
     * Adds a CDC event for the specified expired row. If the row already exists in memory, merges
     * the image with the existing image. Accumulates mutations for batching.
     * @param expiredRow The expired row.
     * @throws Exception If something goes wrong.
     */
    public void addCDCEvent(List<Cell> expiredRow) throws Exception {
      Cell firstCell = expiredRow.get(0);
      byte[] dataRowKey = CellUtil.cloneRow(firstCell);

      Put expiredRowPut = new Put(dataRowKey);
      for (Cell cell : expiredRow) {
        expiredRowPut.add(cell);
      }

      IndexMaintainer cdcIndexMaintainer;
      // rowKey for the Index mutation
      byte[] rowKey;
      try (PhoenixConnection serverConnection =
        QueryUtil.getConnectionOnServer(new Properties(), env.getConfiguration())
          .unwrap(PhoenixConnection.class)) {
        cdcIndexMaintainer = cdcIndex.getIndexMaintainer(dataTable, serverConnection);

        ValueGetter dataRowVG = new IndexUtil.SimpleValueGetter(expiredRowPut);
        ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(expiredRowPut.getRow());

        Put cdcIndexPut = cdcIndexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
          dataRowVG, rowKeyPtr, eventTimestamp, null, null, false,
          region.getRegionInfo().getEncodedNameAsBytes());

        rowKey = cdcIndexPut.getRow().clone();
        System.arraycopy(compactionTimeBytes, 0, rowKey, PartitionIdFunction.PARTITION_ID_LENGTH,
          PDate.INSTANCE.getByteSize());
      }

      byte[] rowKeyWithoutTimestamp = new byte[rowKey.length - PDate.INSTANCE.getByteSize()];
      // copy PARTITION_ID() from offset 0 to 31
      System.arraycopy(rowKey, 0, rowKeyWithoutTimestamp, 0,
        PartitionIdFunction.PARTITION_ID_LENGTH);
      // copy data table rowkey from offset (32 + 8) to end of rowkey
      System.arraycopy(rowKey,
        PartitionIdFunction.PARTITION_ID_LENGTH + PDate.INSTANCE.getByteSize(),
        rowKeyWithoutTimestamp, PartitionIdFunction.PARTITION_ID_LENGTH,
        rowKeyWithoutTimestamp.length - PartitionIdFunction.PARTITION_ID_LENGTH);
      ImmutableBytesPtr cacheKeyPtr = new ImmutableBytesPtr(rowKeyWithoutTimestamp);

      // Check if we already have an image for this row in the shared cache, from other store
      // compaction of the same region
      Cache<ImmutableBytesPtr, Map<String, Object>> cache = getSharedRowImageCache(config);
      Map<String, Object> existingPreImage = cache.getIfPresent(cacheKeyPtr);
      if (existingPreImage == null) {
        existingPreImage = new HashMap<>();
        cache.put(cacheKeyPtr, existingPreImage);
      }

      // Create CDC event with merged pre-image
      Map<String, Object> cdcEvent =
        createTTLDeleteCDCEvent(expiredRowPut, dataTable, existingPreImage);
      byte[] cdcEventBytes = JacksonUtil.getObjectWriter(HashMap.class).writeValueAsBytes(cdcEvent);
      Put cdcIndexPut = buildCDCIndexPut(eventTimestamp, cdcEventBytes, rowKey, cdcIndexMaintainer);

      pendingMutations.put(cacheKeyPtr, cdcIndexPut);

      if (pendingMutations.size() >= batchSize) {
        flushBatch();
      }
    }

    /**
     * Flushes any pending mutations in the current batch.
     */
    public void flushBatch() throws Exception {
      if (pendingMutations.isEmpty()) {
        return;
      }

      Exception lastException = null;
      for (int retryCount = 0; retryCount < cdcTtlMutationMaxRetries; retryCount++) {
        try (Table cdcIndexTable =
          env.getConnection().getTable(TableName.valueOf(cdcIndex.getPhysicalName().getBytes()))) {
          cdcIndexTable.put(new ArrayList<>(pendingMutations.values()));
          lastException = null;
          LOGGER.debug("Successfully flushed batch of {} CDC mutations for table {}",
            pendingMutations.size(), tableName);
          break;
        } catch (Exception e) {
          lastException = e;
          long backoffMs = 100;
          LOGGER.warn("CDC batch mutation attempt {}/{} failed, retrying in {}ms. Batch size: {}",
            retryCount + 1, cdcTtlMutationMaxRetries, backoffMs, pendingMutations.size(), e);
          try {
            Thread.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during CDC batch mutation retry", ie);
          }
        }
      }

      if (lastException != null) {
        LOGGER.error(
          "Failed to flush CDC batch after {} attempts for table {}, index {}. {} "
            + "events are missed.",
          cdcTtlMutationMaxRetries, tableName, cdcIndex.getPhysicalName().getString(),
          pendingMutations.size(), lastException);
      }

      pendingMutations.clear();
    }

    /**
     * Finalizes the batch processor by flushing any remaining mutations.
     */
    public void close() throws Exception {
      flushBatch();
      Cache<ImmutableBytesPtr, Map<String, Object>> cache = getSharedRowImageCache(config);
      LOGGER.debug("CDC batch processor closed for table {}. Shared cache size: {}", tableName,
        cache.size());
    }
  }

  /**
   * Finds the column name for a given cell in the data table.
   * @param dataTable The data table
   * @param cell      The cell
   * @return The column name or null if not found
   */
  private static String findColumnName(PTable dataTable, Cell cell) {
    try {
      byte[] family = CellUtil.cloneFamily(cell);
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      byte[] defaultCf = dataTable.getDefaultFamilyName() != null
        ? dataTable.getDefaultFamilyName().getBytes()
        : QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
      for (PColumn column : dataTable.getColumns()) {
        if (
          column.getFamilyName() != null && Bytes.equals(family, column.getFamilyName().getBytes())
            && Bytes.equals(qualifier, column.getColumnQualifierBytes())
        ) {
          if (Bytes.equals(defaultCf, column.getFamilyName().getBytes())) {
            return column.getName().getString();
          } else {
            return column.getFamilyName().getString() + NAME_SEPARATOR
              + column.getName().getString();
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error finding column name for cell: {}", CellUtil.toString(cell, true), e);
    }
    return null;
  }

  /**
   * Creates a CDC event map for TTL delete with pre-image data.
   * @param expiredRowPut The expired row data
   * @param dataTable     The data table
   * @param preImage      Pre-image map
   * @return CDC event map
   */
  private static Map<String, Object> createTTLDeleteCDCEvent(Put expiredRowPut, PTable dataTable,
    Map<String, Object> preImage) throws Exception {
    Map<String, Object> cdcEvent = new HashMap<>();
    cdcEvent.put(QueryConstants.CDC_EVENT_TYPE, QueryConstants.CDC_TTL_DELETE_EVENT_TYPE);
    for (List<Cell> familyCells : expiredRowPut.getFamilyCellMap().values()) {
      for (Cell cell : familyCells) {
        String columnName = findColumnName(dataTable, cell);
        if (columnName != null) {
          PColumn column = dataTable.getColumnForColumnQualifier(CellUtil.cloneFamily(cell),
            CellUtil.cloneQualifier(cell));
          Object value = column.getDataType().toObject(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength());
          Object encodedValue = CDCUtil.getColumnEncodedValue(value, column.getDataType());
          preImage.put(columnName, encodedValue);
        }
      }
    }
    cdcEvent.put(QueryConstants.CDC_PRE_IMAGE, preImage);
    return cdcEvent;
  }

  /**
   * Builds CDC index Put mutation.
   * @param eventTimestamp     The timestamp for the CDC event
   * @param cdcEventBytes      The CDC event data to store
   * @param rowKey             The rowKey of the CDC index mutation
   * @param cdcIndexMaintainer The index maintainer object for the CDC index
   * @return The CDC index Put mutation
   */
  private static Put buildCDCIndexPut(long eventTimestamp, byte[] cdcEventBytes, byte[] rowKey,
    IndexMaintainer cdcIndexMaintainer) {

    Put newCdcIndexPut = new Put(rowKey, eventTimestamp);

    newCdcIndexPut.addColumn(cdcIndexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
      cdcIndexMaintainer.getEmptyKeyValueQualifier(), eventTimestamp,
      QueryConstants.UNVERIFIED_BYTES);

    // Add CDC event data
    newCdcIndexPut.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
      QueryConstants.CDC_IMAGE_CQ_BYTES, eventTimestamp, cdcEventBytes);

    return newCdcIndexPut;
  }

  /**
   * Creates a CDC batch processor for the given data table and configuration.
   * @param dataTable                The data table
   * @param env                      The region coprocessor environment
   * @param region                   The HBase region
   * @param compactionTimeBytes      The compaction time as bytes
   * @param compactionTime           The compaction timestamp
   * @param tableName                The table name for logging
   * @param cdcTtlMutationMaxRetries Maximum retry attempts for CDC mutations
   * @param batchSize                The batch size for CDC mutations
   * @return CDCBatchProcessor instance or null if no active CDC index
   */
  public static CDCBatchProcessor createBatchProcessor(PTable dataTable,
    RegionCoprocessorEnvironment env, Region region, byte[] compactionTimeBytes,
    long compactionTime, String tableName, int cdcTtlMutationMaxRetries, int batchSize) {
    PTable cdcIndex = CDCUtil.getActiveCDCIndex(dataTable);
    if (cdcIndex == null) {
      LOGGER.warn("No active CDC index found for table {}", tableName);
      return null;
    }
    return new CDCBatchProcessor(cdcIndex, dataTable, env, region, compactionTimeBytes,
      compactionTime, tableName, cdcTtlMutationMaxRetries, batchSize);
  }

  /**
   * Handles TTL row expiration for CDC event generation using batch processing. This method is
   * called when a row is detected as expired during major compaction.
   * @param expiredRow     The cells of the expired row
   * @param expirationType The type of TTL expiration
   * @param tableName      The table name for logging purposes
   * @param batchProcessor The CDC batch processor instance
   */
  static void handleTTLRowExpiration(List<Cell> expiredRow, String expirationType, String tableName,
    CDCBatchProcessor batchProcessor) {
    if (batchProcessor == null) {
      return;
    }

    try {
      Cell firstCell = expiredRow.get(0);
      byte[] rowKey = CellUtil.cloneRow(firstCell);

      LOGGER.debug(
        "TTL row expiration detected: table={}, rowKey={}, expirationType={}, "
          + "cellCount={}, compactionTime={}",
        tableName, Bytes.toStringBinary(rowKey), expirationType, expiredRow.size(),
        batchProcessor.eventTimestamp);

      batchProcessor.addCDCEvent(expiredRow);
    } catch (Exception e) {
      LOGGER.error("Error handling TTL row expiration for CDC: table {}", tableName, e);
    }
  }

}
