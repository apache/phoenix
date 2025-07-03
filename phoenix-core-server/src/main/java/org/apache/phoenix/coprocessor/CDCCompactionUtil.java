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
package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.query.QueryConstants.NAME_SEPARATOR;

/**
 * Utility class for CDC (Change Data Capture) operations during compaction.
 * This class contains utilities for handling TTL row expiration events and generating
 * CDC events with pre-image data that are written directly to CDC index tables.
 */
public class CDCCompactionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(CDCCompactionUtil.class);

    /**
     * Finds the column name for a given cell in the data table.
     *
     * @param dataTable The data table
     * @param cell      The cell
     * @return The column name or null if not found
     */
    static String findColumnName(PTable dataTable, Cell cell) {
        try {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] defaultCf = dataTable.getDefaultFamilyName() != null ?
                    dataTable.getDefaultFamilyName().getBytes() :
                    QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
            for (PColumn column : dataTable.getColumns()) {
                if (column.getFamilyName() != null &&
                        Bytes.equals(family, column.getFamilyName().getBytes()) &&
                        Bytes.equals(qualifier, column.getColumnQualifierBytes())) {
                    if (Bytes.equals(defaultCf, column.getFamilyName().getBytes())) {
                        return column.getName().getString();
                    } else {
                        return column.getFamilyName().getString() + NAME_SEPARATOR
                                + column.getName().getString();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error finding column name for cell: {}", CellUtil.toString(cell, true),
                    e);
        }
        return null;
    }

    /**
     * Creates a CDC event map for TTL delete with pre-image data.
     *
     * @param expiredRowPut The expired row data
     * @param dataTable     The data table
     * @param preImage      Pre-image map
     * @return CDC event map
     */
    static Map<String, Object> createTTLDeleteCDCEvent(Put expiredRowPut, PTable dataTable,
                                                       Map<String, Object> preImage)
            throws Exception {
        Map<String, Object> cdcEvent = new HashMap<>();
        cdcEvent.put(QueryConstants.CDC_EVENT_TYPE, QueryConstants.CDC_TTL_DELETE_EVENT_TYPE);
        for (List<Cell> familyCells : expiredRowPut.getFamilyCellMap().values()) {
            for (Cell cell : familyCells) {
                String columnName = findColumnName(dataTable, cell);
                if (columnName != null) {
                    PColumn column = dataTable.getColumnForColumnQualifier(
                            CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
                    Object value = column.getDataType().toObject(cell.getValueArray(),
                            cell.getValueOffset(),
                            cell.getValueLength());
                    Object encodedValue =
                            CDCUtil.getColumnEncodedValue(value, column.getDataType());
                    preImage.put(columnName, encodedValue);
                }
            }
        }
        cdcEvent.put(QueryConstants.CDC_PRE_IMAGE, preImage);
        cdcEvent.put(QueryConstants.CDC_POST_IMAGE, Collections.emptyMap());
        return cdcEvent;
    }

    /**
     * Builds CDC index Put mutation.
     *
     * @param cdcIndex            The CDC index table
     * @param expiredRowPut       The expired row data as a Put
     * @param eventTimestamp      The timestamp for the CDC event
     * @param cdcEventBytes       The CDC event data to store
     * @param dataTable           The data table
     * @param env                 The region coprocessor environment
     * @param region              The HBase region
     * @param compactionTimeBytes The compaction time as bytes
     * @return The CDC index Put mutation
     */
    static Put buildCDCIndexPut(PTable cdcIndex, Put expiredRowPut, long eventTimestamp,
                                byte[] cdcEventBytes, PTable dataTable,
                                RegionCoprocessorEnvironment env, Region region,
                                byte[] compactionTimeBytes) throws Exception {

        try (PhoenixConnection serverConnection = QueryUtil.getConnectionOnServer(new Properties(),
                env.getConfiguration()).unwrap(PhoenixConnection.class)) {

            IndexMaintainer cdcIndexMaintainer =
                    cdcIndex.getIndexMaintainer(dataTable, serverConnection);

            ValueGetter dataRowVG = new IndexUtil.SimpleValueGetter(expiredRowPut);
            ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(expiredRowPut.getRow());

            Put cdcIndexPut = cdcIndexMaintainer.buildUpdateMutation(
                    GenericKeyValueBuilder.INSTANCE,
                    dataRowVG,
                    rowKeyPtr,
                    eventTimestamp,
                    null,
                    null,
                    false,
                    region.getRegionInfo().getEncodedNameAsBytes());

            byte[] rowKey = cdcIndexPut.getRow().clone();
            System.arraycopy(compactionTimeBytes, 0, rowKey,
                    PartitionIdFunction.PARTITION_ID_LENGTH, PDate.INSTANCE.getByteSize());
            Put newCdcIndexPut = new Put(rowKey, eventTimestamp);

            newCdcIndexPut.addColumn(
                    cdcIndexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                    cdcIndexMaintainer.getEmptyKeyValueQualifier(), eventTimestamp,
                    QueryConstants.UNVERIFIED_BYTES);

            // Add CDC event data
            newCdcIndexPut.addColumn(
                    QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                    QueryConstants.CDC_IMAGE_CQ_BYTES,
                    eventTimestamp,
                    cdcEventBytes);

            return newCdcIndexPut;
        }
    }

    /**
     * Generates and applies a CDC index mutation for TTL expired row with retries if required.
     *
     * @param cdcIndex                 The CDC index table
     * @param dataTable                The data table
     * @param expiredRowPut            The expired row data as a Put
     * @param eventTimestamp           The timestamp for the CDC event
     * @param tableName                The table name for logging
     * @param env                      The region coprocessor environment
     * @param region                   The HBase region
     * @param compactionTimeBytes      The compaction time as bytes
     * @param cdcTtlMutationMaxRetries Maximum retry attempts for CDC mutations
     */
    static void generateCDCIndexMutation(PTable cdcIndex, PTable dataTable,
                                         Put expiredRowPut,
                                         long eventTimestamp, String tableName,
                                         RegionCoprocessorEnvironment env, Region region,
                                         byte[] compactionTimeBytes,
                                         int cdcTtlMutationMaxRetries)
            throws Exception {
        Map<String, Object> cdcEvent =
                createTTLDeleteCDCEvent(expiredRowPut, dataTable, new HashMap<>());
        byte[] cdcEventBytes =
                JacksonUtil.getObjectWriter(HashMap.class).writeValueAsBytes(cdcEvent);
        Put cdcIndexPut =
                buildCDCIndexPut(cdcIndex, expiredRowPut, eventTimestamp, cdcEventBytes,
                        dataTable, env, region, compactionTimeBytes);

        Exception lastException = null;
        for (int retryCount = 0; retryCount <= cdcTtlMutationMaxRetries; retryCount++) {
            try (Table cdcIndexTable = env.getConnection().getTable(TableName.valueOf(
                    cdcIndex.getPhysicalName().getBytes()))) {
                CheckAndMutate checkAndMutate =
                        CheckAndMutate.newBuilder(cdcIndexPut.getRow())
                                .ifNotExists(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                                        QueryConstants.CDC_IMAGE_CQ_BYTES)
                                .build(cdcIndexPut);
                CheckAndMutateResult result = cdcIndexTable.checkAndMutate(checkAndMutate);

                if (result.isSuccess()) {
                    // Successfully inserted new CDC event - Single CF case
                    lastException = null;
                    break;
                } else {
                    // Row already exists, need to retrieve existing pre-image and merge
                    // Likely to happen for multi CF case
                    Get get = new Get(cdcIndexPut.getRow());
                    get.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                            QueryConstants.CDC_IMAGE_CQ_BYTES);
                    Result existingResult = cdcIndexTable.get(get);

                    if (!existingResult.isEmpty()) {
                        Cell existingCell = existingResult.getColumnLatestCell(
                                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                                QueryConstants.CDC_IMAGE_CQ_BYTES);

                        if (existingCell != null) {
                            byte[] existingCdcBytes = CellUtil.cloneValue(existingCell);
                            Map<String, Object> existingCdcEvent =
                                    JacksonUtil.getObjectReader(HashMap.class)
                                            .readValue(existingCdcBytes);
                            Map<String, Object> existingPreImage =
                                    (Map<String, Object>) existingCdcEvent.getOrDefault(
                                            QueryConstants.CDC_PRE_IMAGE, new HashMap<>());

                            // Create new TTL delete event with merged pre-image
                            Map<String, Object> mergedCdcEvent =
                                    createTTLDeleteCDCEvent(expiredRowPut, dataTable,
                                            existingPreImage);
                            byte[] mergedCdcEventBytes =
                                    JacksonUtil.getObjectWriter(HashMap.class)
                                            .writeValueAsBytes(mergedCdcEvent);

                            Put mergedCdcIndexPut = buildCDCIndexPut(cdcIndex, expiredRowPut,
                                    eventTimestamp, mergedCdcEventBytes, dataTable, env, region,
                                    compactionTimeBytes);

                            cdcIndexTable.put(mergedCdcIndexPut);
                            lastException = null;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                lastException = e;
                if (retryCount < cdcTtlMutationMaxRetries) {
                    long backoffMs = 100;
                    LOGGER.warn("CDC mutation attempt {}/{} failed, retrying in {}ms",
                            retryCount + 1, cdcTtlMutationMaxRetries + 1, backoffMs, e);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during CDC mutation retry", ie);
                    }
                }
            }
        }
        if (lastException != null) {
            LOGGER.error("Failed to generate CDC mutation after {} attempts for table {}, index " +
                            "{}. The event update is missed.",
                    cdcTtlMutationMaxRetries + 1,
                    tableName,
                    cdcIndex.getPhysicalName().getString(),
                    lastException);
        }
    }

    /**
     * Generates CDC TTL delete event and writes it directly to CDC index tables.
     * This bypasses the normal CDC update path since the row is being expired.
     *
     * @param expiredRow               The cells of the expired row
     * @param tableName                The table name for logging
     * @param compactionTime           The compaction timestamp
     * @param dataTable                The data table
     * @param env                      The region coprocessor environment
     * @param region                   The HBase region
     * @param compactionTimeBytes      The compaction time as bytes
     * @param cdcTtlMutationMaxRetries Maximum retry attempts for CDC mutations
     */
    static void generateCDCTTLDeleteEvent(List<Cell> expiredRow, String tableName,
                                          long compactionTime, PTable dataTable,
                                          RegionCoprocessorEnvironment env, Region region,
                                          byte[] compactionTimeBytes,
                                          int cdcTtlMutationMaxRetries) {
        if (expiredRow.isEmpty()) {
            return;
        }
        try {
            List<PTable> cdcIndexes = new ArrayList<>();
            for (PTable index : dataTable.getIndexes()) {
                if (CDCUtil.isCDCIndex(index)) {
                    cdcIndexes.add(index);
                }
            }
            if (cdcIndexes.isEmpty()) {
                LOGGER.debug("No CDC indexes found for table {}", tableName);
                return;
            }
            Cell firstCell = expiredRow.get(0);
            byte[] dataRowKey = CellUtil.cloneRow(firstCell);
            Put expiredRowPut = new Put(dataRowKey);

            for (Cell cell : expiredRow) {
                expiredRowPut.add(cell);
            }

            for (PTable cdcIndex : cdcIndexes) {
                try {
                    generateCDCIndexMutation(cdcIndex, dataTable, expiredRowPut, compactionTime,
                            tableName, env, region, compactionTimeBytes, cdcTtlMutationMaxRetries);
                } catch (Exception e) {
                    LOGGER.error("Failed to generate CDC mutation for index {}: {}",
                            cdcIndex.getName().getString(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error generating CDC TTL delete event for table {}",
                    tableName, e);
        }
    }

    /**
     * Handles TTL row expiration for CDC event generation.
     * This method is called when a row is detected as expired during major compaction.
     *
     * @param expiredRow               The cells of the expired row
     * @param expirationType           The type of TTL expiration
     * @param tableName                The table name for logging purposes
     * @param compactionTime           The timestamp when compaction started
     * @param table                    The Phoenix data table metadata
     * @param env                      The region coprocessor environment for accessing HBase
     *                                 resources
     * @param region                   The HBase region being compacted
     * @param compactionTimeBytes      The compaction timestamp as byte array for CDC index row key
     *                                 construction
     * @param cdcTtlMutationMaxRetries Maximum number of retry attempts for CDC mutation operations
     */
    static void handleTTLRowExpiration(List<Cell> expiredRow, String expirationType,
                                       String tableName, long compactionTime, PTable table,
                                       RegionCoprocessorEnvironment env, Region region,
                                       byte[] compactionTimeBytes,
                                       int cdcTtlMutationMaxRetries) {
        if (expiredRow.isEmpty()) {
            return;
        }

        try {
            Cell firstCell = expiredRow.get(0);
            byte[] rowKey = CellUtil.cloneRow(firstCell);

            LOGGER.info("TTL row expiration detected: table={}, rowKey={}, expirationType={}, "
                            + "cellCount={}, compactionTime={}",
                    tableName,
                    Bytes.toStringBinary(rowKey),
                    expirationType,
                    expiredRow.size(),
                    compactionTime);

            // Generate CDC TTL delete event with pre-image data
            generateCDCTTLDeleteEvent(expiredRow, tableName, compactionTime, table, env, region,
                    compactionTimeBytes, cdcTtlMutationMaxRetries);

        } catch (Exception e) {
            LOGGER.error("Error handling TTL row expiration for CDC: table {}", tableName, e);
        }
    }
}