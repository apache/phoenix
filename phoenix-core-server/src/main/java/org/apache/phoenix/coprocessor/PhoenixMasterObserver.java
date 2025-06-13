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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.ParentPartitionNotFound;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixMasterSource;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import java.io.IOException;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;

/**
 * Master Coprocessor for Phoenix.
 */
public class PhoenixMasterObserver implements MasterObserver, MasterCoprocessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMasterObserver.class);
    private MetricsPhoenixMasterSource metricSource;

    private static final String STREAM_STATUS_QUERY
            = "SELECT STREAM_NAME FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = ? AND STREAM_STATUS='"
            + CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue() + "'";

    // tableName, streamName, partitionId, parentId, startTime, endTime, startKey, endKey
    private static final String PARTITION_UPSERT_SQL
            = "UPSERT INTO " + SYSTEM_CDC_STREAM_NAME + " VALUES (?,?,?,?,?,?,?,?)";

    private static final String PARENT_PARTITION_QUERY_FOR_SPLIT
            = "SELECT PARTITION_ID, PARENT_PARTITION_ID FROM " + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = ? AND STREAM_NAME = ? AND PARTITION_END_TIME IS NULL ";

    private static final String PARENT_PARTITION_QUERY_FOR_MERGE
            = "SELECT PARENT_PARTITION_ID FROM " + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = ? AND STREAM_NAME = ? AND PARTITION_ID = ?";

    private static final String PARENT_PARTITION_UPDATE_END_TIME_SQL
            = "UPSERT INTO " + SYSTEM_CDC_STREAM_NAME + " (TABLE_NAME, STREAM_NAME, PARTITION_ID, "
            + "PARENT_PARTITION_ID, PARTITION_END_TIME) VALUES (?,?,?,?,?)";

    public static final String PHOENIX_MASTER_MAX_RETRY_COUNT = "phoenix.master.max.retry.count";
    public static final int DEFAULT_PHOENIX_MASTER_MAX_RETRY_COUNT = 20;

    @Override
    public Optional<MasterObserver> getMasterObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        metricSource =
                MetricsPhoenixCoprocessorSourceFactory.getInstance().getPhoenixMasterSource();
    }

    /**
     * Update parent -> daughter relationship for CDC Streams when a region splits.
     * - find parent partition id using start/end keys of daughters
     * - upsert partition metadata for the 2 daughters
     * - update the end time on the parent's partition metadata
     * @param c           the environment to interact with the framework and master
     * @param regionInfoA the left daughter region
     * @param regionInfoB the right daughter region
     */
    @Override
    public void postCompletedSplitRegionAction(final ObserverContext<MasterCoprocessorEnvironment> c,
                                               final RegionInfo regionInfoA,
                                               final RegionInfo regionInfoB) throws IOException {
        Configuration conf = c.getEnvironment().getConfiguration();
        int maxRetryCount =
                conf.getInt(PHOENIX_MASTER_MAX_RETRY_COUNT, DEFAULT_PHOENIX_MASTER_MAX_RETRY_COUNT);
        int tries = 0;
        Exception caughtException;
        do {
            try (Connection conn = QueryUtil.getConnectionOnServer(conf)) {
                // CDC will be enabled on Phoenix tables only
                PTable phoenixTable = getPhoenixTable(conn, regionInfoA.getTable());
                if (phoenixTable == null) {
                    LOGGER.debug("{} is not a Phoenix Table, skipping partition metadata update.",
                            regionInfoA.getTable());
                    return;
                }
                String tableName = phoenixTable.getName().getString();
                String streamName = getStreamName(conn, tableName);
                if (streamName != null) {
                    LOGGER.info(
                            "Updating split partition metadata for table={}, stream={} "
                                    + "daughters {} {}",
                            tableName, streamName, regionInfoA.getEncodedName(),
                            regionInfoB.getEncodedName());
                    // ancestorIDs = [parentId, grandparentId1, grandparentId2...]
                    List<String> ancestorIDs
                            = getAncestorIdsForSplit(conn, tableName, streamName, regionInfoA,
                            regionInfoB);

                    upsertDaughterPartitions(conn, tableName, streamName, ancestorIDs.subList(0, 1),
                            Arrays.asList(regionInfoA, regionInfoB));

                    updateParentPartitionEndTime(conn, tableName, streamName, ancestorIDs,
                            regionInfoA.getRegionId());
                    return;
                } else {
                    LOGGER.debug("{} does not have a stream enabled, skipping partition metadata"
                            + " update.", regionInfoA.getTable());
                    return;
                }
            } catch (ParentPartitionNotFound e) {
                LOGGER.debug("Parent partition not found, skipping remaining steps.", e);
                return;
            } catch (TableNotFoundException e) {
                LOGGER.warn("System Table not found during region split completion. "
                        + "This must happen before metadata upgrade only.", e);
                return;
            } catch (Exception e) {
                LOGGER.error("Try: {}. Unable to update CDC Stream Partition metadata during"
                                + " split with daughter regions: {} {}",
                        tries, regionInfoA.getEncodedName(), regionInfoB.getEncodedName(), e);
                metricSource.incrementPostSplitPartitionUpdateFailureCount();
                caughtException = e;
            }
        } while (tries++ < maxRetryCount);
        // All retries were exhausted
        throw new IOException(
                "Failed to update CDC Stream Partition metadata after " + maxRetryCount +
                        " retries during split. Daughter regions: " +
                        regionInfoA.getEncodedName() + " " +
                        regionInfoB.getEncodedName(), caughtException);
    }

    /**
     * Update parent -> daughter relationship for CDC Streams when regions merge.
     * - upsert partition metadata for the daughter with each parent
     * - update the end time on all the parents' partition metadata
     * @param c the environment to interact with the framework and master
     * @param regionsToMerge parent regions which merged
     * @param mergedRegion daughter region
     */
    @Override
    public void postCompletedMergeRegionsAction(final ObserverContext<MasterCoprocessorEnvironment> c,
                                                final RegionInfo[] regionsToMerge,
                                                final RegionInfo mergedRegion) throws IOException {
        Configuration conf = c.getEnvironment().getConfiguration();
        int maxRetryCount = conf.getInt(PHOENIX_MASTER_MAX_RETRY_COUNT, DEFAULT_PHOENIX_MASTER_MAX_RETRY_COUNT);
        int tries = 0;
        Exception caughtException;
        do {
            try (Connection conn = QueryUtil.getConnectionOnServer(conf)) {
                // CDC will be enabled on Phoenix tables only
                PTable phoenixTable = getPhoenixTable(conn, mergedRegion.getTable());
                if (phoenixTable == null) {
                    LOGGER.debug("{} is not a Phoenix Table, skipping partition metadata update.",
                            mergedRegion.getTable());
                    return;
                }
                String tableName = phoenixTable.getName().getString();
                String streamName = getStreamName(conn, tableName);
                if (streamName != null) {
                    LOGGER.info(
                            "Updating merged partition metadata for table={}, stream={} "
                                    + "daughter {}",
                            tableName, streamName, mergedRegion.getEncodedName());
                    // upsert a row for daughter-parent for each merged region
                    upsertDaughterPartitions(conn, tableName, streamName,
                            Arrays.stream(regionsToMerge).map(RegionInfo::getEncodedName)
                                    .collect(Collectors.toList()),
                            Collections.singletonList(mergedRegion));

                    // lookup all ancestors of a merged region and update the endTime
                    for (RegionInfo ri : regionsToMerge) {
                        List<String> ancestorIDs =
                                getAncestorIdsForMerge(conn, tableName, streamName, ri);
                        updateParentPartitionEndTime(conn, tableName, streamName, ancestorIDs,
                                mergedRegion.getRegionId());
                    }
                    return;
                } else {
                    LOGGER.debug("{} does not have a stream enabled, skipping partition metadata"
                                    + " update.", mergedRegion.getTable());
                    return;
                }
            } catch (ParentPartitionNotFound e) {
                LOGGER.debug("Parent partition not found, skipping remaining steps.", e);
                return;
            } catch (TableNotFoundException e) {
                LOGGER.warn("System Table not found during region merge completion. "
                        + "This must happen before metadata upgrade only.", e);
                return;
            } catch (Exception e) {
                LOGGER.error("Try: {}. Unable to update CDC Stream Partition metadata during"
                                + " merge with parent regions: {} and daughter region {}",
                        tries, regionsToMerge, mergedRegion.getEncodedName(), e);
                metricSource.incrementPostMergePartitionUpdateFailureCount();
                caughtException = e;
            }
        } while (tries++ < maxRetryCount);
        // All retries exhausted
        throw new IOException(
                "Failed to update CDC Stream Partition metadata after " + maxRetryCount +
                        " retries during merge with parent regions: " +
                        Arrays.toString(regionsToMerge) +
                        " and daughter region: " + mergedRegion.getEncodedName(), caughtException);
    }

    /**
     * Lookup a split parent's partition id (region's encoded name) in SYSTEM.CDC_STREAM.
     * RegionInfoA is left daughter and RegionInfoB is right daughter so parent's key range would
     * be [RegionInfoA startKey, RegionInfoB endKey]
     * Return parent and all grandparent partition ids.
     *
     */
    protected List<String> getAncestorIdsForSplit(Connection conn, String tableName,
                                                  String streamName,
                                                  RegionInfo regionInfoA, RegionInfo regionInfoB)
            throws SQLException {
        byte[] parentStartKey = regionInfoA.getStartKey();
        byte[] parentEndKey = regionInfoB.getEndKey();

        StringBuilder qb = new StringBuilder(PARENT_PARTITION_QUERY_FOR_SPLIT);
        if (parentStartKey.length == 0) {
            qb.append(" AND PARTITION_START_KEY IS NULL ");
        } else {
            qb.append(" AND PARTITION_START_KEY = ? ");
        }
        if (parentEndKey.length == 0) {
            qb.append(" AND PARTITION_END_KEY IS NULL ");
        } else {
            qb.append(" AND PARTITION_END_KEY = ? ");
        }

        PreparedStatement pstmt = conn.prepareStatement(qb.toString());
        int index = 1;
        pstmt.setString(index++, tableName);
        pstmt.setString(index++, streamName);
        if (parentStartKey.length > 0) pstmt.setBytes(index++, parentStartKey);
        if (parentEndKey.length > 0) pstmt.setBytes(index++, parentEndKey);
        LOGGER.info("Query to get parent partition id: " + pstmt);

        List<String> ancestorIDs = new ArrayList<>();
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            ancestorIDs.add(rs.getString(1));
            ancestorIDs.add(rs.getString(2));
        } else {
            throw new ParentPartitionNotFound(
                    String.format("Could not find parent of the provided daughters: "
                                    + "startKeyA=%s endKeyA=%s startKeyB=%s endKeyB=%s",
                    Bytes.toStringBinary(regionInfoA.getStartKey()),
                    Bytes.toStringBinary(regionInfoA.getEndKey()),
                    Bytes.toStringBinary(regionInfoB.getStartKey()),
                    Bytes.toStringBinary(regionInfoB.getEndKey())));
        }
        // if parent was a result of a merge, there will be multiple grandparents.
        while (rs.next()) {
            ancestorIDs.add(rs.getString(2));
        }
        return ancestorIDs;
    }

    /**
     * Lookup the parent of a merged region.
     * If the merged region was an output of a merge in the past, it will have multiple parents.
     */
    protected List<String> getAncestorIdsForMerge(Connection conn, String tableName,
                                                  String streamName,
                                                  RegionInfo parent) throws SQLException {
        List<String> ancestorIDs = new ArrayList<>();
        ancestorIDs.add(parent.getEncodedName());
        PreparedStatement pstmt = conn.prepareStatement(PARENT_PARTITION_QUERY_FOR_MERGE);
        pstmt.setString(1, tableName);
        pstmt.setString(2, streamName);
        pstmt.setString(3, parent.getEncodedName());
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            ancestorIDs.add(rs.getString(1));
        } else {
            throw new ParentPartitionNotFound(String.format(
                    "Could not find parent of the provided merged region: %s",
                    parent.getEncodedName()));
        }
        // if parent was a result of a merge, there will be multiple grandparents.
        while (rs.next()) {
            ancestorIDs.add(rs.getString(1));
        }
        return ancestorIDs;
    }

    /**
     * Insert partition metadata for a daughter region from a split or a merge.
     * split: 2 daughters, 1 parent
     * merge: 1 daughter, N parents
     */
    private void upsertDaughterPartitions(Connection conn, String tableName,
                                         String streamName, List<String> parentPartitionIDs,
                                         List<RegionInfo> daughters)
            throws SQLException {
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement(PARTITION_UPSERT_SQL);
        for (RegionInfo daughter : daughters) {
            for (String parentPartitionID : parentPartitionIDs) {
                String partitionId = daughter.getEncodedName();
                long startTime = daughter.getRegionId();
                byte[] startKey = daughter.getStartKey();
                byte[] endKey = daughter.getEndKey();
                pstmt.setString(1, tableName);
                pstmt.setString(2, streamName);
                pstmt.setString(3, partitionId);
                pstmt.setString(4, parentPartitionID);
                pstmt.setLong(5, startTime);
                // endTime in not set when inserting a new partition
                pstmt.setNull(6, Types.BIGINT);
                pstmt.setBytes(7, startKey.length == 0 ? null : startKey);
                pstmt.setBytes(8, endKey.length == 0 ? null : endKey);
                pstmt.executeUpdate();
            }
        }
        conn.commit();
    }

    /**
     * Update endTime in all rows of parent partition by setting it to daughter's startTime.
     * parent came from a split : there will only be one record
     * parent came from a merge : there will be multiple rows, one per grandparent
     *
     */
    private void updateParentPartitionEndTime(Connection conn, String tableName,
                                              String streamName, List<String> ancestorIDs,
                                              long daughterStartTime) throws SQLException {
        conn.setAutoCommit(false);
        // ancestorIDs = [parentID, grandparentID1, grandparentID2...]
        PreparedStatement pstmt = conn.prepareStatement(PARENT_PARTITION_UPDATE_END_TIME_SQL);
        for (int i=1; i<ancestorIDs.size(); i++) {
            pstmt.setString(1, tableName);
            pstmt.setString(2, streamName);
            pstmt.setString(3, ancestorIDs.get(0));
            pstmt.setString(4, ancestorIDs.get(i));
            pstmt.setLong(5, daughterStartTime);
            pstmt.executeUpdate();
        }
        conn.commit();
    }

    /**
     * Get the stream name on the given table if one exists in ENABLED state.
     */
    private String getStreamName(Connection conn, String tableName) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(STREAM_STATUS_QUERY);
        pstmt.setString(1, tableName);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            return rs.getString(1);
        } else {
            return null;
        }
    }

    private PTable getPhoenixTable(Connection conn, TableName tableName) throws SQLException {
        PTable pTable;
        try {
            pTable = PhoenixRuntime.getTable(conn, tableName.toString());
        } catch (TableNotFoundException e) {
            return null;
        }
        return pTable;
    }
}
