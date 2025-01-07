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
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;

/**
 * Master Coprocessor for Phoenix.
 */
public class PhoenixMasterObserver implements MasterObserver, MasterCoprocessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMasterObserver.class);

    private static final String STREAM_STATUS_QUERY
            = "SELECT STREAM_NAME FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = ? AND STREAM_STATUS='"
            + CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue() + "'";

    // tableName, streamName, partitionId, parentId, startTime, endTime, startKey, endKey
    private static final String PARTITION_UPSERT_SQL
            = "UPSERT INTO " + SYSTEM_CDC_STREAM_NAME + " VALUES (?,?,?,?,?,?,?,?)";

    private static final String PARENT_PARTITION_QUERY
            = "SELECT PARTITION_ID FROM " + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = ? AND STREAM_NAME = ? ";

    private static final String PARENT_PARTITION_UPDATE_END_TIME_SQL
            = "UPSERT INTO " + SYSTEM_CDC_STREAM_NAME + " (TABLE_NAME, STREAM_NAME, PARTITION_ID, "
            + "PARTITION_END_TIME) VALUES (?,?,?,?)";

    @Override
    public Optional<MasterObserver> getMasterObserver() {
        return Optional.of(this);
    }

    /**
     * Update parent -> daughter relationship for CDC Streams.
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
                                               final RegionInfo regionInfoB) {
        Configuration conf = c.getEnvironment().getConfiguration();
        try (Connection conn  = QueryUtil.getConnectionOnServer(conf)) {
            // CDC will be enabled on Phoenix tables only
            PTable phoenixTable = getPhoenixTable(conn, regionInfoA.getTable());
            if (phoenixTable == null) {
                LOGGER.info("{} is not a Phoenix Table, skipping partition metadata update.",
                        regionInfoA.getTable());
                return;
            }
            // find streamName with ENABLED status
            String tableName = phoenixTable.getName().getString();
            PreparedStatement pstmt = conn.prepareStatement(STREAM_STATUS_QUERY);
            pstmt.setString(1, tableName);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                String streamName = rs.getString(1);
                LOGGER.info("Updating partition metadata for table={}, stream={} daughters {} {}",
                        tableName, streamName, regionInfoA.getEncodedName(), regionInfoB.getEncodedName());
                String parentPartitionID = getParentPartitionId(conn, tableName, streamName, regionInfoA, regionInfoB);
                upsertDaughterPartition(conn, tableName, streamName, parentPartitionID, regionInfoA);
                upsertDaughterPartition(conn, tableName, streamName, parentPartitionID, regionInfoB);
                updateParentPartitionEndTime(conn, tableName, streamName, parentPartitionID, regionInfoA.getRegionId());
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to update CDC Stream Partition metadata during split with daughter regions: {} {}",
                    regionInfoA.getEncodedName(), regionInfoB.getEncodedName(), e);
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

    /**
     * Lookup parent's partition id (region's encoded name) in SYSTEM.CDC_STREAM.
     * RegionInfoA is left daughter and RegionInfoB is right daughter so parent's key range would
     * be [RegionInfoA stratKey, RegionInfoB endKey]
     */
    private String getParentPartitionId(Connection conn, String tableName, String streamName,
                                        RegionInfo regionInfoA, RegionInfo regionInfoB)
            throws SQLException {
        byte[] parentStartKey = regionInfoA.getStartKey();
        byte[] parentEndKey = regionInfoB.getEndKey();

        StringBuilder qb = new StringBuilder(PARENT_PARTITION_QUERY);
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
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            return rs.getString(1);
        } else {
            throw new SQLException(String.format("Could not find parent of the provided daughters: "
                            + "startKeyA=%s endKeyA=%s startKeyB=%s endKeyB=%s",
                    Bytes.toStringBinary(regionInfoA.getStartKey()),
                    Bytes.toStringBinary(regionInfoA.getEndKey()),
                    Bytes.toStringBinary(regionInfoB.getStartKey()),
                    Bytes.toStringBinary(regionInfoB.getEndKey())));
        }
    }

    /**
     * Insert partition metadata for a daughter region from the split.
     */
    private void upsertDaughterPartition(Connection conn, String tableName,
                                         String streamName, String parentPartitionID,
                                         RegionInfo regionInfo)
            throws SQLException {
        String partitionId = regionInfo.getEncodedName();
        long startTime = regionInfo.getRegionId();
        byte[] startKey = regionInfo.getStartKey();
        byte[] endKey = regionInfo.getEndKey();
        PreparedStatement pstmt = conn.prepareStatement(PARTITION_UPSERT_SQL);
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
        conn.commit();
    }

    /**
     * Update parent partition's endTime by setting it to daughter's startTime.
     */
    private void updateParentPartitionEndTime(Connection conn, String tableName,
                                              String streamName, String parentPartitionID,
                                              long daughterStartTime) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(PARENT_PARTITION_UPDATE_END_TIME_SQL);
        pstmt.setString(1, tableName);
        pstmt.setString(2, streamName);
        pstmt.setString(3, parentPartitionID);
        pstmt.setLong(4, daughterStartTime);
        pstmt.executeUpdate();
        conn.commit();
    }
}
