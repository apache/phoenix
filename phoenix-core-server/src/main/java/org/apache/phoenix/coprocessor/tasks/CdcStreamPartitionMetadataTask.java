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

package org.apache.phoenix.coprocessor.tasks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.ServerTask;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;
import static org.apache.phoenix.query.QueryServices.PHOENIX_STREAMS_GET_TABLE_REGIONS_TIMEOUT;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_PHOENIX_STREAMS_GET_TABLE_REGIONS_TIMEOUT;

/**
 * Task to bootstrap partition metadata when CDC is enabled on a table.
 * Upserts one row for each region of the table into SYSTEM.CDC_STREAM and marks the status as
 * ENABLED in SYSTEM.CDC_STREAM_STATUS.
 */
public class CdcStreamPartitionMetadataTask extends BaseTask  {

    public static final Logger LOGGER = LoggerFactory.getLogger(CdcStreamPartitionMetadataTask.class);
    private static final String CDC_STREAM_STATUS_UPSERT_SQL
            = "UPSERT INTO " + SYSTEM_CDC_STREAM_STATUS_NAME + " VALUES (?, ?, ?)";

    private static final String CDC_STREAM_PARTITION_UPSERT_SQL
            = "UPSERT INTO " + SYSTEM_CDC_STREAM_NAME + " VALUES (?,?,?,?,?,?,?,?)";

    @Override
    public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
        Configuration conf = HBaseConfiguration.create(env.getConfiguration());
        Configuration configuration = HBaseConfiguration.addHbaseResources(conf);
        int getTableRegionsTimeout = configuration.getInt(PHOENIX_STREAMS_GET_TABLE_REGIONS_TIMEOUT,
                DEFAULT_PHOENIX_STREAMS_GET_TABLE_REGIONS_TIMEOUT);
        PhoenixConnection pconn = null;
        String tableName = taskRecord.getTableName();
        String streamName = taskRecord.getSchemaName();
        Timestamp timestamp = taskRecord.getTimeStamp();
        try {
            pconn = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
            List<HRegionLocation> tableRegions = pconn.getQueryServices().getAllTableRegions(
                    tableName.getBytes(), getTableRegionsTimeout);
            upsertPartitionMetadata(pconn, tableName, streamName, tableRegions);
            updateStreamStatus(pconn, tableName, streamName);
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS, "");
        } catch (SQLException e) {
            try {
                // Update task status to RETRY so that it is retried
                ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder()
                        .setConn(pconn)
                        .setTaskType(taskRecord.getTaskType())
                        .setSchemaName(taskRecord.getSchemaName())
                        .setTableName(taskRecord.getTableName())
                        .setTaskStatus(PTable.TaskStatus.RETRY.toString())
                        .setStartTs(taskRecord.getTimeStamp())
                        .setEndTs(null)
                        .build());
                LOGGER.warn("Marking task as RETRY. " +
                        "SQLException while bootstrapping CDC Stream Partition Metadata for "
                        + taskRecord.getTableName() + " and timestamp " + timestamp.toString(), e);
                return null;
            } catch (IOException ioe) {
                LOGGER.error("Unable to mark task as RETRY. " +
                        "SQLException while bootstrapping CDC Stream Partition Metadata for "
                        + taskRecord.getTableName() + " and timestamp " + timestamp.toString(), e);
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, e.toString());
            }
        } catch (Throwable t) {
            LOGGER.error("Marking task as FAIL. Exception while bootstrapping CDC Stream Partition Metadata for "
                    + taskRecord.getTableName() + " and timestamp " + timestamp.toString(), t);
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, t.toString());
        } finally {
            if (pconn != null) {
                try {
                    pconn.close();
                } catch (SQLException ignored) {
                    LOGGER.debug("CdcStreamPartitionMetadataTask can't close connection", ignored);
                }
            }
        }
    }

    @Override
    public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord)
            throws Exception {
        return null;
    }

    private void updateStreamStatus(PhoenixConnection pconn, String tableName, String streamName)
            throws SQLException {
        try (PreparedStatement ps = pconn.prepareStatement(CDC_STREAM_STATUS_UPSERT_SQL)) {
            ps.setString(1, tableName);
            ps.setString(2, streamName);
            ps.setString(3, CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue());
            ps.executeUpdate();
            pconn.commit();
            LOGGER.info("Marked stream {} for table {} as ENABLED", streamName, tableName);
        }
    }

    private void upsertPartitionMetadata(PhoenixConnection pconn, String tableName,
                                         String streamName, List<HRegionLocation> tableRegions)
            throws SQLException {
        try (PreparedStatement ps = pconn.prepareStatement(CDC_STREAM_PARTITION_UPSERT_SQL)) {
            for (HRegionLocation tableRegion : tableRegions) {
                // set parent_partition_id, partition_end_time to null
                RegionInfo ri = tableRegion.getRegion();
                ps.setString(1, tableName);
                ps.setString(2, streamName);
                ps.setString(3, ri.getEncodedName());
                ps.setNull(4, Types.VARCHAR);
                ps.setLong(5, ri.getRegionId());
                ps.setNull(6, Types.BIGINT);
                ps.setBytes(7, ri.getStartKey());
                ps.setBytes(8, ri.getEndKey());
                ps.executeUpdate();
            }
            pconn.commit();
            LOGGER.info("Upserted {} partition metadata rows for table : {}, stream: {}",
                    tableRegions.size(), tableName, streamName);
        }
    }
}
