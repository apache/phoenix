package org.apache.phoenix.coprocessor.tasks;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;
import static org.apache.phoenix.util.CDCUtil.CDC_STREAM_NAME_FORMAT;

/**
 * Task to bootstrap partition metadata when CDC is enabled on a table.
 * Upserts one row for each region of the table into SYSTEM.CDC_STREAM and marks the status as
 * ENABLED in SYSTEM.CDC_STREAM_STATUS.
 */
public class CdcStreamPartitionMetadataTask extends BaseTask  {

    public static final Logger LOGGER = LoggerFactory.getLogger(CdcStreamPartitionMetadataTask.class);
    public static String CDC_STREAM_STATUS_UPSERT_SQL
            = "UPSERT INTO " + SYSTEM_CDC_STREAM_STATUS_NAME + " VALUES (?, ?, ?)";

    // parent_partition_id will be null, set partition_end_time to -1
    private static String CDC_STREAM_PARTITION_UPSERT_SQL
            = "UPSERT INTO " + SYSTEM_CDC_STREAM_NAME + " VALUES (?,?,?,null,?,-1,?,?)";

    @Override
    public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
        PhoenixConnection pconn = null;
        String tableName = taskRecord.getTableName();
        String streamName = taskRecord.getSchemaName();
        Timestamp timestamp = taskRecord.getTimeStamp();
        try {
            pconn = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
            List<HRegionLocation> tableRegions = pconn.getQueryServices().getAllTableRegions(tableName.getBytes());
            upsertPartitionMetadata(pconn, tableName, streamName, tableRegions);
            updateStreamStatus(pconn, tableName, streamName);
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS, "");
        } catch (Throwable t) {
            LOGGER.error("Exception while bootstrapping CDC Stream Partition Metadata for " +
                    taskRecord.getTableName() +
                    " and timestamp " + timestamp.toString(), t);
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
        PreparedStatement ps = pconn.prepareStatement(CDC_STREAM_STATUS_UPSERT_SQL);
        ps.setString(1, tableName);
        ps.setString(2, streamName);
        ps.setString(3, CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue());
        ps.executeUpdate();
        pconn.commit();
        LOGGER.info("Marked stream {} for table {} as ENABLED", streamName, tableName);
    }

    private void upsertPartitionMetadata(PhoenixConnection pconn, String tableName,
                                         String streamName, List<HRegionLocation> tableRegions)
            throws SQLException {
        PreparedStatement ps = pconn.prepareStatement(CDC_STREAM_PARTITION_UPSERT_SQL);
        for (HRegionLocation tableRegion : tableRegions) {
            RegionInfo ri = tableRegion.getRegionInfo();
            ps.setString(1, tableName);
            ps.setString(2, streamName);
            ps.setString(3, ri.getEncodedName());
            ps.setLong(4, ri.getRegionId());
            ps.setBytes(5, ri.getStartKey());
            ps.setBytes(6, ri.getEndKey());
            ps.executeUpdate();
        }
        pconn.commit();
        LOGGER.info("Upserted {} partition metadata rows for table : {}, stream: {}",
                tableRegions.size(), tableName, streamName);
    }
}
