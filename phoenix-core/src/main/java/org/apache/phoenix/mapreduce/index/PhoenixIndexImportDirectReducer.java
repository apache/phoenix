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
package org.apache.phoenix.mapreduce.index;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexVerifyType;

/**
 * Reducer class that does only one task and that is to update the index state of the table.
 */
public class PhoenixIndexImportDirectReducer extends
        Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable> {
    private AtomicBoolean calledOnce = new AtomicBoolean(false);
    private IndexVerificationResultRepository resultRepository;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixIndexImportDirectReducer.class);
    private String indexTableName;
    private byte[] indexTableNameBytes;

    private void updateCounters(IndexTool.IndexVerifyType verifyType,
                                Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable>.Context context)
            throws IOException {
        Configuration configuration = context.getConfiguration();
        try (final Connection connection = ConnectionUtil.getInputConnection(configuration)) {
            long ts = Long.parseLong(configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE));
            IndexToolVerificationResult verificationResult =
                    resultRepository.getVerificationResult(connection, ts, indexTableNameBytes);
            context.getCounter(PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT).
                    setValue(verificationResult.getScannedDataRowCount());
            context.getCounter(PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT).
                    setValue(verificationResult.getRebuiltIndexRowCount());
            if (verifyType == IndexTool.IndexVerifyType.ONLY || verifyType == IndexTool.IndexVerifyType.BEFORE ||
                    verifyType == IndexTool.IndexVerifyType.BOTH) {
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildValidIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildExpiredIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildMissingIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildInvalidIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildBeyondMaxLookBackMissingIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildBeyondMaxLookBackInvalidIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS).
                        setValue(verificationResult.getBeforeIndexHasExtraCellsCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS).
                        setValue(verificationResult.getBeforeIndexHasMissingCellsCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildUnverifiedIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_OLD_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildOldIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRebuildUnknownIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRepairExtraVerifiedIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT).
                        setValue(verificationResult.getBeforeRepairExtraUnverifiedIndexRowCount());
            }
            if (verifyType == IndexTool.IndexVerifyType.BOTH || verifyType == IndexTool.IndexVerifyType.AFTER) {
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REBUILD_VALID_INDEX_ROW_COUNT).
                        setValue(verificationResult.getAfterRebuildValidIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT).
                        setValue(verificationResult.getAfterRebuildExpiredIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT).
                        setValue(verificationResult.getAfterRebuildMissingIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT).
                        setValue(verificationResult.getAfterRebuildInvalidIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).
                        setValue(verificationResult.getAfterRebuildBeyondMaxLookBackMissingIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).
                        setValue(verificationResult.getAfterRebuildBeyondMaxLookBackInvalidIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS).
                        setValue(verificationResult.getAfterIndexHasExtraCellsCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS).
                        setValue(verificationResult.getAfterIndexHasMissingCellsCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT).
                    setValue(verificationResult.getAfterRepairExtraVerifiedIndexRowCount());
                context.getCounter(PhoenixIndexToolJobCounters.AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT).
                    setValue(verificationResult.getAfterRepairExtraUnverifiedIndexRowCount());
            }
            if (verificationResult.isVerificationFailed()) {
                throw new IOException("Index verification failed! " + verificationResult);
            }
        } catch (Exception e) {
            throw new IOException("Fail to get index verification result", e);
        }
    }

    @Override
    protected void setup(Context context) throws IOException {
        resultRepository = new IndexVerificationResultRepository();
        indexTableName = PhoenixConfigurationUtil.getPhysicalTableName(context.getConfiguration());
        indexTableNameBytes = Bytes.toBytes(indexTableName);
    }

    @Override
    protected void reduce(ImmutableBytesWritable arg0, Iterable<IntWritable> arg1,
                          Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable>.Context context)
            throws IOException, InterruptedException

    {
        if (!calledOnce.compareAndSet(false, true)) {
            return;
        }
        IndexTool.IndexVerifyType verifyType = getIndexVerifyType(context.getConfiguration());
        if (verifyType != IndexTool.IndexVerifyType.NONE) {
            updateCounters(verifyType, context);
        }

        if (verifyType != IndexTool.IndexVerifyType.ONLY) {
            // "ONLY" option should not turn index state to ACTIVE, as it doesn't rebuild the index
            try {
                IndexToolUtil.updateIndexState(context.getConfiguration(), PIndexState.ACTIVE);
            } catch (SQLException e) {
                LOGGER.error(" Failed to update the status to Active", e);
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        try {
            updateTasksTable(context);
            resultRepository.close();
        } catch (SQLException e) {
            LOGGER.error(" Failed to update the tasks table");
            throw new RuntimeException(e.getMessage());
        }
    }

    private void updateTasksTable(Context context) throws SQLException, IOException {
        final Properties overrideProps = new Properties();
        final Connection
                connection = ConnectionUtil
                .getOutputConnection(context.getConfiguration(), overrideProps);
        try {
            String fullTableName = PhoenixConfigurationUtil.getInputTableName(context.getConfiguration());
            String tenantId = context.getConfiguration().get(MAPREDUCE_TENANT_ID, null);
            String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
            String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
            String indexName = PhoenixConfigurationUtil.getDisableIndexes(context.getConfiguration());
            List<Task.TaskRecord> taskRecords = Task.queryTaskTable(connection, null, schemaName, tableName,
                    PTable.TaskType.INDEX_REBUILD, tenantId, indexName);
            if (taskRecords != null && taskRecords.size() > 0) {
                for (Task.TaskRecord taskRecord : taskRecords) {
                    TaskRegionObserver.SelfHealingTask.setEndTaskStatus(
                            connection.unwrap(PhoenixConnection.class), taskRecord,
                            PTable.TaskStatus.COMPLETED.toString());
                }
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
