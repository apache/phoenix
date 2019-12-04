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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID;

/**
 * Reducer class that does only one task and that is to update the index state of the table.
 */
public class PhoenixIndexImportDirectReducer extends
        Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixIndexImportDirectReducer.class);

    private static final String SPLIT_MERGE_ERROR_STR_FMT =
            "Data table split/merge happened during Index rebuild. Old region cnt=%d, New region cnt = %d. "
                    + "Changed regions=%s";

    private static boolean splitRegionForTesting = false;

    // Call this only from test code
    public static void setSplitRegionForTesting(boolean val) {
        splitRegionForTesting = val;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        if (PhoenixConfigurationUtil.getFailOnRegionNumChange(context.getConfiguration())) {
            String strNumOfRegions = PhoenixConfigurationUtil.getNumOfRegions(context.getConfiguration());
            if (strNumOfRegions != null) {
                // Check if we have a region split/merge. Different numOfRegions requires we need to run IndexTool again
                String fullTableName = PhoenixConfigurationUtil.getIndexToolDataTableName(context.getConfiguration());
                final Properties overrideProps = new Properties();
                try {
                    final Connection
                            connection =
                            ConnectionUtil
                                    .getOutputConnection(context.getConfiguration(), overrideProps);
                    if (splitRegionForTesting) {
                        try (Admin admin = connection.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                            TableName tbl = TableName.valueOf(fullTableName);
                            admin.split(tbl);
                            // make sure the split finishes (there's no synchronous splitting before HBase 2.x)
                            admin.disableTable(tbl);
                            admin.enableTable(tbl);
                        }
                    }
                    int newNumRegions = IndexUtil.getRegions(connection, fullTableName).size();
                    int prevNumRegions = Integer.parseInt(strNumOfRegions);
                    if (newNumRegions != prevNumRegions) {
                        detectChangedRegions(connection, context, fullTableName);
                    }
                } catch (SQLException e) {
                    LOGGER.error("Failed get the number of regions");
                    throw new RuntimeException(e.getMessage());
                }
            }
        }

        try {
            IndexToolUtil.updateIndexState(context.getConfiguration(), PIndexState.ACTIVE);

            updateTasksTable(context);
        } catch (SQLException e) {
            LOGGER.error(" Failed to update the status to Active");
            throw new RuntimeException(e.getMessage());
        }
    }

    private void  detectChangedRegions(Connection connection, Context context, String fullTableName)
            throws SQLException, IOException {
        String prevRegionListStr = PhoenixConfigurationUtil.getRegions(context.getConfiguration());
        List<String> prevRegions = Arrays.asList(prevRegionListStr.split(IndexUtil.REGION_NAME_STRING));
        List<String> currentRegions = IndexUtil.getRegionsAsString(connection, fullTableName);

        List<String> changedRegions = new ArrayList<>();
        for (String prev : prevRegions) {
            if (prev.length() == 0) continue;
            String oldRegionName = prev.substring(0, prev.indexOf(IndexUtil.START_KEY_STRING));
            boolean regionFound = false;
            for (String cur : currentRegions) {
                String curRegionName = cur.substring(IndexUtil.REGION_NAME_STRING.length(), cur.indexOf(IndexUtil.START_KEY_STRING));
                if (curRegionName.equals(oldRegionName)) {
                    regionFound = true;
                    break;
                }
            }
            if (!regionFound) {
                changedRegions.add(prev);
            }
        }

        throw new IOException(
                String.format(SPLIT_MERGE_ERROR_STR_FMT, prevRegions.size(),
                        currentRegions.size(), changedRegions));
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
