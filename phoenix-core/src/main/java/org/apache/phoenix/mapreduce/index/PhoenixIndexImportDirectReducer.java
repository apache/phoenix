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
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID;

/**
 * Reducer class that does only one task and that is to update the index state of the table.
 */
public class PhoenixIndexImportDirectReducer extends
        Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixIndexImportDirectReducer.class);

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        try {
            IndexToolUtil.updateIndexState(context.getConfiguration(), PIndexState.ACTIVE);

            updateTasksTable(context);
        } catch (SQLException e) {
            logger.error(" Failed to update the status to Active");
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
            List<Task.TaskRecord> taskRecords = Task.queryTaskTable(connection, schemaName, tableName,
                    PTable.TaskType.INDEX_REBUILD, tenantId, indexName);
            if (taskRecords != null && taskRecords.size() > 0) {
                for (Task.TaskRecord taskRecord : taskRecords) {
                    TaskRegionObserver.SelfHealingTask.setEndTaskStatus(
                            connection.unwrap(PhoenixConnection.class), taskRecords.get(0),
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
