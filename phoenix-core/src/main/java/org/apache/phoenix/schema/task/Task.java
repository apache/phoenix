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
package org.apache.phoenix.schema.task;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.phoenix.coprocessor.tasks.DropChildViewsTask;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class Task {
    public static final Logger LOGGER = LoggerFactory.getLogger(Task.class);
    private static void mutateSystemTaskTable(final PhoenixConnection conn, final PreparedStatement stmt, boolean accessCheckEnabled)
            throws IOException {
        // we need to mutate SYSTEM.TASK with HBase/login user if access is enabled.
        if (accessCheckEnabled) {
            User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    final RpcServer.Call rpcContext = RpcUtil.getRpcContext();
                    // setting RPC context as null so that user can be reset
                    try {
                        RpcUtil.setRpcContext(null);
                        stmt.execute();
                        conn.commit();
                    } catch (SQLException e) {
                        throw new IOException(e);
                    } finally {
                        // setting RPC context back to original context of the RPC
                        RpcUtil.setRpcContext(rpcContext);
                    }
                    return null;
                }
            });
        }
        else {
            try {
                stmt.execute();
                conn.commit();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

    private static  PreparedStatement setValuesToAddTaskPS(PreparedStatement stmt, PTable.TaskType taskType,
            String tenantId, String schemaName, String tableName, String taskStatus, String data,
            Integer priority, Timestamp startTs, Timestamp endTs) throws SQLException {
        stmt.setByte(1, taskType.getSerializedValue());
        if (tenantId != null) {
            stmt.setString(2, tenantId);
        } else {
            stmt.setNull(2, Types.VARCHAR);
        }
        if (schemaName != null) {
            stmt.setString(3, schemaName);
        } else {
            stmt.setNull(3, Types.VARCHAR);
        }
        stmt.setString(4, tableName);
        if (taskStatus != null) {
            stmt.setString(5, taskStatus);
        } else {
            stmt.setString(5, PTable.TaskStatus.CREATED.toString());
        }
        if (priority != null) {
            stmt.setInt(6, priority);
        } else {
            byte defaultPri = 4;
            stmt.setInt(6, defaultPri);
        }
        if (startTs == null) {
            startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
        }
        stmt.setTimestamp(7, startTs);
        if (endTs != null) {
            stmt.setTimestamp(8, endTs);
        } else {
            if (taskStatus != null && taskStatus.equals(PTable.TaskStatus.COMPLETED.toString())) {
                endTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
                stmt.setTimestamp(8, endTs);
            } else {
                stmt.setNull(8, Types.TIMESTAMP);
            }
        }
        if (data != null) {
            stmt.setString(9, data);
        } else {
            stmt.setNull(9, Types.VARCHAR);
        }
        return stmt;
    }

    public static void addTask(PhoenixConnection conn, PTable.TaskType taskType, String tenantId, String schemaName,
            String tableName, String taskStatus, String data, Integer priority, Timestamp startTs, Timestamp endTs,
            boolean accessCheckEnabled)
            throws IOException {
        PreparedStatement stmt;
        try {
            stmt = conn.prepareStatement("UPSERT INTO " +
                    PhoenixDatabaseMetaData.SYSTEM_TASK_NAME + " ( " +
                    PhoenixDatabaseMetaData.TASK_TYPE + ", " +
                    PhoenixDatabaseMetaData.TENANT_ID + ", " +
                    PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                    PhoenixDatabaseMetaData.TABLE_NAME + ", " +
                    PhoenixDatabaseMetaData.TASK_STATUS + ", " +
                    PhoenixDatabaseMetaData.TASK_PRIORITY + ", " +
                    PhoenixDatabaseMetaData.TASK_TS + ", " +
                    PhoenixDatabaseMetaData.TASK_END_TS + ", " +
                    PhoenixDatabaseMetaData.TASK_DATA +
                    " ) VALUES(?,?,?,?,?,?,?,?,?)");
            stmt = setValuesToAddTaskPS(stmt, taskType, tenantId, schemaName, tableName, taskStatus, data, priority, startTs, endTs);
            LOGGER.info("Adding task " + taskType + "," +tableName + "," + taskStatus + "," + startTs, ","+endTs);
        } catch (SQLException e) {
            throw new IOException(e);
        }
        mutateSystemTaskTable(conn, stmt, accessCheckEnabled);
    }

    public static void deleteTask(PhoenixConnection conn, PTable.TaskType taskType, Timestamp ts, String tenantId,
            String schemaName, String tableName, boolean accessCheckEnabled) throws IOException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("DELETE FROM " +
                    PhoenixDatabaseMetaData.SYSTEM_TASK_NAME +
                    " WHERE " + PhoenixDatabaseMetaData.TASK_TYPE + " = ? AND " +
                    PhoenixDatabaseMetaData.TASK_TS + " = ? AND " +
                    PhoenixDatabaseMetaData.TENANT_ID + (tenantId == null ? " IS NULL " : " = '" + tenantId + "'") + " AND " +
                    PhoenixDatabaseMetaData.TABLE_SCHEM + (schemaName == null ? " IS NULL " : " = '" + schemaName + "'") + " AND " +
                    PhoenixDatabaseMetaData.TABLE_NAME + " = ?");
            stmt.setByte(1, taskType.getSerializedValue());
            stmt.setTimestamp(2, ts);
            stmt.setString(3, tableName);
        } catch (SQLException e) {
            throw new IOException(e);
        }
        mutateSystemTaskTable(conn, stmt, accessCheckEnabled);
    }

    private static List<TaskRecord> populateTasks(Connection connection, String taskQuery)
            throws SQLException {
        PreparedStatement taskStatement = connection.prepareStatement(taskQuery);
        ResultSet rs = taskStatement.executeQuery();

        List<TaskRecord> result = new ArrayList<>();
        while (rs.next()) {
            // delete child views only if the parent table is deleted from the system catalog
            TaskRecord taskRecord = parseResult(rs);
            result.add(taskRecord);
        }
        return result;
    }

    public static List<TaskRecord> queryTaskTable(Connection connection, Timestamp ts,
            String schema, String tableName,
            PTable.TaskType taskType, String tenantId, String indexName)
            throws SQLException {
        String taskQuery = "SELECT " +
                PhoenixDatabaseMetaData.TASK_TS + ", " +
                PhoenixDatabaseMetaData.TENANT_ID + ", " +
                PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                PhoenixDatabaseMetaData.TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.TASK_STATUS + ", " +
                PhoenixDatabaseMetaData.TASK_TYPE + ", " +
                PhoenixDatabaseMetaData.TASK_PRIORITY + ", " +
                PhoenixDatabaseMetaData.TASK_DATA +
                " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME;
            taskQuery += " WHERE " +
                    PhoenixDatabaseMetaData.TABLE_NAME + " ='" + tableName + "' AND " +
                    PhoenixDatabaseMetaData.TASK_TYPE + "=" + taskType.getSerializedValue();
            if (!Strings.isNullOrEmpty(tenantId)) {
                taskQuery += " AND " + PhoenixDatabaseMetaData.TENANT_ID + "='" + tenantId + "' ";
            }

            if (!Strings.isNullOrEmpty(schema)) {
                taskQuery += " AND " + PhoenixDatabaseMetaData.TABLE_SCHEM + "='" + schema + "' ";
            }

            if (!Strings.isNullOrEmpty(indexName)) {
                taskQuery += " AND " + PhoenixDatabaseMetaData.TASK_DATA + " LIKE '%" + indexName + "%'";
            }

            List<TaskRecord> taskRecords = populateTasks(connection, taskQuery);
            List<TaskRecord> result = new ArrayList<TaskRecord>();
            if (ts != null) {
                // Adding TASK_TS to the where clause did not work. It returns empty when directly querying with the timestamp.
                for (TaskRecord tr : taskRecords) {
                    if (tr.getTimeStamp().equals(ts)) {
                        result.add(tr);
                    }
                }
            } else {
                result = taskRecords;
            }

        return result;
    }

    public static List<TaskRecord> queryTaskTable(Connection connection, String[] excludedTaskStatus)
            throws SQLException {
        String taskQuery = "SELECT " +
                PhoenixDatabaseMetaData.TASK_TS + ", " +
                PhoenixDatabaseMetaData.TENANT_ID + ", " +
                PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                PhoenixDatabaseMetaData.TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.TASK_STATUS + ", " +
                PhoenixDatabaseMetaData.TASK_TYPE + ", " +
                PhoenixDatabaseMetaData.TASK_PRIORITY + ", " +
                PhoenixDatabaseMetaData.TASK_DATA +
                " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME;
        if (excludedTaskStatus != null && excludedTaskStatus.length > 0) {
            taskQuery += " WHERE " + PhoenixDatabaseMetaData.TASK_STATUS + " IS NULL OR " +
            PhoenixDatabaseMetaData.TASK_STATUS + " NOT IN (";
            String[] values = new String[excludedTaskStatus.length];
            for (int i=0; i < excludedTaskStatus.length; i++) {
                values[i] = String.format("'%s'", excludedTaskStatus[i].trim());
            }

            //Delimit with comma
            taskQuery += String.join(",", values);
            taskQuery += ")";
        }

        return populateTasks(connection, taskQuery);
    }

    public static TaskRecord parseResult(ResultSet rs) throws SQLException {
        TaskRecord taskRecord = new TaskRecord();
        taskRecord.setTimeStamp(rs.getTimestamp(PhoenixDatabaseMetaData.TASK_TS));
        taskRecord.setTenantId(rs.getString(PhoenixDatabaseMetaData.TENANT_ID));
        taskRecord.setTenantIdBytes(rs.getBytes(PhoenixDatabaseMetaData.TENANT_ID));
        taskRecord.setSchemaName(rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM));
        taskRecord.setSchemaNameBytes(rs.getBytes(PhoenixDatabaseMetaData.TABLE_SCHEM));
        taskRecord.setTableName(rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
        taskRecord.setTableNameBytes(rs.getBytes(PhoenixDatabaseMetaData.TABLE_NAME));
        taskRecord.setStatus(rs.getString(PhoenixDatabaseMetaData.TASK_STATUS));
        taskRecord.setTaskType(PTable.TaskType.fromSerializedValue(rs.getByte(PhoenixDatabaseMetaData.TASK_TYPE )));
        taskRecord.setPriority(rs.getInt(PhoenixDatabaseMetaData.TASK_PRIORITY));
        taskRecord.setData(rs.getString(PhoenixDatabaseMetaData.TASK_DATA));
        return taskRecord;
    }

    public static class TaskRecord {
        private String tenantId;
        private Timestamp timeStamp;
        private byte[] tenantIdBytes;
        private String schemaName= null;
        private byte[] schemaNameBytes;
        private String tableName = null;
        private byte[] tableNameBytes;

        private PTable.TaskType taskType;
        private String status;
        private int priority;
        private String data;

        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
        }

        public Timestamp getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(Timestamp timeStamp) {
            this.timeStamp = timeStamp;
        }

        public byte[] getTenantIdBytes() {
            return tenantIdBytes;
        }

        public void setTenantIdBytes(byte[] tenantIdBytes) {
            this.tenantIdBytes = tenantIdBytes;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public byte[] getSchemaNameBytes() {
            return schemaNameBytes;
        }

        public void setSchemaNameBytes(byte[] schemaNameBytes) {
            this.schemaNameBytes = schemaNameBytes;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public byte[] getTableNameBytes() {
            return tableNameBytes;
        }

        public void setTableNameBytes(byte[] tableNameBytes) {
            this.tableNameBytes = tableNameBytes;
        }

        public String getData() {
            if (data == null) {
                return "";
            }
            return data;
        }

        public int getPriority() {
            return priority;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        public void setData(String data) {
            this.data = data;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public PTable.TaskType getTaskType() {
            return taskType;
        }

        public void setTaskType(PTable.TaskType taskType) {
            this.taskType = taskType;
        }

    }
}
