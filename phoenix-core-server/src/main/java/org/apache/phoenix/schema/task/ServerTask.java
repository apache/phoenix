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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable;

public class ServerTask extends Task {
    private static void mutateSystemTaskTable(PhoenixConnection conn, PreparedStatement stmt, boolean accessCheckEnabled)
            throws IOException {
        // we need to mutate SYSTEM.TASK with HBase/login user if access is enabled.
        if (accessCheckEnabled) {
            User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    final RpcCall rpcContext = RpcUtil.getRpcContext();
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

    /**
     * Execute and commit upsert query on SYSTEM.TASK
     * This method should be used only from server side. Client should use
     * {@link #getMutationsForAddTask(SystemTaskParams)} instead of direct
     * upsert commit.
     *
     * @param systemTaskParams Task params with various task related arguments
     * @throws IOException If something goes wrong while preparing mutations
     *     or committing transactions
     */
    public static void addTask(SystemTaskParams systemTaskParams)
            throws IOException {
        addTaskAndGetStatement(systemTaskParams, systemTaskParams.getConn(),
                true);
    }

    private static PreparedStatement addTaskAndGetStatement(
            SystemTaskParams systemTaskParams, PhoenixConnection connection,
            boolean shouldCommit) throws IOException {
        PreparedStatement stmt = addTaskAndGetStatement(systemTaskParams, connection);

        // if query is getting executed by client, do not execute and commit
        // mutations
        if (shouldCommit) {
            mutateSystemTaskTable(connection, stmt,
                    systemTaskParams.isAccessCheckEnabled());
        }
        return stmt;
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
}
