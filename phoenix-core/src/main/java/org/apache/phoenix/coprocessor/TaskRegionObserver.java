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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import java.util.Optional;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.TaskType;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;


/**
 * Coprocessor for task related operations. This coprocessor would only be registered
 * to SYSTEM.TASK table
 */

public class TaskRegionObserver implements RegionObserver, RegionCoprocessor {
    public static final Log LOG = LogFactory.getLog(TaskRegionObserver.class);
    protected ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(TaskType.values().length);
    private long timeInterval = QueryServicesOptions.DEFAULT_TASK_HANDLING_INTERVAL_MS;
    private long timeMaxInterval = QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS;
    @GuardedBy("TaskRegionObserver.class")
    // initial delay before the first task is handled
    private static final long  initialDelay = 10000; // 10 secs

    @Override
    public void preClose(final ObserverContext<RegionCoprocessorEnvironment> c,
            boolean abortRequested) {
        executor.shutdownNow();
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        Configuration config = env.getConfiguration();
        timeInterval =
                config.getLong(
                    QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                    QueryServicesOptions.DEFAULT_TASK_HANDLING_INTERVAL_MS);
        timeMaxInterval =
                config.getLong(
                        QueryServices.TASK_HANDLING_MAX_INTERVAL_MS_ATTRIB,
                        QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        final RegionCoprocessorEnvironment env = e.getEnvironment();

        // turn off verbose deprecation logging
        Logger deprecationLogger = Logger.getLogger("org.apache.hadoop.conf.Configuration.deprecation");
        if (deprecationLogger != null) {
            deprecationLogger.setLevel(Level.WARN);
        }

        DropChildViewsTask task = new DropChildViewsTask(e.getEnvironment(), timeMaxInterval);
        executor.scheduleWithFixedDelay(task, initialDelay, timeInterval, TimeUnit.MILLISECONDS);
    }

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

    public static void addTask(PhoenixConnection conn, TaskType taskType, String tenantId, String schemaName,
                               String tableName, boolean accessCheckEnabled)
            throws IOException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("UPSERT INTO " +
                    PhoenixDatabaseMetaData.SYSTEM_TASK_NAME + " ( " +
                    PhoenixDatabaseMetaData.TASK_TYPE + ", " +
                    PhoenixDatabaseMetaData.TENANT_ID + ", " +
                    PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                    PhoenixDatabaseMetaData.TABLE_NAME + " ) VALUES(?,?,?,?)");
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
        } catch (SQLException e) {
            throw new IOException(e);
        }
        mutateSystemTaskTable(conn, stmt, accessCheckEnabled);
    }

    public static void deleteTask(PhoenixConnection conn, TaskType taskType, Timestamp ts, String tenantId,
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

    /**
     * Task runs periodically to clean up task of child views whose parent is dropped
     *
     */
    public static class DropChildViewsTask extends TimerTask {
        private RegionCoprocessorEnvironment env;
        private long timeMaxInterval;
        private boolean accessCheckEnabled;

        public DropChildViewsTask(RegionCoprocessorEnvironment env, long timeMaxInterval) {
            this.env = env;
            this.accessCheckEnabled = env.getConfiguration().getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                    QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
            this.timeMaxInterval = timeMaxInterval;
        }

        @Override
        public void run() {
            PhoenixConnection connForTask = null;
            Timestamp timestamp = null;
            String tenantId = null;
            byte[] tenantIdBytes;
            String schemaName= null;
            byte[] schemaNameBytes;
            String tableName = null;
            byte[] tableNameBytes;
            PhoenixConnection pconn;
            try {
                String taskQuery = "SELECT " +
                        PhoenixDatabaseMetaData.TASK_TS + ", " +
                        PhoenixDatabaseMetaData.TENANT_ID + ", " +
                        PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                        PhoenixDatabaseMetaData.TABLE_NAME +
                        " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME +
                        " WHERE "+ PhoenixDatabaseMetaData.TASK_TYPE + " = " + PTable.TaskType.DROP_CHILD_VIEWS.getSerializedValue();

                connForTask = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
                PreparedStatement taskStatement = connForTask.prepareStatement(taskQuery);
                ResultSet rs = taskStatement.executeQuery();
                while (rs.next()) {
                    try {
                        // delete child views only if the parent table is deleted from the system catalog
                        timestamp = rs.getTimestamp(1);
                        tenantId = rs.getString(2);
                        tenantIdBytes= rs.getBytes(2);
                        schemaName= rs.getString(3);
                        schemaNameBytes = rs.getBytes(3);
                        tableName= rs.getString(4);
                        tableNameBytes = rs.getBytes(4);

                        if (tenantId != null) {
                            Properties tenantProps = new Properties();
                            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                            pconn = QueryUtil.getConnectionOnServer(tenantProps, env.getConfiguration()).unwrap(PhoenixConnection.class);

                        }
                        else {
                            pconn = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
                        }

                        MetaDataProtocol.MetaDataMutationResult result = new MetaDataClient(pconn).updateCache(pconn.getTenantId(),
                                schemaName, tableName, true);
                        if (result.getMutationCode() != MetaDataProtocol.MutationCode.TABLE_ALREADY_EXISTS) {
                            MetaDataEndpointImpl.dropChildViews(env, tenantIdBytes, schemaNameBytes, tableNameBytes);
                        } else if (System.currentTimeMillis() < timeMaxInterval + timestamp.getTime()) {
                            // skip this task as it has not been expired and its parent table has not been dropped yet
                            LOG.info("Skipping a child view drop task. The parent table has not been dropped yet : " +
                                    schemaName + "." + tableName +
                                    " with tenant id " + (tenantId == null ? " IS NULL" : tenantId) +
                                    " and timestamp " + timestamp.toString());
                            continue;
                        }
                        else {
                            LOG.warn(" A drop child view task has expired and will be removed from the system task table : " +
                                    schemaName + "." + tableName +
                                    " with tenant id " + (tenantId == null ? " IS NULL" : tenantId) +
                                    " and timestamp " + timestamp.toString());
                        }

                        deleteTask(connForTask, PTable.TaskType.DROP_CHILD_VIEWS, timestamp, tenantId, schemaName,
                                tableName, this.accessCheckEnabled);
                    }
                    catch (Throwable t) {
                        LOG.warn("Exception while dropping a child view task. " +
                                "It will be retried in the next system task table scan : " +
                                schemaName + "." + tableName +
                                " with tenant id " + (tenantId == null ? " IS NULL" : tenantId) +
                                " and timestamp " + timestamp.toString(), t);
                    }
                }
            } catch (Throwable t) {
                LOG.error("DropChildViewsTask failed!", t);
            } finally {
                if (connForTask != null) {
                    try {
                        connForTask.close();
                    } catch (SQLException ignored) {
                        LOG.debug("DropChildViewsTask can't close connection", ignored);
                    }
                }
            }
        }
    }
}
