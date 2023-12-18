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

import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerViewUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;

/**
 * Task runs periodically to clean up task of child views whose parent is dropped
 *
 */
public class DropChildViewsTask extends BaseTask {
    public static final Logger LOGGER = LoggerFactory.getLogger(DropChildViewsTask.class);

    public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
        PhoenixConnection pconn = null;
        Timestamp timestamp = taskRecord.getTimeStamp();
        try {
            String tenantId = taskRecord.getTenantId();
            if (tenantId != null) {
                Properties tenantProps = new Properties();
                tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                pconn = QueryUtil.getConnectionOnServer(tenantProps, env.getConfiguration())
                        .unwrap(PhoenixConnection.class);
            }
            else {
                pconn = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
            }

            MetaDataProtocol.MetaDataMutationResult result = new MetaDataClient(pconn).updateCache(pconn.getTenantId(),
                    taskRecord.getSchemaName(), taskRecord.getTableName(), true);
            if (result.getMutationCode() != MetaDataProtocol.MutationCode.TABLE_ALREADY_EXISTS) {
                ServerViewUtil.dropChildViews(env, taskRecord.getTenantIdBytes(),
                        taskRecord.getSchemaNameBytes(), taskRecord.getTableNameBytes(),
                        SchemaUtil.getPhysicalTableName(
                                SYSTEM_CHILD_LINK_NAME_BYTES,
                                env.getConfiguration()).getName());
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS, "");
            } else if (EnvironmentEdgeManager.currentTimeMillis() < timeMaxInterval + timestamp.getTime()) {
                // skip this task as it has not been expired and its parent table has not been dropped yet
                LOGGER.info("Skipping a child view drop task. " +
                        "The parent table has not been dropped yet : " +
                        taskRecord.getSchemaName() + "." + taskRecord.getTableName() +
                        " with tenant id " + (tenantId == null ? " IS NULL" : tenantId) +
                        " and timestamp " + timestamp.toString());
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED, "");
            }
            else {
                LOGGER.warn(" A drop child view task has expired and will be marked as failed : " +
                        taskRecord.getSchemaName() + "." + taskRecord.getTableName() +
                        " with tenant id " + (tenantId == null ? " IS NULL" : tenantId) +
                        " and timestamp " + timestamp.toString());
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, "Expired");
            }
        }
        catch (Throwable t) {
            LOGGER.error("Exception while dropping a child view task. " +
                    taskRecord.getSchemaName()  + "." + taskRecord.getTableName() +
                    " with tenant id " + (taskRecord.getTenantId() == null ? " IS NULL" : taskRecord.getTenantId()) +
                    " and timestamp " + timestamp.toString(), t);
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, t.toString());
        } finally {
            if (pconn != null) {
                try {
                    pconn.close();
                } catch (SQLException ignored) {
                    LOGGER.debug("DropChildViewsTask can't close connection", ignored);
                }
            }
        }
    }

    public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord) throws Exception {
        return null;
    }
}
