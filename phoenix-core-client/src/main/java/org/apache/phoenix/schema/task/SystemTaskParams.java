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

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;

import java.sql.Timestamp;

/**
 * Task params to be used while upserting records in SYSTEM.TASK table.
 * This POJO is mainly used while upserting(and committing) or generating
 * upsert mutations plan in {@link Task} class
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
    justification = "endTs and startTs are not used for mutation")
public class SystemTaskParams {

    private final PhoenixConnection conn;
    private final PTable.TaskType taskType;
    private final String tenantId;
    private final String schemaName;
    private final String tableName;
    private final String taskStatus;
    private final String data;
    private final Integer priority;
    private final Timestamp startTs;
    private final Timestamp endTs;
    private final boolean accessCheckEnabled;

    public SystemTaskParams(PhoenixConnection conn, PTable.TaskType taskType,
            String tenantId, String schemaName, String tableName,
            String taskStatus, String data, Integer priority, Timestamp startTs,
            Timestamp endTs, boolean accessCheckEnabled) {
        this.conn = conn;
        this.taskType = taskType;
        this.tenantId = tenantId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.taskStatus = taskStatus;
        this.data = data;
        this.priority = priority;
        this.startTs = startTs;
        this.endTs = endTs;
        this.accessCheckEnabled = accessCheckEnabled;
    }

    public PhoenixConnection getConn() {
        return conn;
    }

    public PTable.TaskType getTaskType() {
        return taskType;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public String getData() {
        return data;
    }

    public Integer getPriority() {
        return priority;
    }

    public Timestamp getStartTs() {
        return startTs;
    }

    public Timestamp getEndTs() {
        return endTs;
    }

    public boolean isAccessCheckEnabled() {
        return accessCheckEnabled;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
        justification = "endTs and startTs are not used for mutation")
    public static class SystemTaskParamsBuilder {

        private PhoenixConnection conn;
        private PTable.TaskType taskType;
        private String tenantId;
        private String schemaName;
        private String tableName;
        private String taskStatus;
        private String data;
        private Integer priority;
        private Timestamp startTs;
        private Timestamp endTs;
        private boolean accessCheckEnabled;

        public SystemTaskParamsBuilder setConn(PhoenixConnection conn) {
            this.conn = conn;
            return this;
        }

        public SystemTaskParamsBuilder setTaskType(PTable.TaskType taskType) {
            this.taskType = taskType;
            return this;
        }

        public SystemTaskParamsBuilder setTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public SystemTaskParamsBuilder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public SystemTaskParamsBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public SystemTaskParamsBuilder setTaskStatus(String taskStatus) {
            this.taskStatus = taskStatus;
            return this;
        }

        public SystemTaskParamsBuilder setData(String data) {
            this.data = data;
            return this;
        }

        public SystemTaskParamsBuilder setPriority(Integer priority) {
            this.priority = priority;
            return this;
        }

        public SystemTaskParamsBuilder setStartTs(Timestamp startTs) {
            this.startTs = startTs;
            return this;
        }

        public SystemTaskParamsBuilder setEndTs(Timestamp endTs) {
            this.endTs = endTs;
            return this;
        }

        public SystemTaskParamsBuilder setAccessCheckEnabled(
                boolean accessCheckEnabled) {
            this.accessCheckEnabled = accessCheckEnabled;
            return this;
        }

        public SystemTaskParams build() {
            return new SystemTaskParams(conn, taskType, tenantId, schemaName,
                tableName, taskStatus, data, priority, startTs, endTs,
                accessCheckEnabled);
        }
    }
}
