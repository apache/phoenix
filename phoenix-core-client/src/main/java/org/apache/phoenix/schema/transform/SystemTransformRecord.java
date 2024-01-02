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

package org.apache.phoenix.schema.transform;

import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Task params to be used while upserting records in SYSTEM.TRANSFORM table.
 * This POJO is mainly used while upserting(and committing) or generating
 * upsert mutations plan in {@link Transform} class
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
    justification = "lastStateTs and startTs are not used for mutation")
public class SystemTransformRecord {
    private final PTable.TransformType transformType;
    private final String schemaName;
    private final String logicalTableName;
    private final String tenantId;
    private final String logicalParentName;
    private final String newPhysicalTableName;
    private final String transformStatus;
    private final String transformJobId;
    private final Integer transformRetryCount;
    private final Timestamp startTs;
    private final Timestamp lastStateTs;
    private final byte[] oldMetadata;
    private final String newMetadata;
    private final String transformFunction;

    public SystemTransformRecord(PTable.TransformType transformType,
                                 String schemaName, String logicalTableName, String tenantId, String newPhysicalTableName, String logicalParentName,
                                 String transformStatus, String transformJobId, Integer transformRetryCount, Timestamp startTs,
                                 Timestamp lastStateTs, byte[] oldMetadata, String newMetadata, String transformFunction) {
        this.transformType = transformType;
        this.schemaName = schemaName;
        this.tenantId = tenantId;
        this.logicalTableName = logicalTableName;
        this.newPhysicalTableName = newPhysicalTableName;
        this.logicalParentName = logicalParentName;
        this.transformStatus = transformStatus;
        this.transformJobId = transformJobId;
        this.transformRetryCount = transformRetryCount;
        this.startTs = startTs;
        this.lastStateTs = lastStateTs;
        this.oldMetadata = oldMetadata;
        this.newMetadata = newMetadata;
        this.transformFunction = transformFunction;
    }

    public String getString() {
        return String.format("transformType: %s, schameName: %s, logicalTableName: %s, newPhysicalTableName: %s, logicalParentName: %s, status: %s"
                , String.valueOf(transformType), String.valueOf(schemaName), String.valueOf(logicalTableName), String.valueOf(newPhysicalTableName),
                String.valueOf(logicalParentName), String.valueOf(transformStatus));
    }

    public PTable.TransformType getTransformType() {
        return transformType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public String getLogicalParentName() {
        return logicalParentName;
    }

    public String getNewPhysicalTableName() {
        return newPhysicalTableName;
    }

    public String getTransformStatus() {
        return transformStatus;
    }

    public String getTransformJobId() {
        return transformJobId;
    }

    public int getTransformRetryCount() {
        return transformRetryCount;
    }

    public Timestamp getTransformStartTs() {
        return startTs;
    }

    public Timestamp getTransformLastStateTs() {
        return lastStateTs;
    }

    public byte[] getOldMetadata() {
        return oldMetadata;
    }
    public String getNewMetadata() {
        return newMetadata;
    }
    public String getTransformFunction() { return transformFunction; }

    public boolean isActive() {
        return (transformStatus.equals(PTable.TransformStatus.STARTED.name())
                || transformStatus.equals(PTable.TransformStatus.CREATED.name())
                || transformStatus.equals(PTable.TransformStatus.PENDING_CUTOVER.name()));
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
        justification = "lastStateTs and startTs are not used for mutation")
    public static class SystemTransformBuilder {

        private PTable.TransformType transformType = PTable.TransformType.METADATA_TRANSFORM;
        private String schemaName;
        private String tenantId;
        private String logicalTableName;
        private String logicalParentName;
        private String newPhysicalTableName;
        private String transformStatus = PTable.TransformStatus.CREATED.name();
        private String transformJobId;
        private int transformRetryCount =0;
        private Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
        private Timestamp lastStateTs;
        private byte[] oldMetadata;
        private String newMetadata;
        private String transformFunction;

        public SystemTransformBuilder() {

        }

        public SystemTransformBuilder(SystemTransformRecord systemTransformRecord) {
            this.setTransformType(systemTransformRecord.getTransformType());
            this.setTenantId(systemTransformRecord.getTenantId());
            this.setSchemaName(systemTransformRecord.getSchemaName());
            this.setLogicalTableName(systemTransformRecord.getLogicalTableName());
            this.setNewPhysicalTableName(systemTransformRecord.getNewPhysicalTableName());
            this.setLogicalParentName(systemTransformRecord.getLogicalParentName());
            this.setTransformStatus(systemTransformRecord.getTransformStatus());
            this.setTransformJobId(systemTransformRecord.getTransformJobId());
            this.setTransformRetryCount(systemTransformRecord.getTransformRetryCount());
            this.setStartTs(systemTransformRecord.getTransformStartTs());
            this.setLastStateTs(systemTransformRecord.getTransformLastStateTs());
            this.setOldMetadata(systemTransformRecord.getOldMetadata());
            this.setNewMetadata(systemTransformRecord.getNewMetadata());
            this.setTransformFunction(systemTransformRecord.getTransformFunction());
        }

        public SystemTransformBuilder setTransformType(PTable.TransformType transformType) {
            this.transformType = transformType;
            return this;
        }

        public SystemTransformBuilder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public SystemTransformBuilder setLogicalTableName(String tableName) {
            this.logicalTableName = tableName;
            return this;
        }

        public SystemTransformBuilder setTenantId(String tenant) {
            this.tenantId = tenant;
            return this;
        }

        public SystemTransformBuilder setLogicalParentName(String name) {
            this.logicalParentName = name;
            return this;
        }

        public SystemTransformBuilder setNewPhysicalTableName(String tableName) {
            this.newPhysicalTableName = tableName;
            return this;
        }

        public SystemTransformBuilder setTransformStatus(String transformStatus) {
            this.transformStatus = transformStatus;
            return this;
        }

        public SystemTransformBuilder setTransformJobId(String transformJobId) {
            this.transformJobId = transformJobId;
            return this;
        }

        public SystemTransformBuilder setOldMetadata(byte[] oldMetadata) {
            this.oldMetadata = oldMetadata;
            return this;
        }

        public SystemTransformBuilder setNewMetadata(String newMetadata) {
            this.newMetadata = newMetadata;
            return this;
        }

        public SystemTransformBuilder setTransformRetryCount(int transformRetryCount) {
            this.transformRetryCount = transformRetryCount;
            return this;
        }

        public SystemTransformBuilder setStartTs(Timestamp startTs) {
            this.startTs = startTs;
            return this;
        }

        public SystemTransformBuilder setLastStateTs(Timestamp ts) {
            this.lastStateTs = ts;
            return this;
        }

        public SystemTransformBuilder setTransformFunction(String transformFunction) {
            this.transformFunction = transformFunction;
            return this;
        }

        public SystemTransformRecord build() {
            Timestamp lastTs = lastStateTs;
            if (lastTs == null && transformStatus != null && transformStatus.equals(PTable.TaskStatus.COMPLETED.toString())) {
                lastTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            }
            return new SystemTransformRecord(transformType, schemaName,
                    logicalTableName, tenantId, newPhysicalTableName, logicalParentName, transformStatus, transformJobId, transformRetryCount, startTs, lastTs,
                    oldMetadata, newMetadata, transformFunction);
        }

        public static SystemTransformRecord build(ResultSet resultSet) throws SQLException {
            int col = 1;
            SystemTransformBuilder builder = new SystemTransformBuilder();
            builder.setTenantId(resultSet.getString(col++));
            builder.setSchemaName(resultSet.getString(col++));
            builder.setLogicalTableName(resultSet.getString(col++));
            builder.setNewPhysicalTableName(resultSet.getString(col++));
            builder.setTransformType(PTable.TransformType.fromSerializedValue(resultSet.getByte(col++)));
            builder.setLogicalParentName(resultSet.getString(col++));
            builder.setTransformStatus(resultSet.getString(col++));
            builder.setTransformJobId(resultSet.getString(col++));
            builder.setTransformRetryCount(resultSet.getInt(col++));
            builder.setStartTs(resultSet.getTimestamp(col++));
            builder.setLastStateTs(resultSet.getTimestamp(col++));
            builder.setOldMetadata(resultSet.getBytes(col++));
            builder.setNewMetadata(resultSet.getString(col++));
            builder.setTransformFunction(resultSet.getString(col++));

            return builder.build();
        }
    }
}
