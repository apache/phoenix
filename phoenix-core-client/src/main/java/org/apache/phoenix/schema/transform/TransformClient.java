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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.tool.SchemaExtractionProcessor;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TableViewFinderResult;
import org.apache.phoenix.util.TaskMetaDataServiceCallBack;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSFORM_STATUS;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper.DEFAULT_TRANSFORM_MONITOR_ENABLED;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper.TRANSFORM_MONITOR_ENABLED;
import static org.apache.phoenix.query.QueryConstants.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.query.QueryServices.INDEX_CREATE_DEFAULT_STATE;
import static org.apache.phoenix.schema.MetaDataClient.CREATE_LINK;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.schema.PTableType.VIEW;

public class TransformClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformClient.class);
    private static final String TRANSFORM_SELECT = "SELECT " +
            PhoenixDatabaseMetaData.TENANT_ID + ", " +
            PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
            PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", " +
            PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + ", " +
            PhoenixDatabaseMetaData.TRANSFORM_TYPE + ", " +
            PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + ", " +
            TRANSFORM_STATUS + ", " +
            PhoenixDatabaseMetaData.TRANSFORM_JOB_ID + ", " +
            PhoenixDatabaseMetaData.TRANSFORM_RETRY_COUNT + ", " +
            PhoenixDatabaseMetaData.TRANSFORM_START_TS + ", " +
            PhoenixDatabaseMetaData.TRANSFORM_LAST_STATE_TS + ", " +
            PhoenixDatabaseMetaData.OLD_METADATA + " , " +
            PhoenixDatabaseMetaData.NEW_METADATA + " , " +
            PhoenixDatabaseMetaData.TRANSFORM_FUNCTION +
            " FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME;

    public static SystemTransformRecord getTransformRecord(
            PName schema, PName logicalTableName, PName logicalParentName, PName tenantId, PhoenixConnection connection) throws SQLException {
        return  getTransformRecordFromDB((schema==null?null:schema.getString())
                , (logicalTableName==null?null:logicalTableName.getString())
                , (logicalParentName==null?null:logicalParentName.getString())
                , (tenantId==null?null:tenantId.getString()), connection);
    }

    public static SystemTransformRecord getTransformRecord(
            String schema, String logicalTableName, String logicalParentName, String tenantId, PhoenixConnection connection) throws SQLException {
        return getTransformRecordFromDB(schema, logicalTableName, logicalParentName, tenantId, connection);
    }

    public static SystemTransformRecord getTransformRecordFromDB(
            String schema, String logicalTableName, String logicalParentName, String tenantId, PhoenixConnection connection) throws SQLException {
        if (SYSTEM_TRANSFORM_NAME.equals(SchemaUtil.getTableName(schema, logicalTableName))) {
            // Cannot query itself
            return null;
        }
        String sql =  TRANSFORM_SELECT + " WHERE  " +
                (Strings.isNullOrEmpty(tenantId) ? "" : (PhoenixDatabaseMetaData.TENANT_ID + " ='" + tenantId + "' AND ")) +
                (Strings.isNullOrEmpty(schema) ? "" : (PhoenixDatabaseMetaData.TABLE_SCHEM + " ='" + schema + "' AND ")) +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + " ='" + logicalTableName + "'" +
                (Strings.isNullOrEmpty(logicalParentName) ? "" : (" AND " + PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + "='" + logicalParentName + "'"));
        try (ResultSet resultSet = ((PhoenixPreparedStatement) connection.prepareStatement(
                sql)).executeQuery()) {
            if (resultSet.next()) {
                return SystemTransformRecord.SystemTransformBuilder.build(resultSet);
            }
            LOGGER.info("Could not find System.Transform record with " + sql);
            return null;
        }
    }

    private static boolean isTransformNeeded(MetaDataClient.MetaProperties metaProperties, PTable table){
        if (metaProperties.getImmutableStorageSchemeProp()!=null
                && metaProperties.getImmutableStorageSchemeProp() != table.getImmutableStorageScheme()) {
            // Transform is needed
            return true;
        }
        if (metaProperties.getColumnEncodedBytesProp()!=null
                && metaProperties.getColumnEncodedBytesProp() != table.getEncodingScheme()) {
            return true;
        }
        return false;
    }

    public static boolean checkIsTransformNeeded(MetaDataClient.MetaProperties metaProperties, String schemaName,
                                                 PTable table, String logicalTableName, String parentTableName,
                                                 String tenantId, PhoenixConnection connection) throws SQLException {
        boolean isTransformNeeded = isTransformNeeded(metaProperties, table);
        if (isTransformNeeded) {
            SystemTransformRecord existingTransform = getTransformRecord(schemaName, logicalTableName, parentTableName, tenantId,connection);
            if (existingTransform != null && existingTransform.isActive()) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.CANNOT_TRANSFORM_ALREADY_TRANSFORMING_TABLE)
                        .setMessage(" Only one transform at a time is allowed ")
                        .setSchemaName(schemaName).setTableName(logicalTableName).build().buildException();
            }
        }
        return isTransformNeeded;
    }

    protected static SystemTransformRecord getTransformRecord(PhoenixConnection connection, PTableType tableType, PName schemaName,
                                                            PName tableName, PName dataTableName, PName tenantId,
                                                            PName parentLogicalName) throws SQLException {

        if (tableType == PTableType.TABLE) {
            return getTransformRecord(schemaName, tableName, null, tenantId, connection);

        } else if (tableType == INDEX) {
            return getTransformRecord(schemaName, tableName, dataTableName, tenantId, connection);
        } else if (tableType == VIEW) {
            if (parentLogicalName == null) {
                LOGGER.warn("View doesn't seem to have a parent");
                return null;
            }
            return getTransformRecord(SchemaUtil.getSchemaNameFromFullName(parentLogicalName.getString()),
                    SchemaUtil.getTableNameFromFullName(parentLogicalName.getString()), null, tenantId == null ? null : tenantId.getString(), connection);
        }

        return null;
    }

    private static String generateNewTableName(String schema, String logicalTableName, long seqNum) {
        // TODO: Support schema versioning as well.
        String newName = String.format("%s_%d", SchemaUtil.getTableName(schema, logicalTableName), seqNum);
        return newName;
    }

    public static PTable addTransform(PhoenixConnection connection, String tenantId, PTable table, MetaDataClient.MetaProperties changingProperties,
                                      long sequenceNum, PTable.TransformType transformType) throws SQLException {
        try {
            String newMetadata = JacksonUtil.getObjectWriter().writeValueAsString(changingProperties);
            byte[] oldMetadata = PTableImpl.toProto(table).toByteArray();
            String newPhysicalTableName = "";
            SystemTransformRecord.SystemTransformBuilder transformBuilder = new SystemTransformRecord.SystemTransformBuilder();
            String schema = table.getSchemaName()!=null ? table.getSchemaName().getString() : null;
            String logicalTableName = table.getTableName().getString();
            transformBuilder.setSchemaName(schema);
            transformBuilder.setLogicalTableName(logicalTableName);
            transformBuilder.setTenantId(tenantId);
            if (table.getType() == INDEX) {
                transformBuilder.setLogicalParentName(table.getParentName().getString());
            }
            // TODO: add more ways of finding out what transform type this is
            transformBuilder.setTransformType(transformType);
            // TODO: calculate old and new metadata
            transformBuilder.setNewMetadata(newMetadata);
            transformBuilder.setOldMetadata(oldMetadata);
            PIndexState defaultCreateState = PIndexState.valueOf(connection.getQueryServices().getConfiguration().
                    get(INDEX_CREATE_DEFAULT_STATE, QueryServicesOptions.DEFAULT_CREATE_INDEX_STATE));
            if (defaultCreateState == PIndexState.CREATE_DISABLE) {
                // Create a paused transform. This can be enabled later by calling TransformTool resume
                transformBuilder.setTransformStatus(PTable.TransformStatus.PAUSED.name());
            }
            if (Strings.isNullOrEmpty(newPhysicalTableName)) {
                newPhysicalTableName = generateNewTableName(schema, logicalTableName, sequenceNum);
            }
            transformBuilder.setNewPhysicalTableName(newPhysicalTableName);
            return addTransform(table, changingProperties, transformBuilder.build(), sequenceNum, connection);
        } catch (JsonProcessingException ex) {
            LOGGER.error("addTransform failed", ex);
            throw new SQLException("Adding transform failed with JsonProcessingException");
        } catch (SQLException ex) {
            throw ex;
        } catch(Exception ex) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.valueOf("CANNOT_MUTATE_TABLE"))
                    .setSchemaName((table.getSchemaName() == null? null: table.getSchemaName().getString()))
                    .setRootCause(ex)
                    .setTableName(table.getName().getString()).build().buildException();
        }
    }

    protected static PTable addTransform(
            PTable table, MetaDataClient.MetaProperties changedProps, SystemTransformRecord systemTransformParams,
            long sequenceNum, PhoenixConnection connection) throws Exception {
        PName newTableName = PNameFactory.newName(systemTransformParams.getNewPhysicalTableName());
        PName newTableNameWithoutSchema = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(systemTransformParams.getNewPhysicalTableName()));
        PIndexState defaultCreateState = PIndexState.valueOf(connection.getQueryServices().getConfiguration().
                get(INDEX_CREATE_DEFAULT_STATE, QueryServicesOptions.DEFAULT_CREATE_INDEX_STATE));
        PTable newTable = new PTableImpl.Builder()
                .setTableName(newTableNameWithoutSchema)
                .setParentTableName(table.getParentTableName())
                .setBaseTableLogicalName(table.getBaseTableLogicalName())
                .setPhysicalTableName(newTableNameWithoutSchema)
                .setState(defaultCreateState)
                .setAllColumns(table.getColumns())
                .setAppendOnlySchema(table.isAppendOnlySchema())
                .setAutoPartitionSeqName(table.getAutoPartitionSeqName())
                .setBaseColumnCount(table.getBaseColumnCount())
                .setBucketNum(table.getBucketNum())
                .setDefaultFamilyName(table.getDefaultFamilyName())
                .setDisableWAL(table.isWALDisabled())
                .setEstimatedSize(table.getEstimatedSize())
                .setFamilies(table.getColumnFamilies())
                .setImmutableRows(table.isImmutableRows())
                .setIsChangeDetectionEnabled(table.isChangeDetectionEnabled())
                .setIndexType(table.getIndexType())
                .setIndexes(Collections.<PTable>emptyList())
                .setName(newTableName)
                .setMultiTenant(table.isMultiTenant())
                .setParentName(table.getParentName())
                .setParentSchemaName(table.getParentSchemaName())
                .setPhoenixTTL(table.getPhoenixTTL())
                .setNamespaceMapped(table.isNamespaceMapped())
                .setSchemaName(table.getSchemaName())
                .setPkColumns(table.getPKColumns())
                .setPkName(table.getPKName())
                .setPhoenixTTLHighWaterMark(table.getPhoenixTTLHighWaterMark())
                .setRowKeySchema(table.getRowKeySchema())
                .setStoreNulls(table.getStoreNulls())
                .setTenantId(table.getTenantId())
                .setType(table.getType())
                // SchemaExtractor uses physical name to get the table descriptor from. So we use the existing table here
                .setPhysicalNames(ImmutableList.copyOf(table.getPhysicalNames()))
                .setUpdateCacheFrequency(table.getUpdateCacheFrequency())
                .setTransactionProvider(table.getTransactionProvider())
                .setUseStatsForParallelization(table.useStatsForParallelization())
                .setSchemaVersion(table.getSchemaVersion())
                .setIsChangeDetectionEnabled(table.isChangeDetectionEnabled())
                .setStreamingTopicName(table.getStreamingTopicName())
                // Transformables
                .setImmutableStorageScheme(
                        (changedProps.getImmutableStorageSchemeProp() != null? changedProps.getImmutableStorageSchemeProp():table.getImmutableStorageScheme()))
                .setQualifierEncodingScheme(
                        (changedProps.getColumnEncodedBytesProp() != null? changedProps.getColumnEncodedBytesProp() : table.getEncodingScheme()))
                .build();
        SchemaExtractionProcessor schemaExtractionProcessor = new SchemaExtractionProcessor(systemTransformParams.getTenantId(),
                connection.getQueryServices().getConfiguration(), newTable, true);
        String ddl = schemaExtractionProcessor.process();
        LOGGER.info("Creating transforming table via " + ddl);
        connection.createStatement().execute(ddl);
        upsertTransform(systemTransformParams, connection);

        // Add row linking from old table row to new table row
        addTransformTableLink(connection, systemTransformParams.getTenantId(), systemTransformParams.getSchemaName(),
                systemTransformParams.getLogicalTableName(), newTableName, sequenceNum);

        // Also add the transforming new table link to views
        TableViewFinderResult childViewsResult = ViewUtil.findChildViews(connection, systemTransformParams.getTenantId()
                , systemTransformParams.getSchemaName(), systemTransformParams.getLogicalTableName());
        for (TableInfo view : childViewsResult.getLinks()) {
            addTransformTableLink(connection, view.getTenantId()==null? null: Bytes.toString(view.getTenantId()),
                    (view.getSchemaName()==null? null: Bytes.toString(view.getSchemaName())), Bytes.toString(view.getTableName())
                    , newTableName, sequenceNum);
        }

        if (defaultCreateState != PIndexState.CREATE_DISABLE) {
            // add a monitoring task
            addTransformMonitorTask(connection, connection.getQueryServices().getConfiguration(), systemTransformParams,
                    PTable.TaskStatus.CREATED, new Timestamp(EnvironmentEdgeManager.currentTimeMillis()), null);
        } else {
            LOGGER.info("Transform will not be monitored until it is resumed again.");
        }
        return newTable;
    }

    public static void upsertTransform(
            SystemTransformRecord systemTransformParams, PhoenixConnection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("UPSERT INTO " +
                PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " ( " +
                PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.TENANT_ID + "," +
                PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_TYPE + ", " +
                PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + ", " +
                TRANSFORM_STATUS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_JOB_ID + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_RETRY_COUNT + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_START_TS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_LAST_STATE_TS + ", " +
                PhoenixDatabaseMetaData.OLD_METADATA + " , " +
                PhoenixDatabaseMetaData.NEW_METADATA + " , " +
                PhoenixDatabaseMetaData.TRANSFORM_FUNCTION +
                " ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            int colNum = 1;
            if (systemTransformParams.getSchemaName() != null) {
                stmt.setString(colNum++, systemTransformParams.getSchemaName());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            stmt.setString(colNum++, systemTransformParams.getLogicalTableName());
            if (systemTransformParams.getTenantId() != null) {
                stmt.setString(colNum++, systemTransformParams.getTenantId());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            stmt.setString(colNum++, systemTransformParams.getNewPhysicalTableName());

            stmt.setInt(colNum++, systemTransformParams.getTransformType().getSerializedValue());
            if (systemTransformParams.getLogicalParentName() != null) {
                stmt.setString(colNum++, systemTransformParams.getLogicalParentName());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }

            stmt.setString(colNum++, systemTransformParams.getTransformStatus());

            if (systemTransformParams.getTransformJobId() != null) {
                stmt.setString(colNum++, systemTransformParams.getTransformJobId());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            stmt.setInt(colNum++, systemTransformParams.getTransformRetryCount());

            stmt.setTimestamp(colNum++, systemTransformParams.getTransformStartTs());

            if (systemTransformParams.getTransformLastStateTs() != null) {
                stmt.setTimestamp(colNum++, systemTransformParams.getTransformLastStateTs());
            } else {
                stmt.setNull(colNum++, Types.TIMESTAMP);
            }
            if (systemTransformParams.getOldMetadata() != null) {
                stmt.setBytes(colNum++, systemTransformParams.getOldMetadata());
            } else {
                stmt.setNull(colNum++, Types.VARBINARY);
            }
            if (systemTransformParams.getNewMetadata() != null) {
                stmt.setString(colNum++, systemTransformParams.getNewMetadata());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            if (systemTransformParams.getTransformFunction() != null) {
                stmt.setString(colNum++, systemTransformParams.getTransformFunction());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            LOGGER.info("Adding transform type: "
                    + systemTransformParams.getString());
            stmt.execute();
        }
    }

    private static void addTransformTableLink(Connection connection, String tenantId, String schemaName, String tableName,
                                              PName newTableName, long sequenceNum) throws SQLException {
        PreparedStatement linkStatement = connection.prepareStatement(CREATE_LINK);
        linkStatement.setString(1, tenantId);
        linkStatement.setString(2, schemaName);
        linkStatement.setString(3,tableName);
        linkStatement.setString(4, newTableName.getString());
        linkStatement.setByte(5, PTable.LinkType.TRANSFORMING_NEW_TABLE.getSerializedValue());
        linkStatement.setLong(6, sequenceNum);
        linkStatement.setString(7, PTableType.TABLE.getSerializedValue());
        linkStatement.execute();
    }

    public static void addTransformMonitorTask(PhoenixConnection connection, Configuration configuration, SystemTransformRecord systemTransformRecord,
                                               PTable.TaskStatus taskStatus, Timestamp startTimestamp, Timestamp endTimestamp) throws IOException, SQLException {
        boolean transformMonitorEnabled = configuration.getBoolean(TRANSFORM_MONITOR_ENABLED, DEFAULT_TRANSFORM_MONITOR_ENABLED);
        if (!transformMonitorEnabled) {
            LOGGER.warn("TransformMonitor is not enabled. Monitoring/retrying TransformTool and doing cutover will not be done automatically");
            return;
        }

        List<Mutation> sysTaskUpsertMutations = Task.getMutationsForAddTask(new SystemTaskParams.SystemTaskParamsBuilder()
                .setConn(connection)
                .setTaskType(PTable.TaskType.TRANSFORM_MONITOR)
                .setTenantId(systemTransformRecord.getTenantId())
                .setSchemaName(systemTransformRecord.getSchemaName())
                .setTableName(systemTransformRecord.getLogicalTableName())
                .setTaskStatus(taskStatus.toString())
                .setStartTs(startTimestamp)
                .setEndTs(endTimestamp)
                .setAccessCheckEnabled(true)
                .build());
        byte[] rowKey = sysTaskUpsertMutations
                .get(0).getRow();
        MetaDataProtocol.MetaDataMutationResult metaDataMutationResult =
                Task.taskMetaDataCoprocessorExec(connection, rowKey,
                        new TaskMetaDataServiceCallBack(sysTaskUpsertMutations));
        if (MetaDataProtocol.MutationCode.UNABLE_TO_UPSERT_TASK.equals(
                metaDataMutationResult.getMutationCode())) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNABLE_TO_UPSERT_TASK)
                    .setSchemaName(SYSTEM_SCHEMA_NAME)
                    .setTableName(SYSTEM_TASK_TABLE).build().buildException();
        }
    }
}
