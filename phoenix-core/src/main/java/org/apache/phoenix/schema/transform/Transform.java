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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.TableInfo;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.coprocessor.tasks.TransformMonitorTask;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.tool.SchemaExtractionProcessor;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TableViewFinderResult;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.coprocessor.MetaDataProtocol.MIN_TABLE_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ENCODING_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSFORM_STATUS;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
import static org.apache.phoenix.query.QueryServices.INDEX_CREATE_DEFAULT_STATE;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.schema.ColumnMetaDataOps.addColumnMutation;
import static org.apache.phoenix.schema.MetaDataClient.CREATE_LINK;
import static org.apache.phoenix.schema.MetaDataClient.UPDATE_INDEX_STATE_TO_ACTIVE;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.schema.PTableType.VIEW;

public class Transform {
    private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);
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
            return Transform.addTransform(table, changingProperties, transformBuilder.build(), sequenceNum, connection);
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
            addTransformTableLink(connection, view.getTenantId()==null? null:Bytes.toString(view.getTenantId()),
                    (view.getSchemaName()==null? null: Bytes.toString(view.getSchemaName())), Bytes.toString(view.getTableName())
                    , newTableName, sequenceNum);
        }

        if (defaultCreateState != PIndexState.CREATE_DISABLE) {
            // add a monitoring task
            TransformMonitorTask.addTransformMonitorTask(connection, connection.getQueryServices().getConfiguration(), systemTransformParams,
                    PTable.TaskStatus.CREATED, new Timestamp(EnvironmentEdgeManager.currentTimeMillis()), null);
        } else {
            LOGGER.info("Transform will not be monitored until it is resumed again.");
        }
        return newTable;
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

    public static PTable getTransformingNewTable(PhoenixConnection connection, PTable oldTable) throws SQLException{
        SystemTransformRecord transformRecord = Transform.getTransformRecord(connection, oldTable.getType(), oldTable.getSchemaName()
                , oldTable.getTableName(), oldTable.getType()==INDEX? oldTable.getParentTableName():null, oldTable.getTenantId()
                , oldTable.getBaseTableLogicalName());

        PTable transformingNewTable = null;
        if (transformRecord != null && transformRecord.isActive()) {
            // New table will behave like an index
            PName newTableNameWithoutSchema = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(transformRecord.getNewPhysicalTableName()));
            if (!newTableNameWithoutSchema.equals(oldTable.getPhysicalName(true))) {
                transformingNewTable = PhoenixRuntime.getTableNoCache(connection, transformRecord.getNewPhysicalTableName());
            }
        }
        return transformingNewTable;
    }

    private static SystemTransformRecord getTransformRecord(PhoenixConnection connection, PTableType tableType, PName schemaName,
                                                           PName tableName, PName dataTableName, PName tenantId,
                                                           PName parentLogicalName) throws SQLException {

        if (tableType == PTableType.TABLE) {
            return Transform.getTransformRecord(schemaName, tableName, null, tenantId, connection);

        } else if (tableType == INDEX) {
            return Transform.getTransformRecord(schemaName, tableName, dataTableName, tenantId, connection);
        } else if (tableType == VIEW) {
            if (parentLogicalName == null) {
                LOGGER.warn("View doesn't seem to have a parent");
                return null;
            }
            return Transform.getTransformRecord(SchemaUtil.getSchemaNameFromFullName(parentLogicalName.getString()),
                    SchemaUtil.getTableNameFromFullName(parentLogicalName.getString()), null, tenantId == null ? null : tenantId.getString(), connection);
        }

        return null;
    }

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

    public static boolean checkIsTransformNeeded(MetaDataClient.MetaProperties metaProperties, String schemaName,
                                                 PTable table, String logicalTableName, String parentTableName,
                                                 String tenantId, PhoenixConnection connection) throws SQLException {
        boolean isTransformNeeded = isTransformNeeded(metaProperties, table);
        if (isTransformNeeded) {
            SystemTransformRecord existingTransform = Transform.getTransformRecord(schemaName, logicalTableName, parentTableName, tenantId,connection);
            if (existingTransform != null && existingTransform.isActive()) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.CANNOT_TRANSFORM_ALREADY_TRANSFORMING_TABLE)
                        .setMessage(" Only one transform at a time is allowed ")
                        .setSchemaName(schemaName).setTableName(logicalTableName).build().buildException();
            }
        }
        return isTransformNeeded;
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

    public static void updateNewTableState(PhoenixConnection connection, SystemTransformRecord systemTransformRecord,
                                           PIndexState state)
            throws SQLException {
        String schema = SchemaUtil.getSchemaNameFromFullName(systemTransformRecord.getNewPhysicalTableName());
        String tableName = SchemaUtil.getTableNameFromFullName(systemTransformRecord.getNewPhysicalTableName());
        try (PreparedStatement tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE_TO_ACTIVE)){
            tableUpsert.setString(1, systemTransformRecord.getTenantId() == null ? null :
                    systemTransformRecord.getTenantId());
            tableUpsert.setString(2, schema);
            tableUpsert.setString(3, tableName);
            tableUpsert.setString(4, state.getSerializedValue());
            tableUpsert.setLong(5, 0);
            tableUpsert.setLong(6, 0);
            tableUpsert.execute();
        }
        // Update cache
        UpgradeUtil.clearCache(connection, connection.getTenantId(), schema, tableName,
                systemTransformRecord.getLogicalParentName(), MIN_TABLE_TIMESTAMP);
    }

    public static void removeTransformRecord(
            SystemTransformRecord transformRecord, PhoenixConnection connection) throws SQLException {
        connection.prepareStatement("DELETE FROM  "
                + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " WHERE " +
                (Strings.isNullOrEmpty(transformRecord.getSchemaName()) ? "" :
                        (PhoenixDatabaseMetaData.TABLE_SCHEM + " ='" + transformRecord.getSchemaName() + "' AND ")) +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + " ='" + transformRecord.getLogicalTableName() + "' AND " +
                PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + " ='" + transformRecord.getNewPhysicalTableName() + "' AND " +
                PhoenixDatabaseMetaData.TRANSFORM_TYPE + " =" + transformRecord.getTransformType().getSerializedValue()
        ).execute();
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

    public static void doCutover(PhoenixConnection connection, SystemTransformRecord systemTransformRecord) throws Exception{
        String tenantId = systemTransformRecord.getTenantId();
        String schema = systemTransformRecord.getSchemaName();
        String tableName = systemTransformRecord.getLogicalTableName();
        String newTableName = SchemaUtil.getTableNameFromFullName(systemTransformRecord.getNewPhysicalTableName());

        // Calculate changed metadata
        List<String> columnNames = new ArrayList<>();
        List<String> columnValues = new ArrayList<>();

        getMetadataDifference(connection, systemTransformRecord, columnNames, columnValues);
        // TODO In the future, we need to handle rowkey changes and column type changes as well

        String
                changeViewStmt = "UPSERT INTO SYSTEM.CATALOG (TENANT_ID, TABLE_SCHEM, TABLE_NAME %s) VALUES (%s, %s, '%s' %s)";

        String
                changeTable = String.format(
                "UPSERT INTO SYSTEM.CATALOG (TENANT_ID, TABLE_SCHEM, TABLE_NAME, PHYSICAL_TABLE_NAME %s) VALUES (%s, %s, '%s','%s' %s)",
                (columnNames.size() > 0? "," + String.join(",", columnNames):""),
                (tenantId==null? null: ("'" + tenantId + "'")),
                (schema==null ? null : ("'" + schema + "'")), tableName, newTableName,
                (columnValues.size() > 0? "," + String.join(",", columnValues):""));

        LOGGER.info("About to do cutover via " + changeTable);
        TableViewFinderResult childViewsResult = ViewUtil.findChildViews(connection, tenantId, schema, tableName);
        boolean wasCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        List<TableInfo> viewsToUpdateCache = new ArrayList<>();
        try {
            connection.createStatement().execute(changeTable);

            // Update column qualifiers
            PTable pNewTable = PhoenixRuntime.getTable(connection, systemTransformRecord.getNewPhysicalTableName());
            PTable pOldTable = PhoenixRuntime.getTable(connection, SchemaUtil.getTableName(schema, tableName));
            if (pOldTable.getImmutableStorageScheme() != pNewTable.getImmutableStorageScheme() ||
                    pOldTable.getEncodingScheme() != pNewTable.getEncodingScheme()) {
                MetaDataClient.mutateTransformProperties(connection, tenantId, schema, tableName, newTableName,
                        pNewTable.getImmutableStorageScheme(), pNewTable.getEncodingScheme());
                // We need to update the columns's qualifiers as well
                mutateColumns(connection.unwrap(PhoenixConnection.class), pOldTable, pNewTable);

                HashMap<String, PColumn> columnMap = new HashMap<>();
                for (PColumn column : pNewTable.getColumns()) {
                    columnMap.put(column.getName().getString(), column);
                }

                // Also update view column qualifiers
                for (TableInfo view : childViewsResult.getLinks()) {
                    PTable pView = PhoenixRuntime.getTable(connection, view.getTenantId()==null? null: Bytes.toString(view.getTenantId())
                            , SchemaUtil.getTableName(view.getSchemaName(), view.getTableName()));
                    mutateViewColumns(connection.unwrap(PhoenixConnection.class), pView, pNewTable, columnMap);
                }
            }
            connection.commit();

            // We can have millions of views. We need to send it in batches
            int maxBatchSize = connection.getQueryServices().getConfiguration().getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
            int batchSize = 0;
            for (TableInfo view : childViewsResult.getLinks()) {
                String changeView = String.format(changeViewStmt,
                        (columnNames.size() > 0? "," + String.join(",", columnNames):""),
                        (view.getTenantId()==null || view.getTenantId().length == 0? null: ("'" + Bytes.toString(view.getTenantId()) + "'")),
                        (view.getSchemaName()==null || view.getSchemaName().length == 0? null : ("'" + Bytes.toString(view.getSchemaName()) + "'")),
                        Bytes.toString(view.getTableName()),
                        (columnValues.size() > 0? "," + String.join(",", columnValues):""));
                LOGGER.info("Cutover changing view via " + changeView);
                connection.createStatement().execute(changeView);
                viewsToUpdateCache.add(view);
                batchSize++;
                if (batchSize >= maxBatchSize) {
                    connection.commit();
                    batchSize = 0;
                }
            }
            if (batchSize > 0) {
                connection.commit();
                batchSize = 0;
            }

            connection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
            UpgradeUtil.clearCacheAndGetNewTable(connection.unwrap(PhoenixConnection.class),
                    connection.getTenantId(),
                    schema, tableName, systemTransformRecord.getLogicalParentName(), MIN_TABLE_TIMESTAMP);
            for (TableInfo view : viewsToUpdateCache) {
                UpgradeUtil.clearCache(connection.unwrap(PhoenixConnection.class),
                        PNameFactory.newName(view.getTenantId()),
                        PNameFactory.newName(view.getSchemaName()).getString(),  Bytes.toString(view.getTableName()),
                        tableName, MIN_TABLE_TIMESTAMP);
            }

            // TODO: Cleanup syscat so that we don't have an extra index
        } catch (Exception e) {
            LOGGER.error("Error happened during cutover ", e);
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(wasCommit);
        }
    }

    private static void getMetadataDifference(PhoenixConnection connection, SystemTransformRecord systemTransformRecord, List<String> columnNames, List<String> columnValues) throws SQLException {
        PTable pOldTable = PhoenixRuntime.getTable(connection, SchemaUtil.getQualifiedTableName(systemTransformRecord.getSchemaName(),systemTransformRecord.getLogicalTableName()));
        PTable pNewTable = PhoenixRuntime.getTable(connection, SchemaUtil.getQualifiedTableName(SchemaUtil.getSchemaNameFromFullName(systemTransformRecord.getNewPhysicalTableName()),
                SchemaUtil.getTableNameFromFullName(systemTransformRecord.getNewPhysicalTableName())));

        Map<String, String> map = pOldTable.getPropertyValues();
        for(Map.Entry<String, String> entry : map.entrySet()) {
            String oldKey = entry.getKey();
            String oldValue = entry.getValue();
            if (pNewTable.getPropertyValues().containsKey(oldKey)) {
                if (PHYSICAL_TABLE_NAME.equals(oldKey)) {
                    // No need to add here. We will add it.
                    continue;
                }
                String newValue = pNewTable.getPropertyValues().get(oldKey);
                if (!Strings.nullToEmpty(oldValue).equals(Strings.nullToEmpty(newValue))) {
                    columnNames.add(oldKey);
                    // properties value that corresponds to a number will not need single quotes around it
                    // properties value that corresponds to a boolean value will not need single quotes around it
                    if (!Strings.isNullOrEmpty(newValue)) {
                        if(!(StringUtils.isNumeric(newValue)) &&
                                !(newValue.equalsIgnoreCase(Boolean.TRUE.toString()) ||newValue.equalsIgnoreCase(Boolean.FALSE.toString()))) {
                                if (ENCODING_SCHEME.equals(oldKey)) {
                                    newValue = String.valueOf(PTable.QualifierEncodingScheme.valueOf(newValue).getSerializedMetadataValue());
                                } else if (IMMUTABLE_STORAGE_SCHEME.equals(oldKey)) {
                                    newValue = String.valueOf(PTable.ImmutableStorageScheme.valueOf(newValue).getSerializedMetadataValue());
                                }
                                else {
                                    newValue = "'" + newValue + "'";
                                }
                        }
                    }
                    columnValues.add(newValue);
                }
            }
        }
    }

    public static void completeTransform(Connection connection, Configuration configuration) throws Exception{
        // Will be called from Reducer
        String tenantId = configuration.get(MAPREDUCE_TENANT_ID, null);
        String fullOldTableName = PhoenixConfigurationUtil.getInputTableName(configuration);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullOldTableName);
        String oldTableLogicalName = SchemaUtil.getTableNameFromFullName(fullOldTableName);
        String indexTableName = SchemaUtil.getTableNameFromFullName(PhoenixConfigurationUtil.getIndexToolIndexTableName(configuration));
        String logicaTableName = oldTableLogicalName;
        String logicalParentName = null;
        if (PhoenixConfigurationUtil.getTransformingTableType(configuration) == IndexScrutinyTool.SourceTable.INDEX_TABLE_SOURCE) {
            if (!Strings.isNullOrEmpty(indexTableName)) {
                logicaTableName = indexTableName;
                logicalParentName = SchemaUtil.getTableName(schemaName, oldTableLogicalName);
            }
        }

        SystemTransformRecord transformRecord = getTransformRecord(schemaName, logicaTableName, logicalParentName,
                tenantId, connection.unwrap(PhoenixConnection.class));

        if (!PTable.TransformType.isPartialTransform(transformRecord.getTransformType())) {
            updateTransformRecord(connection.unwrap(PhoenixConnection.class), transformRecord, PTable.TransformStatus.PENDING_CUTOVER);
            connection.commit();
        } else {
            updateTransformRecord(connection.unwrap(PhoenixConnection.class), transformRecord, PTable.TransformStatus.COMPLETED);
            connection.commit();
        }
    }

    public static void updateTransformRecord(PhoenixConnection connection, SystemTransformRecord transformRecord, PTable.TransformStatus newStatus) throws SQLException {
        SystemTransformRecord.SystemTransformBuilder builder = new SystemTransformRecord.SystemTransformBuilder(transformRecord);
        builder.setTransformStatus(newStatus.name());
        builder.setLastStateTs(new Timestamp(EnvironmentEdgeManager.currentTimeMillis()));
        if (newStatus == PTable.TransformStatus.STARTED) {
            builder.setTransformRetryCount(transformRecord.getTransformRetryCount() + 1);
        }
        Transform.upsertTransform(builder.build(), connection);
    }

    private static void mutateColumns(PhoenixConnection connection, PTable pOldTable, PTable pNewTable) throws SQLException {
        if (pOldTable.getEncodingScheme() != pNewTable.getEncodingScheme()) {
            Short nextKeySeq = 0;
            for (PColumn column : pNewTable.getColumns()) {
                boolean isPk = SchemaUtil.isPKColumn(column);
                Short keySeq = isPk ? ++nextKeySeq : null;
                PColumn newCol = new PColumnImpl(column.getName(), column.getFamilyName(), column.getDataType(),
                        column.getMaxLength(), column.getScale(), column.isNullable(), column.getPosition(), column.getSortOrder()
                        , column.getArraySize(),
                        column.getViewConstant(), column.isViewReferenced(), column.getExpressionStr(), column.isRowTimestamp(),
                        column.isDynamic(), column.getColumnQualifierBytes(), EnvironmentEdgeManager.currentTimeMillis());
                addColumnMutation(connection, pOldTable.getSchemaName()==null?null:pOldTable.getSchemaName().getString()
                        , pOldTable.getTableName().getString(), newCol,
                        pNewTable.getParentTableName() == null ? null : pNewTable.getParentTableName().getString()
                        , pNewTable.getPKName() == null ? null : pNewTable.getPKName().getString(), keySeq, pNewTable.getBucketNum() != null);
            }
        }
    }

    public static PTable getTransformedView(PTable pOldView, PTable pNewTable, HashMap<String, PColumn> columnMap, boolean withDerivedColumns) throws SQLException {
        List<PColumn> newColumns = new ArrayList<>();
        PTable pNewView = null;
        if (pOldView.getEncodingScheme() != pNewTable.getEncodingScheme()) {
            Short nextKeySeq = 0;
            PTable.EncodedCQCounter cqCounterToUse = pNewTable.getEncodedCQCounter();
            String defaultColumnFamily = pNewTable.getDefaultFamilyName() != null && !Strings.isNullOrEmpty(pNewTable.getDefaultFamilyName().getString()) ?
                    pNewTable.getDefaultFamilyName().getString() : DEFAULT_COLUMN_FAMILY;

            for (PColumn column : pOldView.getColumns()) {
                boolean isPk = SchemaUtil.isPKColumn(column);
                Short keySeq = isPk ? ++nextKeySeq : null;
                if (isPk) {
                    continue;
                }
                String familyName = null;
                if (pNewTable.getImmutableStorageScheme() == SINGLE_CELL_ARRAY_WITH_OFFSETS) {
                    familyName = column.getFamilyName() != null ? column.getFamilyName().getString() : defaultColumnFamily;
                } else {
                    familyName = defaultColumnFamily;
                }
                int encodedCQ = pOldView.isAppendOnlySchema() ? Integer.valueOf(ENCODED_CQ_COUNTER_INITIAL_VALUE + keySeq) : cqCounterToUse.getNextQualifier(familyName);
                byte[] colQualifierBytes = EncodedColumnsUtil.getColumnQualifierBytes(column.getName().getString(),
                        encodedCQ, pNewTable, isPk);
                if (columnMap.containsKey(column.getName().getString())) {
                    colQualifierBytes = columnMap.get(column.getName().getString()).getColumnQualifierBytes();
                } else {
                    if (!column.isDerived()) {
                        cqCounterToUse.increment(familyName);
                    }
                }

                if (!withDerivedColumns && column.isDerived()) {
                    // Don't need to add/change derived columns
                    continue;
                }

                PColumn newCol = new PColumnImpl(column.getName(), PNameFactory.newName(familyName), column.getDataType(),
                        column.getMaxLength(), column.getScale(), column.isNullable(), column.getPosition(), column.getSortOrder()
                        , column.getArraySize(),
                        column.getViewConstant(), column.isViewReferenced(), column.getExpressionStr(), column.isRowTimestamp(),
                        column.isDynamic(), colQualifierBytes, EnvironmentEdgeManager.currentTimeMillis());
                newColumns.add(newCol);
                if (!columnMap.containsKey(newCol.getName().getString())) {
                    columnMap.put(newCol.getName().getString(), newCol) ;
                }
            }

            pNewView = PTableImpl.builderWithColumns(pOldView, newColumns)
                        .setQualifierEncodingScheme(pNewTable.getEncodingScheme())
                        .setImmutableStorageScheme(pNewTable.getImmutableStorageScheme())
                        .setPhysicalNames(
                                Collections.singletonList(SchemaUtil.getPhysicalHBaseTableName(
                                        pNewTable.getSchemaName(), pNewTable.getTableName(), pNewTable.isNamespaceMapped())))
                        .build();
        } else {
            // Have to change this per transform type
        }
        return pNewView;
    }

    private static void mutateViewColumns(PhoenixConnection connection, PTable pView, PTable pNewTable, HashMap<String, PColumn> columnMap) throws SQLException {
        if (pView.getEncodingScheme() != pNewTable.getEncodingScheme()) {
            Short nextKeySeq = 0;
            PTable newView = getTransformedView(pView, pNewTable, columnMap,false);
            for (PColumn newCol : newView.getColumns()) {
                boolean isPk = SchemaUtil.isPKColumn(newCol);
                Short keySeq = isPk ? ++nextKeySeq : null;
                if (isPk) {
                    continue;
                }
                String tenantId = pView.getTenantId() == null ? null : pView.getTenantId().getString();
                addColumnMutation(connection, tenantId, pView.getSchemaName() == null ? null : pView.getSchemaName().getString()
                        , pView.getTableName().getString(), newCol,
                        pView.getParentTableName() == null ? null : pView.getParentTableName().getString()
                        , pView.getPKName() == null ? null : pView.getPKName().getString(), keySeq, pView.getBucketNum() != null);
            }
        }
    }

    public static void doForceCutover(Connection connection, Configuration configuration) throws Exception{
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        // Will be called from Reducer
        String tenantId = configuration.get(MAPREDUCE_TENANT_ID, null);
        String fullOldTableName = PhoenixConfigurationUtil.getInputTableName(configuration);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullOldTableName);
        String oldTableLogicalName = SchemaUtil.getTableNameFromFullName(fullOldTableName);
        String indexTableName = SchemaUtil.getTableNameFromFullName(PhoenixConfigurationUtil.getIndexToolIndexTableName(configuration));
        String logicaTableName = oldTableLogicalName;
        String logicalParentName = null;
        if (PhoenixConfigurationUtil.getTransformingTableType(configuration) == IndexScrutinyTool.SourceTable.INDEX_TABLE_SOURCE)
            if (!Strings.isNullOrEmpty(indexTableName)) {
                logicaTableName = indexTableName;
                logicalParentName = SchemaUtil.getTableName(schemaName, oldTableLogicalName);
            }

        SystemTransformRecord transformRecord = getTransformRecord(schemaName, logicaTableName, logicalParentName,
                tenantId, phoenixConnection);
        Transform.doCutover(phoenixConnection, transformRecord);
        updateTransformRecord(phoenixConnection, transformRecord, PTable.TransformStatus.COMPLETED);
        phoenixConnection.commit();
    }

}


