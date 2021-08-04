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
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.transform.TransformTool;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.tool.SchemaExtractionProcessor;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID;
import static org.apache.phoenix.schema.ColumnMetaDataOps.addColumnMutation;
import static org.apache.phoenix.schema.PTableType.INDEX;

public class Transform {
    private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);

    private static String generateNewTableName(String schema, String logicalTableName, long seqNum) {
        // TODO: Support schema versioning as well.
        String newName = String.format("%s_%d", SchemaUtil.getTableName(schema, logicalTableName), seqNum);
        return newName;
    }

    public static void addTransform(PhoenixConnection connection, String tenantId, PTable table, MetaDataClient.MetaProperties changingProperties,
                                    long sequenceNum, PTable.TransformType transformType) throws SQLException {
        try {
            String newMetadata = JacksonUtil.getObjectWriter().writeValueAsString(changingProperties);
            String oldMetadata = "";
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
            if (Strings.isNullOrEmpty(newPhysicalTableName)) {
                newPhysicalTableName = generateNewTableName(schema, logicalTableName, sequenceNum);
            }
            transformBuilder.setNewPhysicalTableName(newPhysicalTableName);
            Transform.addTransform(table, changingProperties, transformBuilder.build(), connection);
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

    protected static void addTransform(
            PTable table, MetaDataClient.MetaProperties changedProps, SystemTransformRecord systemTransformParams, PhoenixConnection connection) throws Exception {
        PName newTableName = PNameFactory.newName(systemTransformParams.getNewPhysicalTableName());
        PName newTableNameWithoutSchema = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(systemTransformParams.getNewPhysicalTableName()));
        PTable newTable = new PTableImpl.Builder()
                .setTableName(newTableNameWithoutSchema)
                .setParentTableName(table.getParentTableName())
                .setBaseTableLogicalName(table.getBaseTableLogicalName())
                .setPhysicalTableName(newTableNameWithoutSchema)
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
                // Transformables
                .setImmutableStorageScheme(
                        (changedProps.getImmutableStorageSchemeProp() != null? changedProps.getImmutableStorageSchemeProp():table.getImmutableStorageScheme()))
                .setQualifierEncodingScheme(
                        (changedProps.getColumnEncodedBytesProp() != null? changedProps.getColumnEncodedBytesProp() : table.getEncodingScheme()))
                .build();
        SchemaExtractionProcessor schemaExtractionProcessor = new SchemaExtractionProcessor(systemTransformParams.getTenantId(),
                connection.getQueryServices().getConfiguration(), newTable,  true);
        String ddl = schemaExtractionProcessor.process();
        LOGGER.info("Creating transforming table via " + ddl);
        connection.createStatement().execute(ddl);
        upsertTransform(systemTransformParams, connection);
    }

    public static SystemTransformRecord getTransformRecord(
            String schema, String logicalTableName, String logicalParentName, String tenantId, PhoenixConnection connection) throws SQLException {
        try (ResultSet resultSet = connection.prepareStatement("SELECT " +
                PhoenixDatabaseMetaData.TENANT_ID + ", " +
                PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_TYPE + ", " +
                PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_STATUS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_JOB_ID + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_RETRY_COUNT + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_START_TS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_END_TS + ", " +
                PhoenixDatabaseMetaData.OLD_METADATA + " , " +
                PhoenixDatabaseMetaData.NEW_METADATA + " , " +
                PhoenixDatabaseMetaData.TRANSFORM_FUNCTION +
                " FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " WHERE  " +
                (Strings.isNullOrEmpty(tenantId) ? "" : (PhoenixDatabaseMetaData.TENANT_ID + " ='" + tenantId + "' AND ")) +
                (Strings.isNullOrEmpty(schema) ? "" : (PhoenixDatabaseMetaData.TABLE_SCHEM + " ='" + schema + "' AND ")) +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + " ='" + logicalTableName + "'" +
                (Strings.isNullOrEmpty(logicalParentName) ? "": (" AND " + PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + "='" + logicalParentName + "'" ))
        ).executeQuery()) {
            if (resultSet.next()) {
                return SystemTransformRecord.SystemTransformBuilder.build(resultSet);
            }
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
                PhoenixDatabaseMetaData.TRANSFORM_STATUS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_JOB_ID + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_RETRY_COUNT + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_START_TS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_END_TS + ", " +
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

            if (systemTransformParams.getTransformEndTs() != null) {
                stmt.setTimestamp(colNum++, systemTransformParams.getTransformEndTs());
            } else {
                stmt.setNull(colNum++, Types.TIMESTAMP);
            }
            if (systemTransformParams.getOldMetadata() != null) {
                stmt.setString(colNum++, systemTransformParams.getOldMetadata());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
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


    public static void completeTransform(Connection connection, Configuration configuration) throws Exception{
        // Will be called from Reducer
        long timestamp= EnvironmentEdgeManager.currentTimeMillis();
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
        boolean isPartial = PhoenixConfigurationUtil.getIsPartialTransform(configuration);
        SystemTransformRecord transformRecord = getTransformRecord(schemaName, logicaTableName, logicalParentName,
                tenantId, connection.unwrap(PhoenixConnection.class));
        if (!isPartial) {
            String newTableName = SchemaUtil.getTableNameFromFullName(transformRecord.getNewPhysicalTableName());
            PTable pNewTable = PhoenixRuntime.getTable(connection, transformRecord.getNewPhysicalTableName());
            PTable pOldTable = PhoenixRuntime.getTable(connection, SchemaUtil.getTableName(schemaName,logicaTableName));
            if (pOldTable.getImmutableStorageScheme() != pNewTable.getImmutableStorageScheme() ||
                    pOldTable.getEncodingScheme() != pNewTable.getEncodingScheme()) {
                MetaDataClient.mutateTransformProperties(connection, tenantId, schemaName, logicaTableName, newTableName,
                        pNewTable.getImmutableStorageScheme(), pNewTable.getEncodingScheme());
                // We need to update the columns's qualifiers as well
                if (pOldTable.getEncodingScheme() != pNewTable.getEncodingScheme()) {
                    Short nextKeySeq = 0;
                    for (PColumn newCol : pNewTable.getColumns()) {
                        boolean isPk = SchemaUtil.isPKColumn(newCol);
                        Short keySeq = isPk ? ++nextKeySeq : null;
                        PColumn column = new PColumnImpl(newCol.getName(), newCol.getFamilyName(), newCol.getDataType(),
                                newCol.getMaxLength(), newCol.getScale(), newCol.isNullable(), newCol.getPosition(), newCol.getSortOrder()
                                , newCol.getArraySize(),
                                newCol.getViewConstant(), newCol.isViewReferenced(), newCol.getExpressionStr(), newCol.isRowTimestamp(),
                                newCol.isDynamic(), newCol.getColumnQualifierBytes(), EnvironmentEdgeManager.currentTimeMillis());
                        addColumnMutation(connection.unwrap(PhoenixConnection.class), schemaName, logicaTableName, column,
                                pNewTable.getParentTableName()==null? null:pNewTable.getParentTableName().getString()
                                , pNewTable.getPKName()==null? null:pNewTable.getPKName().getString(), keySeq , pNewTable.getBucketNum() != null);
                    }
                }
            }
            // Clear cache so that the new table is used for queries
            connection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
            TransformTool.updateTransformRecord(connection.unwrap(PhoenixConnection.class), transformRecord, PTable.TransformStatus.COMPLETED);

            // TODO Kick partial transform from the TransformMonitor
            SystemTransformRecord.SystemTransformBuilder builder = new SystemTransformRecord.SystemTransformBuilder(transformRecord);
            builder.setTransformStatus(PTable.TransformStatus.CREATED.name());
            builder.setTransformJobId(null);
            builder.setStartTs(new Timestamp(timestamp));
            builder.setTransformRetryCount(0);
            builder.setTransformType(PTable.TransformType.METADATA_TRANSFORM_PARTIAL);
            SystemTransformRecord partialStr = builder.build();
            Transform.upsertTransform(partialStr, connection.unwrap(PhoenixConnection.class));
            connection.commit();
        } else {
            TransformTool.updateTransformRecord(connection.unwrap(PhoenixConnection.class), transformRecord, PTable.TransformStatus.COMPLETED);
            connection.commit();
            // TODO: cleanup
        }
    }

}


