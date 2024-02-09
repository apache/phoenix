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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
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
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TableViewFinderResult;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.MIN_TABLE_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ENCODING_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_TABLE_NAME;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.schema.ColumnMetaDataOps.addColumnMutation;
import static org.apache.phoenix.schema.MetaDataClient.UPDATE_INDEX_STATE_TO_ACTIVE;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
import static org.apache.phoenix.schema.PTableType.INDEX;

public class Transform extends TransformClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);

    public static PTable getTransformingNewTable(PhoenixConnection connection, PTable oldTable) throws SQLException{
        SystemTransformRecord transformRecord = TransformClient.getTransformRecord(connection, oldTable.getType(), oldTable.getSchemaName()
                , oldTable.getTableName(), oldTable.getType()==INDEX? oldTable.getParentTableName():null, oldTable.getTenantId()
                , oldTable.getBaseTableLogicalName());

        PTable transformingNewTable = null;
        if (transformRecord != null && transformRecord.isActive()) {
            // New table will behave like an index
            PName newTableNameWithoutSchema = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(transformRecord.getNewPhysicalTableName()));
            if (!newTableNameWithoutSchema.equals(oldTable.getPhysicalName(true))) {
                transformingNewTable = connection.getTableNoCache(
                        transformRecord.getNewPhysicalTableName());
            }
        }
        return transformingNewTable;
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

        String changeViewStmt = "UPSERT INTO SYSTEM.CATALOG "
            + "(TENANT_ID, TABLE_SCHEM, TABLE_NAME %s) VALUES (?, ?, ? %s)";

        String
                changeTable = String.format("UPSERT INTO SYSTEM.CATALOG "
                + "(TENANT_ID, TABLE_SCHEM, TABLE_NAME, PHYSICAL_TABLE_NAME %s ) "
                + "VALUES(?, ?, ?, ? %s)", columnNames.size() > 0 ? ","
                + String.join(",", columnNames) : "", columnNames.size() > 0
            ? "," + QueryUtil.generateInListParams(columnValues.size()) : "");

        LOGGER.info("About to do cutover via " + changeTable);
        TableViewFinderResult childViewsResult = ViewUtil.findChildViews(connection, tenantId, schema, tableName);
        boolean wasCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        List<TableInfo> viewsToUpdateCache = new ArrayList<>();
        try {
            try (PreparedStatement stmt = connection.prepareStatement(changeTable)) {
                int param = 0;
                if (tenantId == null) {
                    stmt.setNull(++param, Types.VARCHAR);
                } else {
                    stmt.setString(++param, tenantId);
                }
                if (schema == null) {
                    stmt.setNull(++param, Types.VARCHAR);
                } else {
                    stmt.setString(++param, schema);
                }
                stmt.setString(++param, tableName);
                stmt.setString(++param, newTableName);
                for (int i = 0; i < columnValues.size(); i++) {
                    stmt.setInt(++param, Integer.parseInt(columnValues.get(i)));
                }
                stmt.execute();
            }
            // Update column qualifiers
            PTable pNewTable = connection.getTable(systemTransformRecord.getNewPhysicalTableName());
            PTable pOldTable = connection.getTable(SchemaUtil.getTableName(schema, tableName));
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
                    PTable pView = connection.getTable(view.getTenantId() == null
                                    ? null : Bytes.toString(view.getTenantId()),
                            SchemaUtil.getTableName(view.getSchemaName(), view.getTableName()));
                    mutateViewColumns(connection.unwrap(PhoenixConnection.class), pView, pNewTable, columnMap);
                }
            }
            connection.commit();

            // We can have millions of views. We need to send it in batches
            int maxBatchSize = connection.getQueryServices().getConfiguration().getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
            int batchSize = 0;
            for (TableInfo view : childViewsResult.getLinks()) {
                String changeView = String.format(changeViewStmt,
                    columnNames.size() > 0 ? "," + String.join(",", columnNames) : "",
                    columnNames.size() > 0 ? ","
                        + QueryUtil.generateInListParams(columnValues.size()) : "");
                LOGGER.info("Cutover changing view via " + changeView);
                try (PreparedStatement stmt = connection.prepareStatement(changeView)) {
                    int param = 0;
                    if (view.getTenantId() == null || view.getTenantId().length == 0) {
                        stmt.setNull(++param, Types.VARCHAR);
                    } else {
                        stmt.setString(++param, Bytes.toString(view.getTenantId()));
                    }
                    if (view.getSchemaName() == null || view.getSchemaName().length == 0) {
                        stmt.setNull(++param, Types.VARCHAR);
                    } else {
                        stmt.setString(++param, Bytes.toString(view.getSchemaName()));
                    }
                    stmt.setString(++param, Bytes.toString(view.getTableName()));
                    for (int i = 0; i < columnValues.size(); i++) {
                        stmt.setInt(++param, Integer.parseInt(columnValues.get(i)));
                    }
                    stmt.execute();
                }
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

    private static void getMetadataDifference(PhoenixConnection connection,
            SystemTransformRecord systemTransformRecord,
            List<String> columnNames, List<String> columnValues) throws SQLException {
        PTable pOldTable = connection.getTable(SchemaUtil.getQualifiedTableName(
                systemTransformRecord.getSchemaName(),
                systemTransformRecord.getLogicalTableName()));
        PTable pNewTable = connection.getTable(SchemaUtil.getQualifiedTableName(SchemaUtil
                        .getSchemaNameFromFullName(systemTransformRecord.getNewPhysicalTableName()),
                SchemaUtil.getTableNameFromFullName(
                        systemTransformRecord.getNewPhysicalTableName())));

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


