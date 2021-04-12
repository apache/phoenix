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

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.FAMILY_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID_INDEX;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

public class AddColumnMutator implements ColumnMutator {

    private static final Logger logger = LoggerFactory.getLogger(AddColumnMutator.class);

    private int getInteger(Put p, byte[] family, byte[] qualifier) {
        List<Cell> cells = p.get(family, qualifier);
        if (cells != null && cells.size() > 0) {
            Cell cell = cells.get(0);
            return (Integer)PInteger.INSTANCE.toObject(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength());
        }
        return 0;
    }

    @Override
    public MutateColumnType getMutateColumnType() {
        return MutateColumnType.ADD_COLUMN;
    }

    /**
     * Validates that we can add the column to the base table by ensuring that if the same column
     * already exists in any child view all of the column properties match
     */
    @Override
    public MetaDataMutationResult validateWithChildViews(PTable table, List<PTable> childViews,
                                                         List<Mutation> tableMetadata,
                                                         byte[] schemaName, byte[] tableName)
            throws SQLException {
        // Disallow if trying to switch tenancy of a table that has views
        if (!childViews.isEmpty() && switchAttribute(table.isMultiTenant(),
                tableMetadata, MULTI_TENANT_BYTES)) {
            return new MetaDataMutationResult(MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION
                    , EnvironmentEdgeManager.currentTimeMillis(), null);
        }

        List<Put> columnPutsForBaseTable =
                Lists.newArrayListWithExpectedSize(tableMetadata.size());
        boolean salted = table.getBucketNum()!=null;
        // Isolate the puts relevant to adding columns
        for (Mutation m : tableMetadata) {
            if (m instanceof Put) {
                byte[][] rkmd = new byte[5][];
                int pkCount = getVarChars(m.getRow(), rkmd);
                // check if this put is for adding a column
                if (pkCount > COLUMN_NAME_INDEX && rkmd[COLUMN_NAME_INDEX] != null
                        && rkmd[COLUMN_NAME_INDEX].length > 0
                        && Bytes.compareTo(schemaName, rkmd[SCHEMA_NAME_INDEX]) == 0
                        && Bytes.compareTo(tableName, rkmd[TABLE_NAME_INDEX]) == 0) {
                    columnPutsForBaseTable.add((Put)m);
                }
            }
        }
        for (PTable view : childViews) {
            /*
             * Disallow adding columns to a base table with APPEND_ONLY_SCHEMA since this
             * creates a gap in the column positions for every view (PHOENIX-4737).
             */
            if (!columnPutsForBaseTable.isEmpty() && view.isAppendOnlySchema()) {
                return new MetaDataProtocol.MetaDataMutationResult(
                        MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION,
                        EnvironmentEdgeManager.currentTimeMillis(), null);
            }

            // add the new columns to the child view
            List<PColumn> viewPkCols = new ArrayList<>(view.getPKColumns());
            // remove salted column
            if (salted) {
                viewPkCols.remove(0);
            }
            // remove pk columns that are present in the parent
            viewPkCols.removeAll(table.getPKColumns());
            boolean addedPkColumn = false;
            for (Put columnToBeAdded : columnPutsForBaseTable) {
                PColumn existingViewColumn = null;
                byte[][] rkmd = new byte[5][];
                getVarChars(columnToBeAdded.getRow(), rkmd);
                String columnName = Bytes.toString(rkmd[COLUMN_NAME_INDEX]);
                String columnFamily =
                        rkmd[FAMILY_NAME_INDEX] == null ? null
                                : Bytes.toString(rkmd[FAMILY_NAME_INDEX]);
                try {
                    existingViewColumn =
                            columnFamily == null ? view.getColumnForColumnName(columnName)
                                    : view.getColumnFamily(columnFamily)
                                    .getPColumnForColumnName(columnName);
                } catch (ColumnFamilyNotFoundException e) {
                    // ignore since it means that the column family is not present for the column to
                    // be added.
                } catch (ColumnNotFoundException e) {
                    // ignore since it means the column is not present in the view
                }

                boolean isCurrColumnToBeAddPkCol = columnFamily == null;
                addedPkColumn |= isCurrColumnToBeAddPkCol;
                if (existingViewColumn != null) {
                    if (EncodedColumnsUtil.usesEncodedColumnNames(table)
                            && !SchemaUtil.isPKColumn(existingViewColumn)) {
                        /*
                         * If the column already exists in a view, then we cannot add the column to
                         * the base table. The reason is subtle and is as follows: consider the case
                         * where a table has two views where both the views have the same key value
                         * column KV. Now, we dole out encoded column qualifiers for key value
                         * columns in views by using the counters stored in the base physical table.
                         * So the KV column can have different column qualifiers for the two views.
                         * For example, 11 for VIEW1 and 12 for VIEW2. This naturally extends to
                         * rows being inserted using the two views having different column
                         * qualifiers for the column named KV. Now, when an attempt is made to add
                         * column KV to the base table, we cannot decide which column qualifier
                         * should that column be assigned. It cannot be a number different than 11
                         * or 12 since a query like SELECT KV FROM BASETABLE would return null for
                         * KV which is incorrect since column KV is present in rows inserted from
                         * the two views. We cannot use 11 or 12 either because we will then
                         * incorrectly return value of KV column inserted using only one view.
                         */
                        return new MetaDataProtocol.MetaDataMutationResult(MetaDataProtocol.
                                MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), table);
                    }
                    // Validate data type is same
                    int baseColumnDataType =
                            getInteger(columnToBeAdded, TABLE_FAMILY_BYTES, DATA_TYPE_BYTES);
                    if (baseColumnDataType != existingViewColumn.getDataType().getSqlType()) {
                        return new MetaDataProtocol.MetaDataMutationResult(MetaDataProtocol.
                                MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), table);
                    }

                    // Validate max length is same
                    int maxLength =
                            getInteger(columnToBeAdded, TABLE_FAMILY_BYTES, COLUMN_SIZE_BYTES);
                    int existingMaxLength =
                            existingViewColumn.getMaxLength() == null ? 0
                                    : existingViewColumn.getMaxLength();
                    if (maxLength != existingMaxLength) {
                        return new MetaDataProtocol.MetaDataMutationResult(MetaDataProtocol.
                                MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), table);
                    }

                    // Validate scale is same
                    int scale =
                            getInteger(columnToBeAdded, TABLE_FAMILY_BYTES, DECIMAL_DIGITS_BYTES);
                    int existingScale =
                            existingViewColumn.getScale() == null ? 0
                                    : existingViewColumn.getScale();
                    if (scale != existingScale) {
                        return new MetaDataProtocol.MetaDataMutationResult(MetaDataProtocol.
                                MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), table);
                    }

                    // Validate sort order is same
                    int sortOrder =
                            getInteger(columnToBeAdded, TABLE_FAMILY_BYTES, SORT_ORDER_BYTES);
                    if (sortOrder != existingViewColumn.getSortOrder().getSystemValue()) {
                        return new MetaDataProtocol.MetaDataMutationResult(MetaDataProtocol.
                                MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), table);
                    }

                    // if the column to be added to the base table is a pk column, then we need to
                    // validate that the key slot position is the same
                    if (isCurrColumnToBeAddPkCol) {
                        List<Cell> keySeqCells =
                                columnToBeAdded.get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                                        PhoenixDatabaseMetaData.KEY_SEQ_BYTES);
                        if (keySeqCells != null && keySeqCells.size() > 0) {
                            Cell cell = keySeqCells.get(0);
                            int keySeq =
                                    PSmallint.INSTANCE.getCodec().decodeInt(cell.getValueArray(),
                                            cell.getValueOffset(), SortOrder.getDefault());
                            // we need to take into account the columns inherited from the base table
                            // if the table is salted we don't include the salted column (which is
                            // present in getPKColumns())
                            int pkPosition = SchemaUtil.getPKPosition(view, existingViewColumn)
                                    + 1 - (salted ? 1 : 0);
                            if (pkPosition != keySeq) {
                                return new MetaDataProtocol.MetaDataMutationResult(
                                        MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION,
                                        EnvironmentEdgeManager.currentTimeMillis(),
                                        table);
                            }
                        }
                    }
                }
                if (existingViewColumn!=null && isCurrColumnToBeAddPkCol) {
                    viewPkCols.remove(existingViewColumn);
                }
            }
            /*
             * Allow adding a pk columns to base table : 1. if all the view pk columns are exactly
             * the same as the base table pk columns 2. if we are adding all the existing view pk
             * columns to the base table
             */
            if (addedPkColumn && !viewPkCols.isEmpty()) {
                return new MetaDataProtocol.MetaDataMutationResult(MetaDataProtocol.MutationCode
                        .UNALLOWED_TABLE_MUTATION,
                        EnvironmentEdgeManager.currentTimeMillis(), table);
            }
        }
        return null;
    }

    private boolean switchAttribute(boolean currAttribute, List<Mutation> tableMetaData,
                                    byte[] attrQualifier) {
        for (Mutation m : tableMetaData) {
            if (m instanceof Put) {
                Put p = (Put)m;
                List<Cell> cells = p.get(TABLE_FAMILY_BYTES, attrQualifier);
                if (cells != null && cells.size() > 0) {
                    Cell cell = cells.get(0);
                    boolean newAttribute = (boolean)PBoolean.INSTANCE.toObject(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength());
                    return currAttribute != newAttribute;
                }
            }
        }
        return false;
    }

    @Override
    public MetaDataMutationResult validateAndAddMetadata(PTable table, byte[][] rowKeyMetaData,
                                                         List<Mutation> tableMetaData,
                                                         Region region,
                                                         List<ImmutableBytesPtr> invalidateList,
                                                         List<Region.RowLock> locks,
                                                         long clientTimeStamp,
                                                         long clientVersion,
                                                         ExtendedCellBuilder extendedCellBuilder,
                                                         final boolean isAddingColumns) {
        byte[] tenantId = rowKeyMetaData[TENANT_ID_INDEX];
        byte[] schemaName = rowKeyMetaData[SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[TABLE_NAME_INDEX];
        PTableType type = table.getType();
        byte[] tableHeaderRowKey = SchemaUtil.getTableKey(tenantId,
                schemaName, tableName);
        List<Mutation> additionalTableMetadataMutations =
                Lists.newArrayListWithExpectedSize(2);

        boolean addingCol = false;
        for (Mutation m : tableMetaData) {
            byte[] key = m.getRow();
            boolean addingPKColumn = false;
            int pkCount = getVarChars(key, rowKeyMetaData);
            // this means we have are adding a column
            if (pkCount > COLUMN_NAME_INDEX
                    && Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0
                    && Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0) {
                try {
                    addingCol = true;
                    byte[] familyName = null;
                    byte[] colName = null;
                    if (pkCount > FAMILY_NAME_INDEX) {
                        familyName = rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX];
                    }
                    if (pkCount > COLUMN_NAME_INDEX) {
                        colName = rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
                    }
                    if (table.getExcludedColumns().contains(
                            PColumnImpl.createExcludedColumn(MetaDataEndpointImpl.newPName(familyName),
                                    MetaDataEndpointImpl.newPName(colName), 0l))) {
                        // if this column was previously dropped in a view
                        // do not allow adding the column back
                        return new MetaDataProtocol.MetaDataMutationResult(
                                MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                    if (familyName!=null && familyName.length > 0) {
                        MetaDataMutationResult result =
                                compareWithPkColumns(colName, table, familyName);
                        if (result != null) {
                            return result;
                        }
                        PColumnFamily family =
                                table.getColumnFamily(familyName);
                        family.getPColumnForColumnNameBytes(colName);
                    } else if (colName!=null && colName.length > 0) {
                        addingPKColumn = true;
                        table.getPKColumn(Bytes.toString(colName));
                    } else {
                        continue;
                    }
                    return new MetaDataProtocol.MetaDataMutationResult(
                            MetaDataProtocol.MutationCode.COLUMN_ALREADY_EXISTS,
                            EnvironmentEdgeManager.currentTimeMillis(), table);
                } catch (ColumnFamilyNotFoundException e) {
                    continue;
                } catch (ColumnNotFoundException e) {
                    if (addingPKColumn) {
                        // We may be adding a DESC column, so if table is already
                        // able to be rowKeyOptimized, it should continue to be so.
                        if (table.rowKeyOrderOptimizable()) {
                            UpgradeUtil.addRowKeyOrderOptimizableCell(
                                    additionalTableMetadataMutations, tableHeaderRowKey,
                                    clientTimeStamp);
                        } else if (table.getType() == PTableType.VIEW){
                            // Don't allow view PK to diverge from table PK as our upgrade code
                            // does not handle this.
                            return new MetaDataProtocol.MetaDataMutationResult(
                                    MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION,
                                    EnvironmentEdgeManager.currentTimeMillis(), null);
                        }
                        // Add all indexes to invalidate list, as they will all be
                        // adding the same PK column. No need to lock them, as we
                        // have the parent table lock at this point.
                        for (PTable index : table.getIndexes()) {
                            invalidateList.add(new ImmutableBytesPtr(SchemaUtil
                                    .getTableKey(tenantId, index.getSchemaName()
                                            .getBytes(), index.getTableName()
                                            .getBytes())));
                            // We may be adding a DESC column, so if index is already
                            // able to be rowKeyOptimized, it should continue to be so.
                            if (index.rowKeyOrderOptimizable()) {
                                byte[] indexHeaderRowKey =
                                        SchemaUtil.getTableKey(index.getTenantId() == null ?
                                                ByteUtil.EMPTY_BYTE_ARRAY :
                                                index.getTenantId().getBytes(),
                                                index.getSchemaName().getBytes(),
                                                index.getTableName().getBytes());
                                UpgradeUtil.addRowKeyOrderOptimizableCell(
                                        additionalTableMetadataMutations, indexHeaderRowKey,
                                        clientTimeStamp);
                            }
                        }
                    }
                    continue;
                }
            } else if (pkCount == COLUMN_NAME_INDEX &&
                    ! (Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0 &&
                            Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0 ) ) {
                // Invalidate any table with mutations
                // TODO: this likely means we don't need the above logic that
                // loops through the indexes if adding a PK column, since we'd
                // always have header rows for those.
                invalidateList.add(new ImmutableBytesPtr(SchemaUtil
                        .getTableKey(tenantId,
                                rowKeyMetaData[SCHEMA_NAME_INDEX],
                                rowKeyMetaData[TABLE_NAME_INDEX])));
            }
        }
        if (isAddingColumns) {
            //We're changing the application-facing schema by adding a column, so update the DDL
            // timestamp
            long serverTimestamp = EnvironmentEdgeManager.currentTimeMillis();
            if (MetaDataUtil.isTableDirectlyQueried(table.getType())) {
                additionalTableMetadataMutations.add(MetaDataUtil.getLastDDLTimestampUpdate(tableHeaderRowKey,
                    clientTimeStamp, serverTimestamp));
            }
            //we don't need to update the DDL timestamp for child views, because when we look up
            // a PTable, we'll take the max timestamp of a view and all its ancestors. This is true
            // whether the view is diverged or not.
        }
        tableMetaData.addAll(additionalTableMetadataMutations);
        if (type == PTableType.VIEW) {
            if ( EncodedColumnsUtil.usesEncodedColumnNames(table) && addingCol &&
                    !table.isAppendOnlySchema()) {
                // When adding a column to a view that uses encoded column name
                // scheme, we need to modify the CQ counters stored in the view's
                // physical table. So to make sure clients get the latest PTable, we
                // need to invalidate the cache entry.
                // If the table uses APPEND_ONLY_SCHEMA we use the position of the
                // column as the encoded column qualifier and so we don't need to
                // update the CQ counter in the view physical table (see
                // PHOENIX-4737)
                invalidateList.add(new ImmutableBytesPtr(
                        MetaDataUtil.getPhysicalTableRowForView(table)));
            }
            // Pass in null as the parent PTable, since we always want to tag the cells
            // in this case, irrespective of the property values of the parent
            ViewUtil.addTagsToPutsForViewAlteredProperties(tableMetaData, null,
                    extendedCellBuilder);
        }
        return null;
    }

    private MetaDataMutationResult compareWithPkColumns(byte[] colName,
            PTable table, byte[] familyName) {
        // check if column is matching with any of pk columns if given
        // column belongs to default CF
        if (Bytes.compareTo(familyName, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES) == 0
                && colName != null && colName.length > 0) {
            for (PColumn pColumn : table.getPKColumns()) {
                if (Bytes.compareTo(
                        pColumn.getName().getBytes(), colName) == 0) {
                    return new MetaDataProtocol.MetaDataMutationResult(
                            MetaDataProtocol.MutationCode.COLUMN_ALREADY_EXISTS,
                            EnvironmentEdgeManager.currentTimeMillis(), table);
                }
            }
        }
        return null;
    }

    @Override
    public List<Pair<PTable, PColumn>> getTableAndDroppedColumnPairs() {
        return Collections.emptyList();
    }
}
