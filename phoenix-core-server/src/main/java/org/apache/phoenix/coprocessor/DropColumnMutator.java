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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.ListIterator;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.FAMILY_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID_INDEX;
import static org.apache.phoenix.query.QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;
import static org.apache.phoenix.util.ViewUtil.isViewDiverging;

public class DropColumnMutator implements ColumnMutator {

    private List<Pair<PTable, PColumn>> tableAndDroppedColPairs;
    private Configuration conf;

    private static final Logger logger = LoggerFactory.getLogger(DropColumnMutator.class);

    public DropColumnMutator(Configuration conf) {
        this.tableAndDroppedColPairs = Lists.newArrayList();
        this.conf = conf;
    }

    @Override
    public MutateColumnType getMutateColumnType() {
        return MutateColumnType.DROP_COLUMN;
    }

    /**
     * Checks to see if the column being dropped is required by a child view
     */
    @Override
    public MetaDataMutationResult validateWithChildViews(PTable table, List<PTable> childViews,
                                                         List<Mutation> tableMetadata,
                                                         byte[] schemaName, byte[] tableName)
            throws IOException, SQLException {
        List<Delete> columnDeletesForBaseTable = Lists.newArrayListWithExpectedSize(5);
        for (Mutation m : tableMetadata) {
            if (m instanceof Delete) {
                byte[][] rkmd = new byte[5][];
                int pkCount = getVarChars(m.getRow(), rkmd);
                if (pkCount > COLUMN_NAME_INDEX
                        && Bytes.compareTo(schemaName, rkmd[SCHEMA_NAME_INDEX]) == 0
                        && Bytes.compareTo(tableName, rkmd[TABLE_NAME_INDEX]) == 0) {
                    columnDeletesForBaseTable.add((Delete) m);
                }
            }
        }
        for (PTable view : childViews) {
            for (Delete columnDeleteForBaseTable : columnDeletesForBaseTable) {
                PColumn existingViewColumn = null;
                byte[][] rkmd = new byte[5][];
                getVarChars(columnDeleteForBaseTable.getRow(), rkmd);
                String columnName = Bytes.toString(rkmd[COLUMN_NAME_INDEX]);
                String columnFamily =
                        rkmd[FAMILY_NAME_INDEX] == null ? null : Bytes
                                .toString(rkmd[FAMILY_NAME_INDEX]);
                try {
                    existingViewColumn = columnFamily == null ?
                            view.getColumnForColumnName(columnName) :
                            view.getColumnFamily(columnFamily).getPColumnForColumnName(columnName);
                } catch (ColumnFamilyNotFoundException e) {
                    // ignore since it means that the column family is not present for the column to
                    // be added.
                } catch (ColumnNotFoundException e) {
                    // ignore since it means the column is not present in the view
                }

                // check if the view where expression contains the column being dropped and prevent
                // it
                if (existingViewColumn != null && view.getViewStatement() != null) {
                    ParseNode viewWhere =
                            new SQLParser(view.getViewStatement()).parseQuery().getWhere();
                    try (PhoenixConnection conn =
                            QueryUtil.getConnectionOnServer(conf)
                                .unwrap(PhoenixConnection.class)) {
                        PhoenixStatement statement = new PhoenixStatement(conn);
                        TableRef baseTableRef = new TableRef(view);
                        ColumnResolver columnResolver =
                            FromCompiler.getResolver(baseTableRef);
                        StatementContext context =
                            new StatementContext(statement, columnResolver);
                        Expression whereExpression =
                            WhereCompiler.compile(context, viewWhere);
                        Expression colExpression = new ColumnRef(baseTableRef,
                            existingViewColumn.getPosition()).newColumnExpression();
                        MetaDataEndpointImpl.ColumnFinder columnFinder =
                          new MetaDataEndpointImpl.ColumnFinder(colExpression);
                        whereExpression.accept(columnFinder);
                        if (columnFinder.getColumnFound()) {
                            return new MetaDataMutationResult(
                                MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), table);
                        }
                    }
                }

                if (existingViewColumn != null) {
                    tableAndDroppedColPairs.add(new Pair(view, existingViewColumn));
                }
            }

        }
        return null;
    }

    @Override
    public MetaDataMutationResult validateAndAddMetadata(PTable table,
                                                         byte[][] rowKeyMetaData,
                                                         List<Mutation> tableMetaData,
                                                         Region region,
                                                         List<ImmutableBytesPtr> invalidateList,
                                                         List<Region.RowLock> locks,
                                                         long clientTimeStamp,
                                                         long clientVersion,
                                                         ExtendedCellBuilder extendedCellBuilder,
                                                         final boolean isDroppingColumns)
        throws SQLException {
        byte[] tenantId = rowKeyMetaData[TENANT_ID_INDEX];
        byte[] schemaName = rowKeyMetaData[SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[TABLE_NAME_INDEX];
        boolean isView = table.getType() == PTableType.VIEW;
        boolean deletePKColumn = false;

        byte[] tableHeaderRowKey = SchemaUtil.getTableKey(tenantId,
            schemaName, tableName);
        List<Mutation> additionalTableMetaData = Lists.newArrayList();
        ListIterator<Mutation> iterator = tableMetaData.listIterator();
        while (iterator.hasNext()) {
            Mutation mutation = iterator.next();
            byte[] key = mutation.getRow();
            int pkCount = getVarChars(key, rowKeyMetaData);
            if (isView && mutation instanceof Put) {
                PColumn column = null;
                // checking put from the view or index
                if (Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0
                        && Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0) {
                    column = MetaDataUtil.getColumn(pkCount, rowKeyMetaData, table);
                } else {
                    for (int i = 0; i < table.getIndexes().size(); i++) {
                        PTableImpl indexTable = (PTableImpl) table.getIndexes().get(i);
                        byte[] indexTableName = indexTable.getTableName().getBytes();
                        byte[] indexSchema = indexTable.getSchemaName().getBytes();
                        if (Bytes.compareTo(indexSchema, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0
                                && Bytes.compareTo(indexTableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0) {
                            column = MetaDataUtil.getColumn(pkCount, rowKeyMetaData, indexTable);
                            break;
                        }
                    }
                }
                if (column == null)
                    continue;
                // ignore any puts that modify the ordinal positions of columns
                iterator.remove();
            } else if (mutation instanceof Delete) {
                if (pkCount > COLUMN_NAME_INDEX
                        && Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0
                        && Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0) {
                    PColumn columnToDelete = null;
                    try {
                        columnToDelete = MetaDataUtil.getColumn(pkCount, rowKeyMetaData, table);
                        if (columnToDelete == null)
                            continue;
                        deletePKColumn = columnToDelete.getFamilyName() == null;
                        if (isView) {
                            // if we are dropping a derived column add it to the excluded
                            // column list. Note that this is only done for 4.15+ clients
                            // since old clients do not have the isDerived field
                            if (columnToDelete.isDerived()) {
                                mutation = MetaDataUtil.cloneDeleteToPutAndAddColumn((Delete)
                                                mutation, TABLE_FAMILY_BYTES, LINK_TYPE_BYTES,
                                        PTable.LinkType.EXCLUDED_COLUMN.
                                                getSerializedValueAsByteArray());
                                iterator.set(mutation);
                            }

                            if (isViewDiverging(columnToDelete, table, clientVersion)) {
                                // If the column being dropped is inherited from the base table,
                                // then the view is about to diverge itself from the base table.
                                // The consequence of this divergence is that that any further
                                // meta-data changes made to the base table will not be
                                // propagated to the hierarchy of views where this view is the root.
                                byte[] viewKey = SchemaUtil.getTableKey(tenantId, schemaName,
                                        tableName);
                                Put updateBaseColumnCountPut = new Put(viewKey);
                                byte[] baseColumnCountPtr =
                                        new byte[PInteger.INSTANCE.getByteSize()];
                                PInteger.INSTANCE.getCodec().encodeInt(
                                        DIVERGED_VIEW_BASE_COLUMN_COUNT, baseColumnCountPtr, 0);
                                updateBaseColumnCountPut.addColumn(
                                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                                        PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES,
                                        clientTimeStamp,
                                        baseColumnCountPtr);
                                additionalTableMetaData.add(updateBaseColumnCountPut);
                            }
                        }
                        if (columnToDelete.isViewReferenced()) {
                            // Disallow deletion of column referenced in WHERE clause of view
                            return new MetaDataProtocol.MetaDataMutationResult(
                                    MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION,
                                    EnvironmentEdgeManager.currentTimeMillis(), table,
                                    columnToDelete);
                        }
                        // drop any indexes that need the column that is going to be dropped
                        tableAndDroppedColPairs.add(new Pair<>(table, columnToDelete));
                    } catch (ColumnFamilyNotFoundException | ColumnNotFoundException e) {
                        return new MetaDataProtocol.MetaDataMutationResult(
                                MetaDataProtocol.MutationCode.COLUMN_NOT_FOUND,
                                EnvironmentEdgeManager.currentTimeMillis(), table, columnToDelete);
                    }
                }
            }

        }
        //We're changing the application-facing schema by dropping a column, so update the DDL
        // timestamp to current _server_ timestamp
        if (MetaDataUtil.isTableDirectlyQueried(table.getType())) {
            long serverTimestamp = EnvironmentEdgeManager.currentTimeMillis();
            additionalTableMetaData.add(MetaDataUtil.getLastDDLTimestampUpdate(tableHeaderRowKey,
                clientTimeStamp, serverTimestamp));
        }
        //we don't need to update the DDL timestamp for any child views we may have, because
        // when we look up a PTable for any of those child views, we'll take the max timestamp
        // of the view and all its ancestors. This is true
        // whether the view is diverged or not.
        tableMetaData.addAll(additionalTableMetaData);
        if (deletePKColumn) {
            if (table.getPKColumns().size() == 1) {
                return new MetaDataProtocol.MetaDataMutationResult(
                        MetaDataProtocol.MutationCode.NO_PK_COLUMNS,
                        EnvironmentEdgeManager.currentTimeMillis(), null);
            }
        }
        long currentTime = MetaDataUtil.getClientTimeStamp(tableMetaData);
        return new MetaDataProtocol.MetaDataMutationResult(
                MetaDataProtocol.MutationCode.TABLE_ALREADY_EXISTS, currentTime, null);
    }

    @Override
    public List<Pair<PTable, PColumn>> getTableAndDroppedColumnPairs() {
        return tableAndDroppedColPairs;
    }
}
