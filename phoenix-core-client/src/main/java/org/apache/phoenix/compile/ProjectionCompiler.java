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
package org.apache.phoenix.compile;

import static org.apache.phoenix.query.QueryServices.WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.function.ArrayIndexFunction;
import org.apache.phoenix.expression.function.BsonValueFunction;
import org.apache.phoenix.expression.function.JsonQueryFunction;
import org.apache.phoenix.expression.function.JsonValueFunction;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.expression.visitor.ProjectedColumnExpressionVisitor;
import org.apache.phoenix.expression.visitor.ReplaceArrayFunctionExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.PhoenixRowTimestampParseNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ArgumentTypeMismatchException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.IndexUncoveredDataColumnRef;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;


/**
 * 
 * Class that iterates through expressions in SELECT clause and adds projected
 * columns to scan.
 *
 * 
 * @since 0.1
 */
public class ProjectionCompiler {
    private static final Expression NULL_EXPRESSION = LiteralExpression.newConstant(null);
    private ProjectionCompiler() {
    }
    
    private static void projectColumnFamily(PTable table, Scan scan, byte[] family) {
        // Will project all colmuns for given CF
        scan.addFamily(family);
    }
    
    public static RowProjector compile(StatementContext context, SelectStatement statement, GroupBy groupBy) throws SQLException  {
        boolean wildcardIncludesDynamicCols = context.getConnection().getQueryServices()
                .getConfiguration().getBoolean(WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB,
                        DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB);
        return compile(context, statement, groupBy, Collections.<PColumn>emptyList(),
                // Pass null expression because we don't want empty key value to be projected
                NULL_EXPRESSION,
                wildcardIncludesDynamicCols);
    }
    
    private static int getMinPKOffset(PTable table, PName tenantId) {
        // In SELECT *, don't include tenant column or index ID column for tenant connection
        int posOffset = table.getBucketNum() == null ? 0 : 1;
        if (table.isMultiTenant() && tenantId != null) {
            posOffset++;
        }
        if (table.getViewIndexId() != null) {
            posOffset++;
        }
        return posOffset;
    }
    
    private static void projectAllTableColumns(StatementContext context, TableRef tableRef, boolean resolveColumn, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns, List<? extends PDatum> targetColumns) throws SQLException {
        ColumnResolver resolver = context.getResolver();
        PTable table = tableRef.getTable();
        int projectedOffset = projectedExpressions.size();
        int posOffset = table.getBucketNum() == null ? 0 : 1;
        int minPKOffset = getMinPKOffset(table, context.getConnection().getTenantId());
        for (int i = posOffset, j = posOffset; i < table.getColumns().size(); i++) {
            PColumn column = table.getColumns().get(i);
            // Skip tenant ID column (which may not be the first column, but is the first PK column)
            if (SchemaUtil.isPKColumn(column) && j++ < minPKOffset) {
                posOffset++;
                continue;
            }
            ColumnRef ref = new ColumnRef(tableRef,i);
            String colName = ref.getColumn().getName().getString();
            String tableAlias = tableRef.getTableAlias();
            if (resolveColumn) {
                try {
                    if (tableAlias != null) {
                        ref = resolver.resolveColumn(null, tableAlias, colName);
                    } else {
                        String schemaName = table.getSchemaName().getString();
                        ref = resolver.resolveColumn(schemaName.length() == 0 ? null : schemaName, table.getTableName().getString(), colName);
                    }
                    // The freshly revolved column's family better be the same as the original one.
                    // If not, trigger the disambiguation logic. Also see PTableImpl.getColumnForColumnName(...)
                    if (column.getFamilyName() != null && !column.getFamilyName().equals(ref.getColumn().getFamilyName())) {
                        throw new AmbiguousColumnException();
                    }
                } catch (AmbiguousColumnException e) {
                    if (column.getFamilyName() != null) {
                        ref = resolver.resolveColumn(tableAlias != null ? tableAlias : table.getTableName().getString(), column.getFamilyName().getString(), colName);
                    } else {
                        throw e;
                    }
                }
            }
            Expression expression = ref.newColumnExpression();
            expression = coerceIfNecessary(i-posOffset+projectedOffset, targetColumns, expression);
            ImmutableBytesWritable ptr = context.getTempPtr();
            if (IndexUtil.getViewConstantValue(column, ptr)) {
                expression = LiteralExpression.newConstant(
                        column.getDataType().toObject(ptr, column.getSortOrder()),
                        expression.getDataType(),
                        column.getSortOrder());
            }
            projectedExpressions.add(expression);
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            projectedColumns.add(new ExpressionProjector(colName, colName, tableRef.getTableAlias() == null ? table.getName().getString() : tableRef.getTableAlias(), expression, isCaseSensitive));
        }
    }
    
    private static void projectAllIndexColumns(StatementContext context, TableRef tableRef, boolean resolveColumn, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns, List<? extends PDatum> targetColumns) throws SQLException {
        ColumnResolver resolver = context.getResolver();
        PTable index = tableRef.getTable();
        int projectedOffset = projectedExpressions.size();
        PhoenixConnection conn = context.getConnection();
        PName tenantId = conn.getTenantId();
        String dataTableName = index.getParentName().getString();
        PTable dataTable = conn.getTable(dataTableName);
        int tableOffset = dataTable.getBucketNum() == null ? 0 : 1;
        int minTablePKOffset = getMinPKOffset(dataTable, tenantId);
        int minIndexPKOffset = getMinPKOffset(index, tenantId);
        if (!IndexUtil.shouldIndexBeUsedForUncoveredQuery(tableRef)) {
            if (index.getColumns().size()-minIndexPKOffset != dataTable.getColumns().size()-minTablePKOffset) {
                // We'll end up not using this by the optimizer, so just throw
                String schemaNameStr = dataTable.getSchemaName()==null?null:dataTable.getSchemaName().getString();
                String tableNameStr = dataTable.getTableName()==null?null:dataTable.getTableName().getString();
                throw new ColumnNotFoundException(schemaNameStr, tableNameStr,null, WildcardParseNode.INSTANCE.toString());
            }
        }
        // At this point, the index table is either fully covered, or we are projecting uncovered
        // columns
        // The easy thing would be to just call projectAllTableColumns on the projected table,
        // but its columns are not in the same order as the data column, so we have to map them to
        // the data column order
        TableRef projectedTableRef =
                new TableRef(resolver.getTables().get(0), tableRef.getTableAlias());
        for (int i = tableOffset, j = tableOffset; i < dataTable.getColumns().size(); i++) {
            PColumn column = dataTable.getColumns().get(i);
            // Skip tenant ID column (which may not be the first column, but is the first PK column)
            if (SchemaUtil.isPKColumn(column) && j++ < minTablePKOffset) {
                tableOffset++;
                continue;
            }
            PColumn dataTableColumn = dataTable.getColumns().get(i);
            String indexColName = IndexUtil.getIndexColumnName(dataTableColumn);
            PColumn indexColumn = null;
            ColumnRef ref = null;
            try {
                indexColumn = index.getColumnForColumnName(indexColName);
                // TODO: Should we do this more efficiently than catching the exception ?
            } catch (ColumnNotFoundException e) {
                if (IndexUtil.shouldIndexBeUsedForUncoveredQuery(tableRef)) {
                    //Projected columns have the same name as in the data table
                    String familyName =
                            dataTableColumn.getFamilyName() == null ? null
                                    : dataTableColumn.getFamilyName().getString();
                    ref =
                            resolver.resolveColumn(familyName,
                                tableRef.getTableAlias() == null
                                        ? tableRef.getTable().getName().getString()
                                        : tableRef.getTableAlias(),
                                indexColName);
                    indexColumn = ref.getColumn();
                } else {
                    throw e;
                }
            }
            ref = new ColumnRef(projectedTableRef, indexColumn.getPosition());
            String colName = dataTableColumn.getName().getString();
            String tableAlias = tableRef.getTableAlias();
            if (resolveColumn) {
                try {
                    if (tableAlias != null) {
                        ref = resolver.resolveColumn(null, tableAlias, indexColName);
                    } else {
                        String schemaName = index.getSchemaName().getString();
                        ref = resolver.resolveColumn(schemaName.length() == 0 ? null : schemaName, index.getTableName().getString(), indexColName);
                    }
                } catch (AmbiguousColumnException e) {
                    if (indexColumn.getFamilyName() != null) {
                        ref = resolver.resolveColumn(tableAlias != null ? tableAlias : index.getTableName().getString(), indexColumn.getFamilyName().getString(), indexColName);
                    } else {
                        throw e;
                    }
                }
            }
            Expression expression = ref.newColumnExpression();
            expression = coerceIfNecessary(i-tableOffset+projectedOffset, targetColumns, expression);
            // We do not need to check if the column is a viewConstant, because view constants never
            // appear as a column in an index
            projectedExpressions.add(expression);
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            ExpressionProjector projector = new ExpressionProjector(colName, colName, tableRef.getTableAlias() == null ? dataTable.getName().getString() : tableRef.getTableAlias(), expression, isCaseSensitive);
            projectedColumns.add(projector);
        }
    }
    
    private static void projectTableColumnFamily(StatementContext context, String cfName, TableRef tableRef, boolean resolveColumn, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns) throws SQLException {
        PTable table = tableRef.getTable();
        PColumnFamily pfamily = table.getColumnFamily(cfName);
        for (PColumn column : pfamily.getColumns()) {
            ColumnRef ref = new ColumnRef(tableRef, column.getPosition());
            if (resolveColumn) {
                ref = context.getResolver().resolveColumn(table.getTableName().getString(), cfName, column.getName().getString());
            }
            Expression expression = ref.newColumnExpression();
            projectedExpressions.add(expression);
            String colName = column.getName().toString();
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            projectedColumns.add(new ExpressionProjector(colName, colName, tableRef.getTableAlias() == null ?
                    table.getName().getString() : tableRef.getTableAlias(), expression, isCaseSensitive));
        }
    }

    private static void projectIndexColumnFamily(StatementContext context, String cfName, TableRef tableRef, boolean resolveColumn, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns) throws SQLException {
        ColumnResolver resolver = context.getResolver();
        PTable index = tableRef.getTable();
        PhoenixConnection conn = context.getConnection();
        String dataTableName = index.getParentName().getString();
        PTable dataTable = conn.getTable(dataTableName);
        PColumnFamily pfamily = dataTable.getColumnFamily(cfName);
        TableRef projectedTableRef =
                new TableRef(resolver.getTables().get(0), tableRef.getTableAlias());
        PTable projectedIndex = projectedTableRef.getTable();
        for (PColumn column : pfamily.getColumns()) {
            String indexColName = IndexUtil.getIndexColumnName(column);
            PColumn indexColumn = null;
            ColumnRef ref = null;
            String indexColumnFamily = null;
            try {
                indexColumn = index.getColumnForColumnName(indexColName);
                ref = new ColumnRef(projectedTableRef, indexColumn.getPosition());
                indexColumnFamily =
                        indexColumn.getFamilyName() == null ? null
                                : indexColumn.getFamilyName().getString();
            } catch (ColumnNotFoundException e) {
                if (IndexUtil.shouldIndexBeUsedForUncoveredQuery(tableRef)) {
                    try {
                        //Projected columns have the same name as in the data table
                        String colName = column.getName().getString();
                        String familyName =
                                column.getFamilyName() == null ? null
                                        : column.getFamilyName().getString();
                        resolver.resolveColumn(familyName,
                            tableRef.getTableAlias() == null
                                    ? tableRef.getTable().getName().getString()
                                    : tableRef.getTableAlias(),
                            indexColName);
                        indexColumn = projectedIndex.getColumnForColumnName(colName);
                    } catch (ColumnFamilyNotFoundException c) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
            if (resolveColumn) {
                ref =
                        resolver.resolveColumn(index.getTableName().getString(), indexColumnFamily,
                            indexColName);
            }
            Expression expression = ref.newColumnExpression();
            projectedExpressions.add(expression);
            String colName = column.getName().toString();
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            projectedColumns.add(new ExpressionProjector(colName, colName,
                    tableRef.getTableAlias() == null ? dataTable.getName().getString()
                            : tableRef.getTableAlias(),
                    expression, isCaseSensitive));
        }
    }
    
    private static Expression coerceIfNecessary(int index, List<? extends PDatum> targetColumns, Expression expression) throws SQLException {
        if (index < targetColumns.size()) {
            PDatum targetColumn = targetColumns.get(index);
            if (targetColumn.getDataType() != expression.getDataType()) {
                PDataType targetType = targetColumn.getDataType();
                // Check if coerce allowed using more relaxed isCastableTo check, since we promote INTEGER to LONG 
                // during expression evaluation and then convert back to INTEGER on UPSERT SELECT (and we don't have
                // (an actual value we can specifically check against).
                if (expression.getDataType() != null && !expression.getDataType().isCastableTo(targetType)) {
                    throw new ArgumentTypeMismatchException(targetType, expression.getDataType(), "column: " + targetColumn);
                }
                expression = CoerceExpression.create(expression, targetType, targetColumn.getSortOrder(), targetColumn.getMaxLength());
            }
        }
        return expression;
    }
    /**
     * Builds the projection for the scan
     * @param context query context kept between compilation of different query clauses
     * @param statement the statement being compiled
     * @param groupBy compiled GROUP BY clause
     * @param targetColumns list of columns, parallel to aliasedNodes, that are being set for an
     * UPSERT SELECT statement. Used to coerce expression types to the expected target type.
     * @param where the where clause expression
     * @param wildcardIncludesDynamicCols true if wildcard queries should include dynamic columns
     * @return projector used to access row values during scan
     * @throws SQLException 
     */
    public static RowProjector compile(StatementContext context, SelectStatement statement,
            GroupBy groupBy, List<? extends PDatum> targetColumns, Expression where,
            boolean wildcardIncludesDynamicCols) throws SQLException {
        List<KeyValueColumnExpression> serverParsedKVRefs = new ArrayList<>();
        List<ProjectedColumnExpression> serverParsedProjectedColumnRefs = new ArrayList<>();
        List<Expression> serverParsedKVFuncs = new ArrayList<>();
        List<Expression> serverParsedOldFuncs = new ArrayList<>();
        Map<Expression, Integer> serverParsedExpressionCounts = new HashMap<>();
        List<AliasedNode> aliasedNodes = statement.getSelect();
        // Setup projected columns in Scan
        SelectClauseVisitor
                selectVisitor =
                new SelectClauseVisitor(context, groupBy, serverParsedKVRefs, serverParsedKVFuncs,
                        serverParsedExpressionCounts, serverParsedProjectedColumnRefs,
                        serverParsedOldFuncs, statement);
        List<ExpressionProjector> projectedColumns = new ArrayList<>();
        ColumnResolver resolver = context.getResolver();
        TableRef tableRef = context.getCurrentTable();
        PTable table = tableRef.getTable();
        boolean resolveColumn = !tableRef.equals(resolver.getTables().get(0));
        boolean isWildcard = false;
        Scan scan = context.getScan();
        int index = 0;
        List<Expression> projectedExpressions = Lists.newArrayListWithExpectedSize(aliasedNodes.size());
        List<byte[]> projectedFamilies = Lists.newArrayListWithExpectedSize(aliasedNodes.size());
        for (AliasedNode aliasedNode : aliasedNodes) {
            ParseNode node = aliasedNode.getNode();
            // TODO: visitor?
            if (node instanceof WildcardParseNode) {
                if (statement.isAggregate()) {
                    ExpressionCompiler.throwNonAggExpressionInAggException(node.toString());
                }
                if (tableRef == TableRef.EMPTY_TABLE_REF) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_TABLE_SPECIFIED_FOR_WILDCARD_SELECT).build().buildException();
                }
                isWildcard = true;
                if (tableRef.getTable().getType() == PTableType.INDEX && ((WildcardParseNode)node).isRewrite()) {
                    projectAllIndexColumns(context, tableRef, resolveColumn, projectedExpressions, projectedColumns, targetColumns);
                } else {
                    projectAllTableColumns(context, tableRef, resolveColumn, projectedExpressions, projectedColumns, targetColumns);
                }
            } else if (node instanceof TableWildcardParseNode) {
                TableName tName = ((TableWildcardParseNode) node).getTableName();
                TableRef tRef = resolver.resolveTable(tName.getSchemaName(), tName.getTableName());
                if (tRef.equals(tableRef)) {
                    isWildcard = true;
                }
                if (tRef.getTable().getType() == PTableType.INDEX && ((TableWildcardParseNode)node).isRewrite()) {
                    projectAllIndexColumns(context, tRef, true, projectedExpressions, projectedColumns, targetColumns);
                } else {
                    projectAllTableColumns(context, tRef, true, projectedExpressions, projectedColumns, targetColumns);
                }
            } else if (node instanceof  FamilyWildcardParseNode) {
                if (tableRef == TableRef.EMPTY_TABLE_REF) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_TABLE_SPECIFIED_FOR_WILDCARD_SELECT).build().buildException();
                }
                // Project everything for SELECT cf.*
                String cfName = ((FamilyWildcardParseNode) node).getName();
                // Delay projecting to scan, as when any other column in the column family gets
                // added to the scan, it overwrites that we want to project the entire column
                // family. Instead, we do the projection at the end.
                // TODO: consider having a ScanUtil.addColumn and ScanUtil.addFamily to work
                // around this, as this code depends on this function being the last place where
                // columns are projected (which is currently true, but could change).
                projectedFamilies.add(Bytes.toBytes(cfName));
                if (tableRef.getTable().getType() == PTableType.INDEX && ((FamilyWildcardParseNode)node).isRewrite()) {
                    projectIndexColumnFamily(context, cfName, tableRef, resolveColumn, projectedExpressions, projectedColumns);
                } else {
                    projectTableColumnFamily(context, cfName, tableRef, resolveColumn, projectedExpressions, projectedColumns);
                }
            } else {
                if (node instanceof PhoenixRowTimestampParseNode) {
                    if (statement.isAggregate()) {
                        ExpressionCompiler.throwNonAggExpressionInAggException(node.toString());
                    }
                }
                Expression expression = node.accept(selectVisitor);
                projectedExpressions.add(expression);
                expression = coerceIfNecessary(index, targetColumns, expression);
                if (node instanceof BindParseNode) {
                    context.getBindManager().addParamMetaData((BindParseNode)node, expression);
                }
                if (!node.isStateless()) {
                    if (!selectVisitor.isAggregate() && statement.isAggregate()) {
                        ExpressionCompiler.throwNonAggExpressionInAggException(expression.toString());
                    }
                }

                String tableName = tableRef.getTableAlias() == null ?
                        (table.getName() == null ?
                                "" :
                                table.getName().getString()) :
                        tableRef.getTableAlias();
                String colName = SchemaUtil.normalizeIdentifier(aliasedNode.getNode().getAlias());
                String name = colName == null ? expression.toString() : colName;
                boolean isCaseSensitive = aliasedNode.getAlias() != null ?
                        aliasedNode.isCaseSensitve() :
                        (colName != null ?
                                SchemaUtil.isCaseSensitive(aliasedNode.getNode().getAlias()) :
                                selectVisitor.isCaseSensitive);
                if (null != aliasedNode.getAlias()){
                    projectedColumns.add(new ExpressionProjector(name, aliasedNode.getAlias(), tableName, expression, isCaseSensitive));
                } else {
                    projectedColumns.add(new ExpressionProjector(name, name, tableName, expression, isCaseSensitive));
                }
            }

            selectVisitor.reset();
            index++;
        }

        for (int i = serverParsedProjectedColumnRefs.size() - 1; i >= 0; i--) {
            Expression expression = serverParsedProjectedColumnRefs.get(i);
            Integer count = serverParsedExpressionCounts.get(expression);
            if (count != 0) {
                serverParsedKVRefs.remove(i);
                serverParsedKVFuncs.remove(i);
                serverParsedOldFuncs.remove(i);
            }
        }

        if (serverParsedKVFuncs.size() > 0 && serverParsedKVRefs.size() > 0) {
            String[]
                    scanAttributes =
                    new String[] { BaseScannerRegionObserverConstants.SPECIFIC_ARRAY_INDEX,
                            BaseScannerRegionObserverConstants.JSON_VALUE_FUNCTION,
                            BaseScannerRegionObserverConstants.JSON_QUERY_FUNCTION,
                            BaseScannerRegionObserverConstants.BSON_VALUE_FUNCTION};
            Map<String, Class> attributeToFunctionMap = new HashMap<String, Class>() {{
                put(scanAttributes[0], ArrayIndexFunction.class);
                put(scanAttributes[1], JsonValueFunction.class);
                put(scanAttributes[2], JsonQueryFunction.class);
                put(scanAttributes[3], BsonValueFunction.class);
            }};
            // This map is to keep track of the positions that get swapped with rearranging
            // the functions in the serialized data to server.
            Map<Integer, Integer> initialToShuffledPositionMap = new HashMap<>();
            Map<String, List<Expression>>
                    serverAttributeToFuncExpressionMap =
                    new HashMap<String, List<Expression>>() {{
                        for (String attribute : attributeToFunctionMap.keySet()) {
                            put(attribute, new ArrayList<>());
                        }
                    }};
            Map<String, List<KeyValueColumnExpression>>
                    serverAttributeToKVExpressionMap =
                    new HashMap<String, List<KeyValueColumnExpression>>() {{
                        for (String attribute : attributeToFunctionMap.keySet()) {
                            put(attribute, new ArrayList<>());
                        }
                    }};
            int counter = 0;
            for (String attribute : scanAttributes) {
                for (int i = 0; i < serverParsedKVFuncs.size(); i++) {
                    if (attributeToFunctionMap.get(attribute)
                            .isInstance(serverParsedKVFuncs.get(i))) {
                        initialToShuffledPositionMap.put(i, counter++);
                        serverAttributeToFuncExpressionMap.get(attribute)
                                .add(serverParsedKVFuncs.get(i));
                        serverAttributeToKVExpressionMap.get(attribute)
                                .add(serverParsedKVRefs.get(i));
                    }
                }
            }
            for (Map.Entry<String, Class> entry : attributeToFunctionMap.entrySet()) {
                if (serverAttributeToFuncExpressionMap.get(entry.getKey()).size() > 0) {
                    serializeServerParsedExpressionInformationAndSetInScan(context, entry.getKey(),
                            serverAttributeToFuncExpressionMap.get(entry.getKey()),
                            serverAttributeToKVExpressionMap.get(entry.getKey()));
                }
            }
            KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
            for (Expression expression : serverParsedKVRefs) {
                builder.addField(expression);
            }
            KeyValueSchema kvSchema = builder.build();
            ValueBitSet arrayIndexesBitSet = ValueBitSet.newInstance(kvSchema);
            builder = new KeyValueSchemaBuilder(0);
            for (Expression expression : serverParsedKVFuncs) {
                builder.addField(expression);
            }
            KeyValueSchema arrayIndexesSchema = builder.build();

            Map<Expression, Expression> replacementMap = new HashMap<>();
            for (int i = 0; i < serverParsedOldFuncs.size(); i++) {
                Expression function = serverParsedKVFuncs.get(i);
                replacementMap.put(serverParsedOldFuncs.get(i),
                        new ArrayIndexExpression(initialToShuffledPositionMap.get(i),
                                function.getDataType(), arrayIndexesBitSet, arrayIndexesSchema));

            }

            ReplaceArrayFunctionExpressionVisitor
                    visitor =
                    new ReplaceArrayFunctionExpressionVisitor(replacementMap);
            for (int i = 0; i < projectedColumns.size(); i++) {
                ExpressionProjector projector = projectedColumns.get(i);
                projectedColumns.set(i,
                        new ExpressionProjector(projector.getName(), projector.getLabel(),
                                tableRef.getTableAlias() == null ?
                                        (table.getName() == null ?
                                                "" :
                                                table.getName().getString()) :
                                        tableRef.getTableAlias(),
                                projector.getExpression().accept(visitor),
                                projector.isCaseSensitive()));
            }
        }

        boolean isProjectEmptyKeyValue = false;
        // Don't project known/declared column families into the scan if we want to support
        // surfacing dynamic columns in wildcard queries
        if (isWildcard && !wildcardIncludesDynamicCols) {
            projectAllColumnFamilies(table, scan);
        } else {
            isProjectEmptyKeyValue = where == null || LiteralExpression.isTrue(where) || where.requiresFinalEvaluation();
            for (byte[] family : projectedFamilies) {
                try {
                    if (table.getColumnFamily(family) != null) {
                        projectColumnFamily(table, scan, family);
                    }
                } catch (ColumnFamilyNotFoundException e) {
                    if (!IndexUtil.shouldIndexBeUsedForUncoveredQuery(tableRef)) {
                        throw e;
                    }
                }
            }
        }
        
        // TODO make estimatedByteSize more accurate by counting the joined columns.
        int estimatedKeySize = table.getRowKeySchema().getEstimatedValueLength();
        int estimatedByteSize = 0;
        for (Map.Entry<byte[],NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
            try {
                PColumnFamily family = table.getColumnFamily(entry.getKey());
                if (entry.getValue() == null) {
                    for (PColumn column : family.getColumns()) {
                        Integer maxLength = column.getMaxLength();
                        int byteSize = column.getDataType().isFixedWidth() ? maxLength == null ? column.getDataType().getByteSize() : maxLength : RowKeySchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
                        estimatedByteSize += SizedUtil.KEY_VALUE_SIZE + estimatedKeySize + byteSize;
                    }
                } else {
                    for (byte[] cq : entry.getValue()) {
                            PColumn column = family.getPColumnForColumnQualifier(cq);
                            // Continue: If an EMPTY_COLUMN is in the projection list,
                            // since the table column list does not contain the EMPTY_COLUMN
                            // no value is returned.
                            if (column == null) {
                                continue;
                            }
                            Integer maxLength = column.getMaxLength();
                            int byteSize = column.getDataType().isFixedWidth() ? maxLength == null ? column.getDataType().getByteSize() : maxLength : RowKeySchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
                            estimatedByteSize += SizedUtil.KEY_VALUE_SIZE + estimatedKeySize + byteSize;
                        }
                }
            } catch (ColumnFamilyNotFoundException e) {
                // Ignore as this can happen for local indexes when the data table has a column family, but there are no covered columns in the family
            }
        }
        return new RowProjector(projectedColumns, Math.max(estimatedKeySize, estimatedByteSize),
                isProjectEmptyKeyValue, resolver.hasUDFs(), isWildcard,
                wildcardIncludesDynamicCols);
    }

    private static void projectAllColumnFamilies(PTable table, Scan scan) {
        // Will project all known/declared column families
        scan.getFamilyMap().clear();
        for (PColumnFamily family : table.getColumnFamilies()) {
            scan.addFamily(family.getName().getBytes());
        }
    }

    // A replaced ArrayIndex function that retrieves the exact array value retrieved from the server
    static class ArrayIndexExpression extends BaseTerminalExpression {
        private final int position;
        private final PDataType type;
        private final ValueBitSet arrayIndexesBitSet;
        private final KeyValueSchema arrayIndexesSchema;

        public ArrayIndexExpression(int position, PDataType type, ValueBitSet arrayIndexesBitSet, KeyValueSchema arrayIndexesSchema) {
            this.position = position;
            this.type =  type;
            this.arrayIndexesBitSet = arrayIndexesBitSet;
            this.arrayIndexesSchema = arrayIndexesSchema;
        }

        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            if (!tuple.getValue(QueryConstants.ARRAY_VALUE_COLUMN_FAMILY, QueryConstants.ARRAY_VALUE_COLUMN_QUALIFIER,
                    ptr)) { 
              return false;
            }
            int maxOffset = ptr.getOffset() + ptr.getLength();
            arrayIndexesBitSet.or(ptr);
            arrayIndexesSchema.iterator(ptr, position, arrayIndexesBitSet);
            Boolean hasValue = arrayIndexesSchema.next(ptr, position, maxOffset, arrayIndexesBitSet);
            arrayIndexesBitSet.clear();
            if (hasValue == null) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            }
            return true;
        }

        @Override
        public PDataType getDataType() {
            return this.type;
        }

        @Override
        public <T> T accept(ExpressionVisitor<T> visitor) {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private static void serializeServerParsedExpressionInformationAndSetInScan(
            StatementContext context, String serverParsedExpressionAttribute,
            List<Expression> serverParsedKVFuncs,
            List<KeyValueColumnExpression> serverParsedKVRefs) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            // Write the KVRef size followed by the keyvalues that needs to be of
            // type arrayindex or json function based on serverParsedExpressionAttribute
            WritableUtils.writeVInt(output, serverParsedKVRefs.size());
            for (Expression expression : serverParsedKVRefs) {
                    expression.write(output);
            }
            // then write the number of arrayindex or json functions followed
            // by the expression itself
            WritableUtils.writeVInt(output, serverParsedKVFuncs.size());
            for (Expression expression : serverParsedKVFuncs) {
                    expression.write(output);
            }
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        context.getScan().setAttribute(serverParsedExpressionAttribute, stream.toByteArray());
    }

    private static class SelectClauseVisitor extends ExpressionCompiler {

        /**
         * Track whether or not the projection expression is case sensitive. We use this
         * information to determine whether or not we normalize the column name passed
         */
        private boolean isCaseSensitive;
        private int elementCount;
        // Looks at PHOENIX-2160 for the context and use of the below variables.
        // These are used for reference counting and converting to KeyValueColumnExpressions
        private List<KeyValueColumnExpression> serverParsedKVRefs;
        private List<Expression> serverParsedKVFuncs;
        private List<Expression> serverParsedOldFuncs;
        private List<ProjectedColumnExpression> serverParsedProjectedColumnRefs;
        private Map<Expression, Integer> serverParsedExpressionCounts;
        private SelectStatement statement;

        private SelectClauseVisitor(StatementContext context, GroupBy groupBy,
                List<KeyValueColumnExpression> serverParsedKVRefs,
                List<Expression> serverParsedKVFuncs,
                Map<Expression, Integer> serverParsedExpressionCounts,
                List<ProjectedColumnExpression> serverParsedProjectedColumnRefs,
                List<Expression> serverParsedOldFuncs, SelectStatement statement) {
            super(context, groupBy);
            this.serverParsedKVRefs = serverParsedKVRefs;
            this.serverParsedKVFuncs = serverParsedKVFuncs;
            this.serverParsedOldFuncs = serverParsedOldFuncs;
            this.serverParsedExpressionCounts = serverParsedExpressionCounts;
            this.serverParsedProjectedColumnRefs = serverParsedProjectedColumnRefs;
            this.statement = statement;
            reset();
        }

        @Override
        public void reset() {
            super.reset();
            elementCount = 0;
            isCaseSensitive = true;
        }
        
        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            ColumnRef ref = super.resolveColumn(node);
            isCaseSensitive = isCaseSensitive && node.isCaseSensitive();
            return ref;
        }

        @Override
        public Expression visit(ColumnParseNode node) throws SQLException {
            Expression expression = super.visit(node);
            if (parseOnServer(expression)) {
                Integer count = serverParsedExpressionCounts.get(expression);
                serverParsedExpressionCounts.put(expression, count != null ? (count + 1) : 1);
            }
            return expression;
        }

        private static boolean parseOnServer(Expression expression) {
            return expression.getDataType().isArrayType() || expression.getDataType()
                    .equals(PJson.INSTANCE) || expression.getDataType().equals(PBson.INSTANCE);
        }

        @Override
        public void addElement(List<Expression> l, Expression element) {
            elementCount++;
            isCaseSensitive &= elementCount == 1;
            super.addElement(l, element);
        }
        
        @Override
        public Expression visit(SequenceValueParseNode node) throws SQLException {
            if (aggregateFunction != null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_USE_OF_NEXT_VALUE_FOR)
                .setSchemaName(node.getTableName().getSchemaName())
                .setTableName(node.getTableName().getTableName()).build().buildException();
            }
            return context.getSequenceManager().newSequenceReference(node);
        }
        
        @Override
        public Expression visitLeave(FunctionParseNode node, final List<Expression> children) throws SQLException {

            // this need not be done for group by clause with array or json. Hence, the below check
            if (!statement.isAggregate() && (ArrayIndexFunction.NAME.equals(
                    node.getName()) || isJsonFunction(node) || isBsonFunction(node)) &&
                    children.get(0) instanceof ProjectedColumnExpression) {
                final List<KeyValueColumnExpression> indexKVs = Lists.newArrayList();
                final List<ProjectedColumnExpression> indexProjectedColumns = Lists.newArrayList();
                final List<Expression> copyOfChildren = new ArrayList<>(children);
                // Create anon visitor to find reference to array or json in a generic way
                children.get(0).accept(new ProjectedColumnExpressionVisitor() {
                    @Override
                    public Void visit(ProjectedColumnExpression expression) {
                        if (expression.getDataType().isArrayType() || expression.getDataType()
                            .equals(PJson.INSTANCE) || expression.getDataType()
                            .equals(PBson.INSTANCE)) {
                            indexProjectedColumns.add(expression);
                            PColumn col = expression.getColumn();
                            // hack'ish... For covered columns with local indexes we defer to the server.
                            if (col instanceof ProjectedColumn && ((ProjectedColumn) col).getSourceColumnRef() instanceof IndexUncoveredDataColumnRef) {
                                return null;
                            }
                            PTable table = context.getCurrentTable().getTable();
                            KeyValueColumnExpression keyValueColumnExpression;
                            if (table.getImmutableStorageScheme() != ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                                keyValueColumnExpression =
                                        new SingleCellColumnExpression(col,
                                                col.getName().getString(),
                                                table.getEncodingScheme(),
                                                table.getImmutableStorageScheme());
                            } else {
                                keyValueColumnExpression = new KeyValueColumnExpression(col);
                            }
                            indexKVs.add(keyValueColumnExpression);
                            copyOfChildren.set(0, keyValueColumnExpression);
                            Integer count = serverParsedExpressionCounts.get(expression);
                            serverParsedExpressionCounts.put(expression,
                                    count != null ? (count - 1) : -1);
                        }
                        return null;
                    }
                });

                Expression func = super.visitLeave(node, children);
                // Add the keyvalues which is of type array or json
                if (!indexKVs.isEmpty()) {
                    serverParsedKVRefs.addAll(indexKVs);
                    serverParsedProjectedColumnRefs.addAll(indexProjectedColumns);
                    Expression funcModified = super.visitLeave(node, copyOfChildren);
                    // Track the array index or json function also
                    serverParsedKVFuncs.add(funcModified);
                    serverParsedOldFuncs.add(func);
                }
                return func;
            } else {
                return super.visitLeave(node, children);
            }
        }
    }

    private static boolean isJsonFunction(FunctionParseNode node) {
        return JsonValueFunction.NAME.equals(node.getName()) || JsonQueryFunction.NAME.equals(
                node.getName());
    }

    private static boolean isBsonFunction(FunctionParseNode node) {
        return BsonValueFunction.NAME.equals(node.getName());
    }
}
