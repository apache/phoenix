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
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
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
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.LocalIndexDataColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;

import com.google.common.collect.Lists;


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
        return compile(context, statement, groupBy, Collections.<PColumn>emptyList(), 
                NULL_EXPRESSION// Pass null expression because we don't want empty key value to be projected
                );
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
                expression = LiteralExpression.newConstant(column.getDataType().toObject(ptr), expression.getDataType());
            }
            projectedExpressions.add(expression);
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            projectedColumns.add(new ExpressionProjector(colName, tableRef.getTableAlias() == null ? table.getName().getString() : tableRef.getTableAlias(), expression, isCaseSensitive));
        }
    }
    
    private static void projectAllIndexColumns(StatementContext context, TableRef tableRef, boolean resolveColumn, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns, List<? extends PDatum> targetColumns) throws SQLException {
        ColumnResolver resolver = context.getResolver();
        PTable index = tableRef.getTable();
        int projectedOffset = projectedExpressions.size();
        PhoenixConnection conn = context.getConnection();
        PName tenantId = conn.getTenantId();
        String tableName = index.getParentName().getString();
        PTable dataTable = null;
        try {
        	dataTable = conn.getTable(new PTableKey(tenantId, tableName));
        } catch (TableNotFoundException e) {
            if (tenantId != null) {
                // Check with null tenantId
            	dataTable = conn.getTable(new PTableKey(null, tableName));
            }
            else {
            	throw e;
            }
        }
        int tableOffset = dataTable.getBucketNum() == null ? 0 : 1;
        int minTablePKOffset = getMinPKOffset(dataTable, tenantId);
        int minIndexPKOffset = getMinPKOffset(index, tenantId);
        if (index.getIndexType() != IndexType.LOCAL) {
            if (index.getColumns().size()-minIndexPKOffset != dataTable.getColumns().size()-minTablePKOffset) {
                // We'll end up not using this by the optimizer, so just throw
                throw new ColumnNotFoundException(WildcardParseNode.INSTANCE.toString());
            }
        }
        for (int i = tableOffset, j = tableOffset; i < dataTable.getColumns().size(); i++) {
            PColumn column = dataTable.getColumns().get(i);
            // Skip tenant ID column (which may not be the first column, but is the first PK column)
            if (SchemaUtil.isPKColumn(column) && j++ < minTablePKOffset) {
                tableOffset++;
                continue;
            }
            PColumn tableColumn = dataTable.getColumns().get(i);
            String indexColName = IndexUtil.getIndexColumnName(tableColumn);
            PColumn indexColumn = null;
            ColumnRef ref = null;
            try {
                indexColumn = index.getColumnForColumnName(indexColName);
                ref = new ColumnRef(tableRef, indexColumn.getPosition());
            } catch (ColumnNotFoundException e) {
                if (index.getIndexType() == IndexType.LOCAL) {
                    try {
                        ref = new LocalIndexDataColumnRef(context, indexColName);
                        indexColumn = ref.getColumn();
                    } catch (ColumnFamilyNotFoundException c) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
            String colName = tableColumn.getName().getString();
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
            ExpressionProjector projector = new ExpressionProjector(colName, tableRef.getTableAlias() == null ? dataTable.getName().getString() : tableRef.getTableAlias(), expression, isCaseSensitive);
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
            projectedColumns.add(new ExpressionProjector(colName, tableRef.getTableAlias() == null ? 
                    table.getName().getString() : tableRef.getTableAlias(), expression, isCaseSensitive));
        }
    }

    private static void projectIndexColumnFamily(StatementContext context, String cfName, TableRef tableRef, boolean resolveColumn, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns) throws SQLException {
        PTable index = tableRef.getTable();
        PhoenixConnection conn = context.getConnection();
        String tableName = index.getParentName().getString();
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), tableName));
        PColumnFamily pfamily = table.getColumnFamily(cfName);
        for (PColumn column : pfamily.getColumns()) {
            String indexColName = IndexUtil.getIndexColumnName(column);
            PColumn indexColumn = null;
            ColumnRef ref = null;
            String indexColumnFamily = null;
            try {
                indexColumn = index.getColumnForColumnName(indexColName);
                ref = new ColumnRef(tableRef, indexColumn.getPosition());
                indexColumnFamily = indexColumn.getFamilyName() == null ? null : indexColumn.getFamilyName().getString();
            } catch (ColumnNotFoundException e) {
                if (index.getIndexType() == IndexType.LOCAL) {
                    try {
                        ref = new LocalIndexDataColumnRef(context, indexColName);
                        indexColumn = ref.getColumn();
                        indexColumnFamily =
                                indexColumn.getFamilyName() == null ? null
                                        : (index.getIndexType() == IndexType.LOCAL ? IndexUtil
                                                .getLocalIndexColumnFamily(indexColumn
                                                        .getFamilyName().getString()) : indexColumn
                                                .getFamilyName().getString());
                    } catch (ColumnFamilyNotFoundException c) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
            if (resolveColumn) {
                ref = context.getResolver().resolveColumn(index.getTableName().getString(), indexColumnFamily, indexColName);
            }
            Expression expression = ref.newColumnExpression();
            projectedExpressions.add(expression);
            String colName = column.getName().toString();
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            projectedColumns.add(new ExpressionProjector(colName, 
                    tableRef.getTableAlias() == null ? table.getName().getString() : tableRef.getTableAlias(), expression, isCaseSensitive));
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
     * @param statement TODO
     * @param groupBy compiled GROUP BY clause
     * @param targetColumns list of columns, parallel to aliasedNodes, that are being set for an
     * UPSERT SELECT statement. Used to coerce expression types to the expected target type.
     * @return projector used to access row values during scan
     * @throws SQLException 
     */
    public static RowProjector compile(StatementContext context, SelectStatement statement, GroupBy groupBy, List<? extends PDatum> targetColumns, Expression where) throws SQLException {
        List<KeyValueColumnExpression> arrayKVRefs = new ArrayList<KeyValueColumnExpression>();
        List<ProjectedColumnExpression> arrayProjectedColumnRefs = new ArrayList<ProjectedColumnExpression>();
        List<Expression> arrayKVFuncs = new ArrayList<Expression>();
        List<Expression> arrayOldFuncs = new ArrayList<Expression>();
        Map<Expression, Integer> arrayExpressionCounts = new HashMap<>();
        List<AliasedNode> aliasedNodes = statement.getSelect();
        // Setup projected columns in Scan
        SelectClauseVisitor selectVisitor = new SelectClauseVisitor(context, groupBy, arrayKVRefs, arrayKVFuncs, arrayExpressionCounts, arrayProjectedColumnRefs, arrayOldFuncs, statement);
        List<ExpressionProjector> projectedColumns = new ArrayList<ExpressionProjector>();
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
                String columnAlias = aliasedNode.getAlias() != null ? aliasedNode.getAlias() : SchemaUtil.normalizeIdentifier(aliasedNode.getNode().getAlias());
                boolean isCaseSensitive = aliasedNode.getAlias() != null ? aliasedNode.isCaseSensitve() : (columnAlias != null ? SchemaUtil.isCaseSensitive(aliasedNode.getNode().getAlias()) : selectVisitor.isCaseSensitive);
                String name = columnAlias == null ? expression.toString() : columnAlias;
                projectedColumns.add(new ExpressionProjector(name, tableRef.getTableAlias() == null ? (table.getName() == null ? "" : table.getName().getString()) : tableRef.getTableAlias(), expression, isCaseSensitive));
            }

            selectVisitor.reset();
            index++;
        }

        for (int i = arrayProjectedColumnRefs.size() - 1; i >= 0; i--) {
            Expression expression = arrayProjectedColumnRefs.get(i);
            Integer count = arrayExpressionCounts.get(expression);
            if (count != 0) {
                arrayKVRefs.remove(i);
                arrayKVFuncs.remove(i);
                arrayOldFuncs.remove(i);
            }
        }

        if (arrayKVFuncs.size() > 0 && arrayKVRefs.size() > 0) {
            serailizeArrayIndexInformationAndSetInScan(context, arrayKVFuncs, arrayKVRefs);
            KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
            for (Expression expression : arrayKVRefs) {
                builder.addField(expression);
            }
            KeyValueSchema kvSchema = builder.build();
            ValueBitSet arrayIndexesBitSet = ValueBitSet.newInstance(kvSchema);
            builder = new KeyValueSchemaBuilder(0);
            for (Expression expression : arrayKVFuncs) {
                builder.addField(expression);
            }
            KeyValueSchema arrayIndexesSchema = builder.build();

            Map<Expression, Expression> replacementMap = new HashMap<>();
            for(int i = 0; i < arrayOldFuncs.size(); i++){
                Expression function =arrayKVFuncs.get(i);
                replacementMap.put(arrayOldFuncs.get(i), new ArrayIndexExpression(i, function.getDataType(), arrayIndexesBitSet, arrayIndexesSchema));
            }

            ReplaceArrayFunctionExpressionVisitor visitor = new ReplaceArrayFunctionExpressionVisitor(replacementMap);
            for (int i = 0; i < projectedColumns.size(); i++) {
                ExpressionProjector projector = projectedColumns.get(i);
                projectedColumns.set(i, new ExpressionProjector(projector.getName(), tableRef.getTableAlias() == null ? (table.getName() == null ? "" : table.getName().getString()) : tableRef.getTableAlias(), projector.getExpression().accept(visitor), projector.isCaseSensitive()));
            }
        }

        // TODO make estimatedByteSize more accurate by counting the joined columns.
        int estimatedKeySize = table.getRowKeySchema().getEstimatedValueLength();
        int estimatedByteSize = 0;
        for (Map.Entry<byte[],NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
            PColumnFamily family = table.getColumnFamily(entry.getKey());
            if (entry.getValue() == null) {
                for (PColumn column : family.getColumns()) {
                    Integer maxLength = column.getMaxLength();
                    int byteSize = column.getDataType().isFixedWidth() ? maxLength == null ? column.getDataType().getByteSize() : maxLength : RowKeySchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
                    estimatedByteSize += SizedUtil.KEY_VALUE_SIZE + estimatedKeySize + byteSize;
                }
            } else {
                for (byte[] cq : entry.getValue()) {
                    //if (!Bytes.equals(cq, ByteUtil.EMPTY_BYTE_ARRAY) || cq.length > 0) {
                        PColumn column = family.getPColumnForColumnQualifier(cq);
                        Integer maxLength = column.getMaxLength();
                        int byteSize = column.getDataType().isFixedWidth() ? maxLength == null ? column.getDataType().getByteSize() : maxLength : RowKeySchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
                        estimatedByteSize += SizedUtil.KEY_VALUE_SIZE + estimatedKeySize + byteSize;
                    }
                //}
            }
        }
        boolean isProjectEmptyKeyValue = false;
        if (isWildcard) {
            projectAllColumnFamilies(table, scan);
        } else {
            isProjectEmptyKeyValue = where == null || LiteralExpression.isTrue(where) || where.requiresFinalEvaluation();
            for (byte[] family : projectedFamilies) {
                projectColumnFamily(table, scan, family);
            }
        }
        return new RowProjector(projectedColumns, estimatedByteSize, isProjectEmptyKeyValue, resolver.hasUDFs(), isWildcard);
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
    private static void serailizeArrayIndexInformationAndSetInScan(StatementContext context, List<Expression> arrayKVFuncs,
            List<KeyValueColumnExpression> arrayKVRefs) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            // Write the arrayKVRef size followed by the keyvalues that needs to be of type arrayindex function
            WritableUtils.writeVInt(output, arrayKVRefs.size());
            for (Expression expression : arrayKVRefs) {
                    expression.write(output);
            }
            // then write the number of arrayindex functions followeed by the expression itself
            WritableUtils.writeVInt(output, arrayKVFuncs.size());
            for (Expression expression : arrayKVFuncs) {
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
        context.getScan().setAttribute(BaseScannerRegionObserver.SPECIFIC_ARRAY_INDEX, stream.toByteArray());
    }

    private static class SelectClauseVisitor extends ExpressionCompiler {

        /**
         * Track whether or not the projection expression is case sensitive. We use this
         * information to determine whether or not we normalize the column name passed
         */
        private boolean isCaseSensitive;
        private int elementCount;
        private List<KeyValueColumnExpression> arrayKVRefs;
        private List<Expression> arrayKVFuncs;
        private List<Expression> arrayOldFuncs;
        private List<ProjectedColumnExpression> arrayProjectedColumnRefs;
        private Map<Expression, Integer> arrayExpressionCounts;
        private SelectStatement statement; 
        
        private SelectClauseVisitor(StatementContext context, GroupBy groupBy, 
                List<KeyValueColumnExpression> arrayKVRefs, List<Expression> arrayKVFuncs, Map<Expression, Integer> arrayExpressionCounts, List<ProjectedColumnExpression> arrayProjectedColumnRefs, List<Expression> arrayOldFuncs, SelectStatement statement) {
            super(context, groupBy);
            this.arrayKVRefs = arrayKVRefs;
            this.arrayKVFuncs = arrayKVFuncs;
            this.arrayOldFuncs = arrayOldFuncs;
            this.arrayExpressionCounts = arrayExpressionCounts;
            this.arrayProjectedColumnRefs = arrayProjectedColumnRefs;
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
            if (expression.getDataType().isArrayType()) {
                Integer count = arrayExpressionCounts.get(expression);
                arrayExpressionCounts.put(expression, count != null ? (count + 1) : 1);
            }
            return expression;
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

            // this need not be done for group by clause with array. Hence the below check
            if (!statement.isAggregate() && ArrayIndexFunction.NAME.equals(node.getName()) && children.get(0) instanceof ProjectedColumnExpression) {
                 final List<KeyValueColumnExpression> indexKVs = Lists.newArrayList();
                 final List<ProjectedColumnExpression> indexProjectedColumns = Lists.newArrayList();
                 final List<Expression> copyOfChildren = new ArrayList<>(children);
                 // Create anon visitor to find reference to array in a generic way
                 children.get(0).accept(new ProjectedColumnExpressionVisitor() {
                     @Override
                     public Void visit(ProjectedColumnExpression expression) {
                         if (expression.getDataType().isArrayType()) {
                             indexProjectedColumns.add(expression);
                             PColumn col = expression.getColumn();
                             PTable table = context.getCurrentTable().getTable();
                             KeyValueColumnExpression keyValueColumnExpression;
                             if (table.getImmutableStorageScheme() != ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                                 keyValueColumnExpression = new SingleCellColumnExpression(col, col.getName().getString(), table.getEncodingScheme());
                             } else {
                                 keyValueColumnExpression = new KeyValueColumnExpression(col);
                             }
                             indexKVs.add(keyValueColumnExpression);
                             copyOfChildren.set(0, keyValueColumnExpression);
                             Integer count = arrayExpressionCounts.get(expression);
                             arrayExpressionCounts.put(expression, count != null ? (count - 1) : -1);
                         }
                         return null;
                     }
                 });

                 Expression func = super.visitLeave(node,children);
                 // Add the keyvalues which is of type array
                 if (!indexKVs.isEmpty()) {
                    arrayKVRefs.addAll(indexKVs);
                    arrayProjectedColumnRefs.addAll(indexProjectedColumns);
                    Expression funcModified = super.visitLeave(node, copyOfChildren);
                    // Track the array index function also
                    arrayKVFuncs.add(funcModified);
                    arrayOldFuncs.add(func);
                }
                return func;
            } else {
                return super.visitLeave(node,children);
            }
        }
    }
}
