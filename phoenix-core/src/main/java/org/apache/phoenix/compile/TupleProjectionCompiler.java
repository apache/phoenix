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
import static org.apache.phoenix.query.QueryConstants.VALUE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.util.IndexUtil.isHintedGlobalIndex;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.IndexDataColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.EncodedCQCounter;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class TupleProjectionCompiler {
    public static final PName PROJECTED_TABLE_SCHEMA = PNameFactory.newName(".");
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    public static PTable createProjectedTable(SelectStatement select, StatementContext context) throws SQLException {
        Preconditions.checkArgument(!select.isJoin());
        // Non-group-by or group-by aggregations will create its own projected result.
        if (select.getInnerSelectStatement() != null 
                || select.getFrom() == null
                || select.isAggregate() 
                || select.isDistinct()
                || (context.getResolver().getTables().get(0).getTable().getType() != PTableType.TABLE
                && context.getResolver().getTables().get(0).getTable().getType() != PTableType.INDEX && context.getResolver().getTables().get(0).getTable().getType() != PTableType.VIEW))
            return null;
        
        List<PColumn> projectedColumns = new ArrayList<PColumn>();
        boolean isWildcard = false;
        Set<String> families = new HashSet<String>();
        ColumnRefVisitor visitor = new ColumnRefVisitor(context);
        TableRef tableRef = context.getCurrentTable();
        PTable table = tableRef.getTable();

        for (AliasedNode aliasedNode : select.getSelect()) {
            ParseNode node = aliasedNode.getNode();
            if (node instanceof WildcardParseNode) {
                if (((WildcardParseNode) node).isRewrite()) {
                    TableRef parentTableRef = FromCompiler.getResolver(
                            NODE_FACTORY.namedTable(null, TableName.create(table.getSchemaName().getString(), 
                                    table.getParentTableName().getString())), context.getConnection()).resolveTable(
                            table.getSchemaName().getString(),
                            table.getParentTableName().getString());
                    for (PColumn column : parentTableRef.getTable().getColumns()) {
                        // don't attempt to rewrite the parents SALTING COLUMN
                        if (column == SaltingUtil.SALTING_COLUMN) {
                            continue;
                        }
                        NODE_FACTORY.column(null, '"' + IndexUtil.getIndexColumnName(column) + '"', null).accept(visitor);
                    }
                }
                isWildcard = true;
            } else if (node instanceof FamilyWildcardParseNode) {
                FamilyWildcardParseNode familyWildcardNode = (FamilyWildcardParseNode) node;
                String familyName = familyWildcardNode.getName();
                if (familyWildcardNode.isRewrite()) {
                    TableRef parentTableRef = FromCompiler.getResolver(
                            NODE_FACTORY.namedTable(null, TableName.create(table.getSchemaName().getString(), 
                                    table.getParentTableName().getString())), context.getConnection()).resolveTable(
                            table.getSchemaName().getString(),
                            table.getParentTableName().getString());
                    for (PColumn column : parentTableRef.getTable().getColumnFamily(familyName).getColumns()) {
                        NODE_FACTORY.column(null, '"' + IndexUtil.getIndexColumnName(column) + '"', null).accept(visitor);
                    }
                }else{
                    for (PColumn column : table.getColumnFamily(familyName).getColumns()) {
                        NODE_FACTORY.column(TableName.create(null, familyName), '"' + column.getName().getString() + '"', null).accept(visitor);
                    }
                }
                families.add(familyName);
            } else {
                node.accept(visitor);
            }
        }
        if (!isWildcard) {
            for (OrderByNode orderBy : select.getOrderBy()) {
                orderBy.getNode().accept(visitor);
            }
        }

        boolean hasSaltingColumn = table.getBucketNum() != null;
        int position = hasSaltingColumn ? 1 : 0;
        // Always project PK columns first in case there are some PK columns added by alter table.
        for (int i = position; i < table.getPKColumns().size(); i++) {
            PColumn sourceColumn = table.getPKColumns().get(i);
            ColumnRef sourceColumnRef = new ColumnRef(tableRef, sourceColumn.getPosition());
            PColumn column = new ProjectedColumn(sourceColumn.getName(), sourceColumn.getFamilyName(), 
                    position++, sourceColumn.isNullable(), sourceColumnRef, null);
            projectedColumns.add(column);
        }

        List<ColumnRef> nonPkColumnRefList = new ArrayList<ColumnRef>(visitor.nonPkColumnRefSet);
        for (PColumn sourceColumn : table.getColumns()) {
            if (SchemaUtil.isPKColumn(sourceColumn))
                continue;
            ColumnRef sourceColumnRef = new ColumnRef(tableRef, sourceColumn.getPosition());
            if (!isWildcard 
                    && !visitor.nonPkColumnRefSet.contains(sourceColumnRef)
                    && !families.contains(sourceColumn.getFamilyName().getString()))
                continue;

            PColumn column = new ProjectedColumn(sourceColumn.getName(), sourceColumn.getFamilyName(),
                    visitor.nonPkColumnRefSet.contains(sourceColumnRef)
                            ? position + nonPkColumnRefList.indexOf(sourceColumnRef) : position++,
                    sourceColumn.isNullable(), sourceColumnRef, sourceColumn.getColumnQualifierBytes());

            projectedColumns.add(column);
            // Wildcard or FamilyWildcard will be handled by ProjectionCompiler.
            if (!isWildcard && !families.contains(sourceColumn.getFamilyName().toString())) {
            	EncodedColumnsUtil.setColumns(column, table, context.getScan());
            }
        }
        // add IndexDataColumnRef
        position = projectedColumns.size();
        for (IndexDataColumnRef sourceColumnRef : visitor.indexColumnRefSet) {
            PColumn column = new ProjectedColumn(sourceColumnRef.getColumn().getName(), 
                    sourceColumnRef.getColumn().getFamilyName(), position++, 
                    sourceColumnRef.getColumn().isNullable(), sourceColumnRef, sourceColumnRef.getColumn().getColumnQualifierBytes());
            projectedColumns.add(column);
        }
        if (!visitor.indexColumnRefSet.isEmpty()
                && tableRef.isHinted()) {
            context.setUncoveredIndex(true);
        }
        return PTableImpl.builderWithColumns(table, projectedColumns)
                .setType(PTableType.PROJECTED)
                .setBaseColumnCount(BASE_TABLE_BASE_COLUMN_COUNT)
                .setExcludedColumns(ImmutableList.of())
                .setPhysicalNames(ImmutableList.of())
                .build();
    }
    
    public static PTable createProjectedTable(TableRef tableRef, List<ColumnRef> sourceColumnRefs, boolean retainPKColumns) throws SQLException {
        PTable table = tableRef.getTable();
        List<PColumn> projectedColumns = new ArrayList<PColumn>();
        int position = table.getBucketNum() != null ? 1 : 0;
        for (int i = retainPKColumns ? position : 0; i < sourceColumnRefs.size(); i++) {
            ColumnRef sourceColumnRef = sourceColumnRefs.get(i);
            PColumn sourceColumn = sourceColumnRef.getColumn();
            String colName = sourceColumn.getName().getString();
            String aliasedName = tableRef.getTableAlias() == null ? 
                      SchemaUtil.getColumnName(table.getName().getString(), colName) 
                    : SchemaUtil.getColumnName(tableRef.getTableAlias(), colName);
            PName familyName =  SchemaUtil.isPKColumn(sourceColumn) ? (retainPKColumns ? null : PNameFactory.newName(VALUE_COLUMN_FAMILY)) : sourceColumn.getFamilyName();
            // If we're not retaining the PK columns, then we should switch columns to be nullable
            PColumn column = new ProjectedColumn(PNameFactory.newName(aliasedName), familyName, 
                    position++, sourceColumn.isNullable(), sourceColumnRef, sourceColumn.getColumnQualifierBytes());
            projectedColumns.add(column);
        }
        EncodedCQCounter cqCounter = EncodedCQCounter.NULL_COUNTER;
        if (EncodedColumnsUtil.usesEncodedColumnNames(table)) {
            cqCounter = EncodedCQCounter.copy(table.getEncodedCQCounter());
        }
        return new PTableImpl.Builder()
                .setType(PTableType.PROJECTED)
                .setTimeStamp(table.getTimeStamp())
                .setIndexDisableTimestamp(table.getIndexDisableTimestamp())
                .setSequenceNumber(table.getSequenceNumber())
                .setImmutableRows(table.isImmutableRows())
                .setDisableWAL(table.isWALDisabled())
                .setMultiTenant(table.isMultiTenant())
                .setStoreNulls(table.getStoreNulls())
                .setViewType(table.getViewType())
                .setViewIndexIdType(table.getviewIndexIdType())
                .setViewIndexId(table.getViewIndexId())
                .setTransactionProvider(table.getTransactionProvider())
                .setUpdateCacheFrequency(table.getUpdateCacheFrequency())
                .setNamespaceMapped(table.isNamespaceMapped())
                .setAutoPartitionSeqName(table.getAutoPartitionSeqName())
                .setAppendOnlySchema(table.isAppendOnlySchema())
                .setImmutableStorageScheme(table.getImmutableStorageScheme())
                .setQualifierEncodingScheme(table.getEncodingScheme())
                .setBaseColumnCount(BASE_TABLE_BASE_COLUMN_COUNT)
                .setEncodedCQCounter(cqCounter)
                .setUseStatsForParallelization(table.useStatsForParallelization())
                .setExcludedColumns(ImmutableList.of())
                .setTenantId(table.getTenantId())
                .setSchemaName(PROJECTED_TABLE_SCHEMA)
                .setTableName(table.getTableName())
                .setPkName(table.getPKName())
                .setRowKeyOrderOptimizable(table.rowKeyOrderOptimizable())
                .setBucketNum(table.getBucketNum())
                .setIndexes(Collections.emptyList())
                .setPhysicalNames(ImmutableList.of())
                .setColumns(projectedColumns)
                .build();
    }

    // For extracting column references from single select statement
    private static class ColumnRefVisitor extends StatelessTraverseAllParseNodeVisitor {
        private final StatementContext context;
        private final LinkedHashSet<ColumnRef> nonPkColumnRefSet;
        private final LinkedHashSet<IndexDataColumnRef> indexColumnRefSet;
        
        private ColumnRefVisitor(StatementContext context) {
            this.context = context;
            this.nonPkColumnRefSet = new LinkedHashSet<ColumnRef>();
            this.indexColumnRefSet = new LinkedHashSet<IndexDataColumnRef>();
        }

        @Override
        public Void visit(ColumnParseNode node) throws SQLException {
            try {
                ColumnRef resolveColumn = context.getResolver().resolveColumn(node.getSchemaName(), node.getTableName(),
                        node.getName());
                if (!SchemaUtil.isPKColumn(resolveColumn.getColumn())) {
                    nonPkColumnRefSet.add(resolveColumn);
                }
            } catch (ColumnNotFoundException e) {
                if (context.getCurrentTable().getTable().getIndexType() == PTable.IndexType.LOCAL
                        || isHintedGlobalIndex(context.getCurrentTable())) {
                    try {
                        context.setUncoveredIndex(true);
                        indexColumnRefSet.add(new IndexDataColumnRef(context,
                                context.getCurrentTable(), node.getName()));
                    } catch (ColumnFamilyNotFoundException c) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
            return null;
        }        
    }
}
