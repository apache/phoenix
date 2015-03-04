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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.phoenix.execute.TupleProjector;
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
import org.apache.phoenix.schema.LocalIndexDataColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Preconditions;

public class TupleProjectionCompiler {
    public static final PName PROJECTED_TABLE_SCHEMA = PNameFactory.newName(".");
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    public static PTable createProjectedTable(SelectStatement select, StatementContext context) throws SQLException {
        Preconditions.checkArgument(!select.isJoin());
        // Non-group-by or group-by aggregations will create its own projected result.
        if (select.getInnerSelectStatement() != null 
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
                    position++, sourceColumn.isNullable(), sourceColumnRef);
            projectedColumns.add(column);
        }
        for (PColumn sourceColumn : table.getColumns()) {
            if (SchemaUtil.isPKColumn(sourceColumn))
                continue;
            ColumnRef sourceColumnRef = new ColumnRef(tableRef, sourceColumn.getPosition());
            if (!isWildcard 
                    && !visitor.columnRefSet.contains(sourceColumnRef)
                    && !families.contains(sourceColumn.getFamilyName().getString()))
                continue;
            PColumn column = new ProjectedColumn(sourceColumn.getName(), sourceColumn.getFamilyName(), 
                    position++, sourceColumn.isNullable(), sourceColumnRef);
            projectedColumns.add(column);
            // Wildcard or FamilyWildcard will be handled by ProjectionCompiler.
            if (!isWildcard && !families.contains(sourceColumn.getFamilyName())) {
                context.getScan().addColumn(sourceColumn.getFamilyName().getBytes(), sourceColumn.getName().getBytes());
            }
        }
        // add LocalIndexDataColumnRef
        for (LocalIndexDataColumnRef sourceColumnRef : visitor.localIndexColumnRefSet) {
            PColumn column = new ProjectedColumn(sourceColumnRef.getColumn().getName(), 
                    sourceColumnRef.getColumn().getFamilyName(), position++, 
                    sourceColumnRef.getColumn().isNullable(), sourceColumnRef);
            projectedColumns.add(column);
        }
        
        return PTableImpl.makePTable(table.getTenantId(), table.getSchemaName(), table.getName(), PTableType.PROJECTED,
                table.getIndexState(), table.getTimeStamp(), table.getSequenceNumber(), table.getPKName(),
                table.getBucketNum(), projectedColumns, table.getParentSchemaName(),
                table.getParentName(), table.getIndexes(), table.isImmutableRows(), Collections.<PName>emptyList(), null, null,
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(),
                table.getIndexType());
    }

    public static PTable createProjectedTable(TableRef tableRef, List<ColumnRef> sourceColumnRefs, boolean retainPKColumns) throws SQLException {
        PTable table = tableRef.getTable();
        boolean hasSaltingColumn = retainPKColumns && table.getBucketNum() != null;
        List<PColumn> projectedColumns = new ArrayList<PColumn>();
        int position = hasSaltingColumn ? 1 : 0;
        for (int i = position; i < sourceColumnRefs.size(); i++) {
            ColumnRef sourceColumnRef = sourceColumnRefs.get(i);
            PColumn sourceColumn = sourceColumnRef.getColumn();
            String colName = sourceColumn.getName().getString();
            String aliasedName = tableRef.getTableAlias() == null ? 
                      SchemaUtil.getColumnName(table.getName().getString(), colName) 
                    : SchemaUtil.getColumnName(tableRef.getTableAlias(), colName);

            PColumn column = new ProjectedColumn(PNameFactory.newName(aliasedName), 
                    retainPKColumns && SchemaUtil.isPKColumn(sourceColumn) ? 
                            null : PNameFactory.newName(TupleProjector.VALUE_COLUMN_FAMILY), 
                    position++, sourceColumn.isNullable(), sourceColumnRef);
            projectedColumns.add(column);
        }
        return PTableImpl.makePTable(table.getTenantId(), PROJECTED_TABLE_SCHEMA, table.getName(), PTableType.PROJECTED,
                    null, table.getTimeStamp(), table.getSequenceNumber(), table.getPKName(),
                    retainPKColumns ? table.getBucketNum() : null, projectedColumns, null,
                    null, Collections.<PTable>emptyList(), table.isImmutableRows(), Collections.<PName>emptyList(), null, null,
                    table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(),
                    null);
    }

    // For extracting column references from single select statement
    private static class ColumnRefVisitor extends StatelessTraverseAllParseNodeVisitor {
        private final StatementContext context;
        private final Set<ColumnRef> columnRefSet;
        private final Set<LocalIndexDataColumnRef> localIndexColumnRefSet;
        
        private ColumnRefVisitor(StatementContext context) {
            this.context = context;
            this.columnRefSet = new HashSet<ColumnRef>();
            this.localIndexColumnRefSet = new HashSet<LocalIndexDataColumnRef>();
        }

        @Override
        public Void visit(ColumnParseNode node) throws SQLException {
            try {
                columnRefSet.add(context.getResolver().resolveColumn(node.getSchemaName(), node.getTableName(), node.getName()));
            } catch (ColumnNotFoundException e) {
                if (context.getCurrentTable().getTable().getIndexType() == IndexType.LOCAL) {
                    try {
                        localIndexColumnRefSet.add(new LocalIndexDataColumnRef(context, node.getName()));
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
