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

import static org.apache.phoenix.schema.SaltingUtil.SALTING_COLUMN;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.join.ScanProjector;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.BetweenParseNode;
import org.apache.phoenix.parse.BindTableNode;
import org.apache.phoenix.parse.CaseParseNode;
import org.apache.phoenix.parse.CastParseNode;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.ConcreteTableNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.InListParseNode;
import org.apache.phoenix.parse.IsNullParseNode;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.LikeParseNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.NotParseNode;
import org.apache.phoenix.parse.OrParseNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TableNodeVisitor;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.parse.TraverseNoParseNodeVisitor;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;


public class JoinCompiler {
    
    public enum ColumnRefType {
        PREFILTER,
        JOINLOCAL,
        GENERAL,
    }
    
    public static class JoinSpec {
        private ColumnResolver origResolver;
        private TableNode mainTableNode;
        private List<ColumnDef> dynamicColumns;
        private TableRef mainTable;
        private List<AliasedNode> select; // all basic nodes related to mainTable, no aggregation.
        private List<ParseNode> preFilters;
        private List<ParseNode> postFilters;
        private List<JoinTable> joinTables;
        private boolean useStarJoin;
        private Map<TableRef, JoinTable> tableRefToJoinTableMap;
        private Map<ColumnRef, ColumnRefType> columnRefs;
        
        private JoinSpec(SelectStatement statement, ColumnResolver resolver) throws SQLException {
            this.origResolver = resolver;
            List<AliasedNode> selectList = statement.getSelect();
            List<TableNode> tableNodes = statement.getFrom();
            assert (tableNodes.size() > 1);
            Iterator<TableNode> iter = tableNodes.iterator();
            Iterator<TableRef> tableRefIter = resolver.getTables().iterator();
            this.mainTableNode = iter.next();
            DynamicColumnsVisitor v = new DynamicColumnsVisitor();
            this.mainTableNode.accept(v);
            this.dynamicColumns = v.getDynamicColumns();
            this.mainTable = tableRefIter.next();
            this.select = extractFromSelect(selectList, mainTable, resolver);
            this.joinTables = new ArrayList<JoinTable>(tableNodes.size() - 1);
            this.preFilters = new ArrayList<ParseNode>();
            this.postFilters = new ArrayList<ParseNode>();
            this.useStarJoin = !statement.getHint().hasHint(Hint.NO_STAR_JOIN);
            this.tableRefToJoinTableMap = new HashMap<TableRef, JoinTable>();
            ColumnParseNodeVisitor generalRefVisitor = new ColumnParseNodeVisitor(resolver);
            ColumnParseNodeVisitor joinLocalRefVisitor = new ColumnParseNodeVisitor(resolver);
            ColumnParseNodeVisitor prefilterRefVisitor = new ColumnParseNodeVisitor(resolver);            
            int lastRightJoinIndex = -1;
            TableNode tableNode = null;
            int i = 0;
            while (iter.hasNext()) {
                tableNode = iter.next();
                if (!(tableNode instanceof JoinTableNode))
                    throw new SQLFeatureNotSupportedException("Implicit joins not supported.");
                JoinTableNode joinTableNode = (JoinTableNode) tableNode;
                JoinTable joinTable = new JoinTable(joinTableNode, tableRefIter.next(), statement, resolver);
                for (ParseNode condition : joinTable.conditions) {
                    ComparisonParseNode comparisonNode = (ComparisonParseNode) condition;
                    comparisonNode.getLHS().accept(generalRefVisitor);
                    comparisonNode.getRHS().accept(joinLocalRefVisitor);
                }
                if (joinTable.getType() == JoinType.Right) {
                	lastRightJoinIndex = i;
                }
                joinTables.add(joinTable);
                tableRefToJoinTableMap.put(joinTable.getTable(), joinTable);
                i++;
            }
            List<TableRef> prefilterAcceptedTables = new ArrayList<TableRef>();
            for (i = lastRightJoinIndex == -1 ? 0 : lastRightJoinIndex; i < joinTables.size(); i++) {
                JoinTable joinTable = joinTables.get(i);
                if (joinTable.getType() != JoinType.Left) {
                    prefilterAcceptedTables.add(joinTable.getTable());
                }
            }
            if (statement.getWhere() != null) {
            	if (lastRightJoinIndex > -1 && prefilterAcceptedTables.isEmpty()) {
            		// conditions can't be pushed down to the scan filter.
            		postFilters.add(statement.getWhere());
            	} else {
            		statement.getWhere().accept(new WhereNodeVisitor(resolver, lastRightJoinIndex > -1, prefilterAcceptedTables));
            		for (ParseNode prefilter : preFilters) {
            		    prefilter.accept(prefilterRefVisitor);
            		}
            	}
            	for (ParseNode postfilter : postFilters) {
            		postfilter.accept(generalRefVisitor);
            	}
            }
            // Delayed to this point, since pre-filters might have been post-fixed by WhereNodeVisitor.
            for (JoinTable joinTable : joinTables) {
                for (ParseNode prefilter : joinTable.preFilters) {
                    prefilter.accept(prefilterRefVisitor);
                }
            }
            for (AliasedNode node : selectList) {
                node.getNode().accept(generalRefVisitor);
            }
            if (statement.getGroupBy() != null) {
                for (ParseNode node : statement.getGroupBy()) {
                    node.accept(generalRefVisitor);
                }
            }
            if (statement.getHaving() != null) {
                statement.getHaving().accept(generalRefVisitor);
            }
            if (statement.getOrderBy() != null) {
                for (OrderByNode node : statement.getOrderBy()) {
                    node.getNode().accept(generalRefVisitor);
                }
            }
            this.columnRefs = new HashMap<ColumnRef, ColumnRefType>();
            for (ColumnRef ref : generalRefVisitor.getColumnRefMap().keySet()) {
                columnRefs.put(ref, ColumnRefType.GENERAL);
            }
            for (ColumnRef ref : joinLocalRefVisitor.getColumnRefMap().keySet()) {
                if (!columnRefs.containsKey(ref))
                    columnRefs.put(ref, ColumnRefType.JOINLOCAL);
            }
            for (ColumnRef ref : prefilterRefVisitor.getColumnRefMap().keySet()) {
                if (!columnRefs.containsKey(ref))
                    columnRefs.put(ref, ColumnRefType.PREFILTER);
            }
        }
        
        private JoinSpec(ColumnResolver resolver, TableNode tableNode, List<ColumnDef> dynamicColumns, TableRef table, List<AliasedNode> select, List<ParseNode> preFilters, 
                List<ParseNode> postFilters, List<JoinTable> joinTables, boolean useStarJoin, Map<TableRef, JoinTable> tableRefToJoinTableMap, Map<ColumnRef, ColumnRefType> columnRefs) {
            this.origResolver = resolver;
            this.mainTableNode = tableNode;
            this.dynamicColumns = dynamicColumns;
            this.mainTable = table;
            this.select = select;
            this.preFilters = preFilters;
            this.postFilters = postFilters;
            this.joinTables = joinTables;
            this.useStarJoin = useStarJoin;
            this.tableRefToJoinTableMap = tableRefToJoinTableMap;
            this.columnRefs = columnRefs;
        }
        
        public ColumnResolver getOriginalResolver() {
            return origResolver;
        }
        
        public TableNode getMainTableNode() {
            return mainTableNode;
        }
        
        public List<ColumnDef> getDynamicColumns() {
            return dynamicColumns;
        }
        
        public TableRef getMainTable() {
            return mainTable;
        }
        
        public List<AliasedNode> getSelect() {
            return select;
        }
        
        public List<ParseNode> getPreFilters() {
            return preFilters;
        }
        
        public List<ParseNode> getPostFilters() {
            return postFilters;
        }
        
        public List<JoinTable> getJoinTables() {
            return joinTables;
        }
        
        public ParseNode getPreFiltersCombined() {
            if (preFilters == null || preFilters.isEmpty())
                return null;
            
            if (preFilters.size() == 1)
                return preFilters.get(0);
            
            return NODE_FACTORY.and(preFilters);
        }
        
        public Expression compilePostFilterExpression(StatementContext context) throws SQLException {
        	if (postFilters == null || postFilters.isEmpty())
        		return null;
        	
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            List<Expression> expressions = new ArrayList<Expression>(postFilters.size());
            for (ParseNode postFilter : postFilters) {
                expressionCompiler.reset();
                Expression expression = postFilter.accept(expressionCompiler);
                expressions.add(expression);
            }
            
            if (expressions.size() == 1)
            	return expressions.get(0);
            
            return AndExpression.create(expressions);
        }
        
        /**
         * Returns a boolean vector indicating whether the evaluation of join expressions
         * can be evaluated at an early stage if the input JoinSpec can be taken as a
         * star join. Otherwise returns null.  
         * @param join the JoinSpec
         * @return a boolean vector for a star join; or null for non star join.
         */
        public boolean[] getStarJoinVector() {
            assert(!joinTables.isEmpty());
            
            int count = joinTables.size();
            if (!useStarJoin 
                    && count > 1 
                    && joinTables.get(count - 1).getType() != JoinType.Left)
                return null;

            boolean[] vector = new boolean[count];
            for (int i = 0; i < count; i++) {
                JoinTable joinTable = joinTables.get(i);
                if (joinTable.getType() != JoinType.Left 
                        && joinTable.getType() != JoinType.Inner)
                    return null;
                vector[i] = true;
                Iterator<TableRef> iter = joinTable.getLeftTableRefs().iterator();
                while (vector[i] == true && iter.hasNext()) {
                    TableRef tableRef = iter.next();
                    if (!tableRef.equals(mainTable)) {
                        vector[i] = false;
                    }
                }
            }
            
            return vector;
        }
        
        protected boolean isWildCardSelect(TableRef table) {
            List<AliasedNode> selectList = table.equals(mainTable) ? this.select : tableRefToJoinTableMap.get(table).getSelect();
            return (selectList.size() == 1 && selectList.get(0).getNode() instanceof TableWildcardParseNode);
        }

        public void projectColumns(Scan scan, TableRef table) {
            if (isWildCardSelect(table)) {
                scan.getFamilyMap().clear();
                return;
            }
            for (ColumnRef columnRef : columnRefs.keySet()) {
                if (columnRef.getTableRef().equals(table)
                        && !SchemaUtil.isPKColumn(columnRef.getColumn())) {
                    scan.addColumn(columnRef.getColumn().getFamilyName().getBytes(), columnRef.getColumn().getName().getBytes());
                }
            }
        }
        
        public ProjectedPTableWrapper createProjectedTable(TableRef tableRef, boolean retainPKColumns) throws SQLException {
        	List<PColumn> projectedColumns = new ArrayList<PColumn>();
        	List<Expression> sourceExpressions = new ArrayList<Expression>();
        	ListMultimap<String, String> columnNameMap = ArrayListMultimap.<String, String>create();
            PTable table = tableRef.getTable();
            boolean hasSaltingColumn = retainPKColumns && table.getBucketNum() != null;
            if (retainPKColumns) {
            	for (PColumn column : table.getPKColumns()) {
            		addProjectedColumn(projectedColumns, sourceExpressions, columnNameMap,
            				column, tableRef, column.getFamilyName(), hasSaltingColumn);
            	}
            }
            if (isWildCardSelect(tableRef)) {
            	for (PColumn column : table.getColumns()) {
            		if (!retainPKColumns || !SchemaUtil.isPKColumn(column)) {
            			addProjectedColumn(projectedColumns, sourceExpressions, columnNameMap,
            					column, tableRef, PNameFactory.newName(ScanProjector.VALUE_COLUMN_FAMILY), hasSaltingColumn);
            		}
            	}
            } else {
                for (Map.Entry<ColumnRef, ColumnRefType> e : columnRefs.entrySet()) {
                    ColumnRef columnRef = e.getKey();
                    if (e.getValue() != ColumnRefType.PREFILTER 
                            && columnRef.getTableRef().equals(tableRef)
                            && (!retainPKColumns || !SchemaUtil.isPKColumn(columnRef.getColumn()))) {
                    	PColumn column = columnRef.getColumn();
            			addProjectedColumn(projectedColumns, sourceExpressions, columnNameMap,
            					column, tableRef, PNameFactory.newName(ScanProjector.VALUE_COLUMN_FAMILY), hasSaltingColumn);
                    }
                }            	
            }
            
            PTable t = PTableImpl.makePTable(table.getTenantId(), PNameFactory.newName(PROJECTED_TABLE_SCHEMA), table.getName(), PTableType.JOIN,
                        table.getIndexState(), table.getTimeStamp(), table.getSequenceNumber(), table.getPKName(),
                        retainPKColumns ? table.getBucketNum() : null, projectedColumns, table.getParentTableName(),
                        table.getIndexes(), table.isImmutableRows(), Collections.<PName>emptyList(), null, null, table.isWALDisabled(), table.isMultiTenant(), table.getViewType(), table.getViewIndexId());
            return new ProjectedPTableWrapper(t, columnNameMap, sourceExpressions);
        }
        
        private static void addProjectedColumn(List<PColumn> projectedColumns, List<Expression> sourceExpressions,
        		ListMultimap<String, String> columnNameMap, PColumn sourceColumn, TableRef sourceTable, PName familyName, boolean hasSaltingColumn) 
        throws SQLException {
            if (sourceColumn == SALTING_COLUMN)
                return;
            
        	int position = projectedColumns.size() + (hasSaltingColumn ? 1 : 0);
        	PTable table = sourceTable.getTable();
        	String schemaName = table.getSchemaName().getString();
        	String tableName = table.getTableName().getString();
        	String colName = sourceColumn.getName().getString();
            String fullName = getProjectedColumnName(schemaName, tableName, colName);
            String aliasedName = sourceTable.getTableAlias() == null ? fullName : getProjectedColumnName(null, sourceTable.getTableAlias(), colName);
        	
            columnNameMap.put(colName, aliasedName);
        	if (!fullName.equals(aliasedName)) {
        		columnNameMap.put(fullName, aliasedName);
        	}
            
        	PName name = PNameFactory.newName(aliasedName);
    		PColumnImpl column = new PColumnImpl(name, familyName, sourceColumn.getDataType(), 
    				sourceColumn.getMaxLength(), sourceColumn.getScale(), sourceColumn.isNullable(), 
    				position, sourceColumn.getSortOrder(), sourceColumn.getArraySize(), sourceColumn.getViewConstant(), sourceColumn.isViewReferenced());
        	Expression sourceExpression = new ColumnRef(sourceTable, sourceColumn.getPosition()).newColumnExpression();
        	projectedColumns.add(column);
        	sourceExpressions.add(sourceExpression);
        }
        
        public ColumnResolver getColumnResolver(PTableWrapper table) {
            return new JoinedTableColumnResolver(table, origResolver);
        }
        
        public boolean hasPostReference(TableRef table) {
            if (isWildCardSelect(table)) 
                return true;
            
            for (Map.Entry<ColumnRef, ColumnRefType> e : columnRefs.entrySet()) {
                if (e.getValue() == ColumnRefType.GENERAL && e.getKey().getTableRef().equals(table)) {
                    return true;
                }
            }
            
            return false;
        }
        
        private class WhereNodeVisitor  extends TraverseNoParseNodeVisitor<Void> {
            private ColumnResolver resolver;
            private boolean hasRightJoin;
            private List<TableRef> prefilterAcceptedTables;
            
            public WhereNodeVisitor(ColumnResolver resolver, boolean hasRightJoin, List<TableRef> prefilterAcceptedTables) {
                this.resolver = resolver;
                this.hasRightJoin = hasRightJoin;
                this.prefilterAcceptedTables = prefilterAcceptedTables;
            }
            
            private Void leaveBooleanNode(ParseNode node,
                    List<Void> l) throws SQLException {
                ColumnParseNodeVisitor visitor = new ColumnParseNodeVisitor(resolver);
                node.accept(visitor);
                ColumnParseNodeVisitor.ContentType type = visitor.getContentType(mainTable);
                switch (type) {
                case NONE:
                case SELF_ONLY:
                    if (!hasRightJoin) {
                        preFilters.add(node);
                    } else {
                        postFilters.add(node);
                    }
                    break;
                case FOREIGN_ONLY:
                    TableRef matched = null;
                    for (TableRef table : prefilterAcceptedTables) {
                        if (visitor.getContentType(table) == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                            matched = table;
                            break;
                        }
                    }
                    if (matched != null) {
                        tableRefToJoinTableMap.get(matched).preFilters.add(node);
                    } else {
                        postFilters.add(node);
                    }
                    break;
                default:
                    postFilters.add(node);
                    break;
                }
                return null;
            }

            @Override
            public Void visitLeave(LikeParseNode node,
                    List<Void> l) throws SQLException {                
                return leaveBooleanNode(node, l);
            }

            @Override
            public boolean visitEnter(AndParseNode node) {
                return true;
            }
            
            @Override
            public Void visitLeave(OrParseNode node, List<Void> l)
                    throws SQLException {
                return leaveBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(ComparisonParseNode node, List<Void> l) 
                    throws SQLException {
                return leaveBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(NotParseNode node, List<Void> l)
                    throws SQLException {
                return leaveBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(InListParseNode node,
                    List<Void> l) throws SQLException {
                return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(IsNullParseNode node, List<Void> l) 
                    throws SQLException {
                return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(FunctionParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(BetweenParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(CaseParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(CastParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveBooleanNode(node, l);
            }			
        }
    }
    
    public static JoinSpec getSubJoinSpecWithoutPostFilters(JoinSpec join) {
        return new JoinSpec(join.origResolver, join.mainTableNode, join.dynamicColumns, join.mainTable, join.select, join.preFilters, new ArrayList<ParseNode>(), 
                join.joinTables.subList(0, join.joinTables.size() - 1), join.useStarJoin, join.tableRefToJoinTableMap, join.columnRefs);
    }
    
    public static class JoinTable {
        private JoinType type;
        private TableNode tableNode; // original table node
        private List<ColumnDef> dynamicColumns;
        private TableRef table;
        private List<AliasedNode> select; // all basic nodes related to this table, no aggregation.
        private HintNode hint;
        private List<ParseNode> preFilters;
        private List<ParseNode> conditions;
        private SelectStatement subquery;
        
        private Set<TableRef> leftTableRefs;
        
        public JoinTable(JoinTableNode node, TableRef tableRef, SelectStatement statement, ColumnResolver resolver) throws SQLException {
            if (!(node.getTable() instanceof ConcreteTableNode))
                throw new SQLFeatureNotSupportedException("Subqueries not supported.");
            
            this.type = node.getType();
            this.tableNode = node.getTable();
            DynamicColumnsVisitor v = new DynamicColumnsVisitor();
            this.tableNode.accept(v);
            this.dynamicColumns = v.getDynamicColumns();
            this.table = tableRef;
            this.select = extractFromSelect(statement.getSelect(),tableRef,resolver);
            this.hint = statement.getHint();
            this.preFilters = new ArrayList<ParseNode>();
            this.conditions = new ArrayList<ParseNode>();
            this.leftTableRefs = new HashSet<TableRef>();
            node.getOnNode().accept(new OnNodeVisitor(resolver));
        }
        
        public JoinType getType() {
            return type;
        }
        
        public TableNode getTableNode() {
            return tableNode;
        }
        
        public List<ColumnDef> getDynamicColumns() {
            return dynamicColumns;
        }
        
        public TableRef getTable() {
            return table;
        }
        
        public List<AliasedNode> getSelect() {
            return select;
        }
        
        public List<ParseNode> getPreFilters() {
            return preFilters;
        }
        
        public List<ParseNode> getJoinConditions() {
            return conditions;
        }
        
        public SelectStatement getSubquery() {
            return subquery;
        }
        
        public Set<TableRef> getLeftTableRefs() {
            return leftTableRefs;
        }
        
        public ParseNode getPreFiltersCombined() {
            if (preFilters == null || preFilters.isEmpty())
                return null;
            
            if (preFilters.size() == 1)
                return preFilters.get(0);
            
            return NODE_FACTORY.and(preFilters);
        }
        
        public SelectStatement getAsSubquery() {
            if (subquery != null)
                return subquery;
            
            List<TableNode> from = new ArrayList<TableNode>(1);
            from.add(tableNode);
            return NODE_FACTORY.select(from, hint, false, select, getPreFiltersCombined(), null, null, null, null, 0, false);
        }
        
        public Pair<List<Expression>, List<Expression>> compileJoinConditions(StatementContext context, ColumnResolver leftResolver, ColumnResolver rightResolver) throws SQLException {
        	ColumnResolver resolver = context.getResolver();
            List<Pair<Expression, Expression>> compiled = new ArrayList<Pair<Expression, Expression>>(conditions.size());
        	context.setResolver(leftResolver);
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            for (ParseNode condition : conditions) {
                assert (condition instanceof EqualParseNode);
                EqualParseNode equalNode = (EqualParseNode) condition;
                expressionCompiler.reset();
                Expression left = equalNode.getLHS().accept(expressionCompiler);
                compiled.add(new Pair<Expression, Expression>(left, null));
            }
        	context.setResolver(rightResolver);
            expressionCompiler = new ExpressionCompiler(context);
            Iterator<Pair<Expression, Expression>> iter = compiled.iterator();
            for (ParseNode condition : conditions) {
                Pair<Expression, Expression> p = iter.next();
                EqualParseNode equalNode = (EqualParseNode) condition;
                expressionCompiler.reset();
                Expression right = equalNode.getRHS().accept(expressionCompiler);
                Expression left = p.getFirst();
                PDataType toType = getCommonType(left.getDataType(), right.getDataType());
                if (left.getDataType() != toType) {
                    left = CoerceExpression.create(left, toType);
                    p.setFirst(left);
                }
                if (right.getDataType() != toType) {
                    right = CoerceExpression.create(right, toType);
                }
                p.setSecond(right);
            }
            context.setResolver(resolver); // recover the resolver
            Collections.sort(compiled, new Comparator<Pair<Expression, Expression>>() {
                @Override
                public int compare(Pair<Expression, Expression> o1, Pair<Expression, Expression> o2) {
                    Expression e1 = o1.getFirst();
                    Expression e2 = o2.getFirst();
                    boolean isFixed1 = e1.getDataType().isFixedWidth();
                    boolean isFixed2 = e2.getDataType().isFixedWidth();
                    boolean isFixedNullable1 = e1.isNullable() &&isFixed1;
                    boolean isFixedNullable2 = e2.isNullable() && isFixed2;
                    if (isFixedNullable1 == isFixedNullable2) {
                        if (isFixed1 == isFixed2) {
                            return 0;
                        } else if (isFixed1) {
                            return -1;
                        } else {
                            return 1;
                        }
                    } else if (isFixedNullable1) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            List<Expression> lConditions = new ArrayList<Expression>(compiled.size());
            List<Expression> rConditions = new ArrayList<Expression>(compiled.size());
            for (Pair<Expression, Expression> pair : compiled) {
                lConditions.add(pair.getFirst());
                rConditions.add(pair.getSecond());
            }
            
            return new Pair<List<Expression>, List<Expression>>(lConditions, rConditions);
        }
        
        private PDataType getCommonType(PDataType lType, PDataType rType) throws SQLException {
            if (lType == rType)
                return lType;
            
            if (!lType.isComparableTo(rType))
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CONVERT_TYPE)
                    .setMessage("On-clause LHS expression and RHS expression must be comparable. LHS type: " + lType + ", RHS type: " + rType)
                    .build().buildException();

            if ((lType == null || lType.isCoercibleTo(PDataType.TINYINT))
                    && (rType == null || rType.isCoercibleTo(PDataType.TINYINT))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.SMALLINT))
                    && (rType == null || rType.isCoercibleTo(PDataType.SMALLINT))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.INTEGER))
                    && (rType == null || rType.isCoercibleTo(PDataType.INTEGER))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.LONG))
                    && (rType == null || rType.isCoercibleTo(PDataType.LONG))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.DOUBLE))
                    && (rType == null || rType.isCoercibleTo(PDataType.DOUBLE))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.DECIMAL))
                    && (rType == null || rType.isCoercibleTo(PDataType.DECIMAL))) {
                return PDataType.DECIMAL;
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.DATE))
                    && (rType == null || rType.isCoercibleTo(PDataType.DATE))) {
                return lType == null ? rType : lType;
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.TIMESTAMP))
                    && (rType == null || rType.isCoercibleTo(PDataType.TIMESTAMP))) {
                return lType == null ? rType : lType;
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.VARCHAR))
                    && (rType == null || rType.isCoercibleTo(PDataType.VARCHAR))) {
                return PDataType.VARCHAR;
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.BOOLEAN))
                    && (rType == null || rType.isCoercibleTo(PDataType.BOOLEAN))) {
                return PDataType.BOOLEAN;
            }

            return PDataType.VARBINARY;
        }
        
        private class OnNodeVisitor  extends TraverseNoParseNodeVisitor<Void> {
            private ColumnResolver resolver;
            
            public OnNodeVisitor(ColumnResolver resolver) {
                this.resolver = resolver;
            }
            
            private Void leaveNonEqBooleanNode(ParseNode node,
                    List<Void> l) throws SQLException {
                ColumnParseNodeVisitor visitor = new ColumnParseNodeVisitor(resolver);
                node.accept(visitor);
                ColumnParseNodeVisitor.ContentType type = visitor.getContentType(table);
                if (type == ColumnParseNodeVisitor.ContentType.NONE 
                        || type == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                    preFilters.add(node);
                } else {
                    throwUnsupportedJoinConditionException();
                }
                return null;
            }

            @Override
            public Void visitLeave(LikeParseNode node,
                    List<Void> l) throws SQLException {                
                return leaveNonEqBooleanNode(node, l);
            }

            @Override
            public boolean visitEnter(AndParseNode node) {
                return true;
            }
            
            @Override
            public Void visitLeave(OrParseNode node, List<Void> l)
                    throws SQLException {
                return leaveNonEqBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(ComparisonParseNode node, List<Void> l) 
                    throws SQLException {
                if (!(node instanceof EqualParseNode))
                    return leaveNonEqBooleanNode(node, l);
                ColumnParseNodeVisitor lhsVisitor = new ColumnParseNodeVisitor(resolver);
                ColumnParseNodeVisitor rhsVisitor = new ColumnParseNodeVisitor(resolver);
                node.getLHS().accept(lhsVisitor);
                node.getRHS().accept(rhsVisitor);
                ColumnParseNodeVisitor.ContentType lhsType = lhsVisitor.getContentType(table);
                ColumnParseNodeVisitor.ContentType rhsType = rhsVisitor.getContentType(table);
                if ((lhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY || lhsType == ColumnParseNodeVisitor.ContentType.NONE)
                		&& (rhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY || rhsType == ColumnParseNodeVisitor.ContentType.NONE)) {
                    preFilters.add(node);
                } else if (lhsType == ColumnParseNodeVisitor.ContentType.FOREIGN_ONLY 
                		&& rhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                    conditions.add(node);
                    leftTableRefs.addAll(lhsVisitor.getTableRefSet());
                } else if (rhsType == ColumnParseNodeVisitor.ContentType.FOREIGN_ONLY 
                		&& lhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                    conditions.add(NODE_FACTORY.equal(node.getRHS(), node.getLHS()));
                    leftTableRefs.addAll(rhsVisitor.getTableRefSet());
                } else {
                	throwUnsupportedJoinConditionException();
                }
                return null;
            }

            @Override
            public Void visitLeave(NotParseNode node, List<Void> l)
                    throws SQLException {
                return leaveNonEqBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(InListParseNode node,
                    List<Void> l) throws SQLException {
                return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(IsNullParseNode node, List<Void> l) 
                    throws SQLException {
                return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(FunctionParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(BetweenParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(CaseParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(CastParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveNonEqBooleanNode(node, l);
            }

            /*
             * Conditions in the ON clause can only be:
             * 1) an equal test between a self table expression and a foreign 
             *    table expression.
             * 2) a boolean condition referencing to the self table only.
             * Otherwise, it can be ambiguous.
             */
            private void throwUnsupportedJoinConditionException() 
            		throws SQLFeatureNotSupportedException {
            	throw new SQLFeatureNotSupportedException("Does not support non-standard or non-equi join conditions.");
            }			
        }
    }
    
    private static class ColumnParseNodeVisitor  extends StatelessTraverseAllParseNodeVisitor {
        public enum ContentType {NONE, SELF_ONLY, FOREIGN_ONLY, COMPLEX};
        
        private ColumnResolver resolver;
        private final Set<TableRef> tableRefSet;
        private final Map<ColumnRef, ColumnParseNode> columnRefMap;
       
        public ColumnParseNodeVisitor(ColumnResolver resolver) {
            this.resolver = resolver;
            this.tableRefSet = new HashSet<TableRef>();
            this.columnRefMap = new HashMap<ColumnRef, ColumnParseNode>();
        }
        
        public void reset() {
            this.tableRefSet.clear();
            this.columnRefMap.clear();
        }
        
        @Override
        public Void visit(ColumnParseNode node) throws SQLException {
            ColumnRef columnRef = resolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
            columnRefMap.put(columnRef, node);
            tableRefSet.add(columnRef.getTableRef());
            return null;                
        }
        
        public Set<TableRef> getTableRefSet() {
            return tableRefSet;
        }
        
        public Map<ColumnRef, ColumnParseNode> getColumnRefMap() {
            return columnRefMap;
        }
        
        public ContentType getContentType(TableRef selfTable) {
            if (tableRefSet.isEmpty())
                return ContentType.NONE;
            if (tableRefSet.size() > 1)
                return ContentType.COMPLEX;
            if (tableRefSet.contains(selfTable))
                return ContentType.SELF_ONLY;
            return ContentType.FOREIGN_ONLY;
        }
    }
    
    private static String PROJECTED_TABLE_SCHEMA = ".";
    // for creation of new statements
    private static ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    private static List<AliasedNode> extractFromSelect(List<AliasedNode> select, TableRef table, ColumnResolver resolver) throws SQLException {
        List<AliasedNode> ret = new ArrayList<AliasedNode>();
        ColumnParseNodeVisitor visitor = new ColumnParseNodeVisitor(resolver);
        for (AliasedNode aliasedNode : select) {
            ParseNode node = aliasedNode.getNode();
            if (node instanceof TableWildcardParseNode) {
                TableName tableName = ((TableWildcardParseNode) node).getTableName();
                if (table.equals(resolver.resolveTable(tableName.getSchemaName(), tableName.getTableName()))) {
                    ret.clear();
                    ret.add(aliasedNode);
                    return ret;
                }
                continue;
            }
            
            node.accept(visitor);
            ColumnParseNodeVisitor.ContentType type = visitor.getContentType(table);
            if (type == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                ret.add(aliasedNode);
            } else if (type == ColumnParseNodeVisitor.ContentType.COMPLEX) {
                for (Map.Entry<ColumnRef, ColumnParseNode> entry : visitor.getColumnRefMap().entrySet()) {
                    if (entry.getKey().getTableRef().equals(table)) {
                        ret.add(NODE_FACTORY.aliasedNode(null, entry.getValue()));
                    }
                }
            }
            visitor.reset();
        }
        return ret;
    }
    
    public static JoinSpec getJoinSpec(StatementContext context, SelectStatement statement) throws SQLException {
        return new JoinSpec(statement, context.getResolver());
    }
    
    public static SelectStatement optimize(StatementContext context, SelectStatement select, PhoenixStatement statement) throws SQLException {
        ColumnResolver resolver = context.getResolver();
        TableRef groupByTableRef = null;
        TableRef orderByTableRef = null;
        if (select.getGroupBy() != null && !select.getGroupBy().isEmpty()) {
            ColumnParseNodeVisitor groupByVisitor = new ColumnParseNodeVisitor(resolver);
            for (ParseNode node : select.getGroupBy()) {
                node.accept(groupByVisitor);
            }
            Set<TableRef> set = groupByVisitor.getTableRefSet();
            if (set.size() == 1) {
                groupByTableRef = set.iterator().next();
            }
        } else if (select.getOrderBy() != null && !select.getOrderBy().isEmpty()) {
            ColumnParseNodeVisitor orderByVisitor = new ColumnParseNodeVisitor(resolver);
            for (OrderByNode node : select.getOrderBy()) {
                node.getNode().accept(orderByVisitor);
            }
            Set<TableRef> set = orderByVisitor.getTableRefSet();
            if (set.size() == 1) {
                orderByTableRef = set.iterator().next();
            }
        }
        JoinSpec join = getJoinSpec(context, select);
        if (groupByTableRef != null || orderByTableRef != null) {
            QueryCompiler compiler = new QueryCompiler(statement, select, resolver);
            List<Object> binds = statement.getParameters();
            StatementContext ctx = new StatementContext(statement, resolver, new Scan());
            QueryPlan plan = compiler.compileJoinQuery(ctx, select, binds, join, false);
            TableRef table = plan.getTableRef();
            if (groupByTableRef != null && !groupByTableRef.equals(table)) {
                groupByTableRef = null;
            }
            if (orderByTableRef != null && !orderByTableRef.equals(table)) {
                orderByTableRef = null;
            }            
        }
        
        Map<TableRef, TableRef> replacement = new HashMap<TableRef, TableRef>();
        List<TableNode> from = select.getFrom();
        List<TableNode> newFrom = Lists.newArrayListWithExpectedSize(from.size());

        class TableNodeRewriter implements TableNodeVisitor {
            private TableRef table;
            private TableNode replaced;
            
            TableNodeRewriter(TableRef table) {
                this.table = table;
            }
            
            public TableNode getReplacedTableNode() {
                return replaced;
            }

            @Override
            public void visit(BindTableNode boundTableNode) throws SQLException {
                String alias = boundTableNode.getAlias();
                replaced = NODE_FACTORY.bindTable(alias == null ? null : '"' + alias + '"', getReplacedTableName());
            }

            @Override
            public void visit(JoinTableNode joinNode) throws SQLException {
                joinNode.getTable().accept(this);
                replaced = NODE_FACTORY.join(joinNode.getType(), joinNode.getOnNode(), replaced);
            }

            @Override
            public void visit(NamedTableNode namedTableNode)
                    throws SQLException {
                String alias = namedTableNode.getAlias();
                replaced = NODE_FACTORY.namedTable(alias == null ? null : '"' + alias + '"', getReplacedTableName(), namedTableNode.getDynamicColumns());
            }

            @Override
            public void visit(DerivedTableNode subselectNode)
                    throws SQLException {
                throw new SQLFeatureNotSupportedException();
            }
            
            private TableName getReplacedTableName() {
                String schemaName = table.getTable().getSchemaName().getString();
                return TableName.create(schemaName.length() == 0 ? null : schemaName, table.getTable().getTableName().getString());
            }
        };
        
        // get optimized plans for join tables
        for (int i = 1; i < from.size(); i++) {
            TableNode jNode = from.get(i);
            assert (jNode instanceof JoinTableNode);
            TableNode tNode = ((JoinTableNode) jNode).getTable();
            for (JoinTable jTable : join.getJoinTables()) {
                if (jTable.getTableNode() != tNode)
                    continue;
                TableRef table = jTable.getTable();
                List<ParseNode> groupBy = table.equals(groupByTableRef) ? select.getGroupBy() : null;
                List<OrderByNode> orderBy = table.equals(orderByTableRef) ? select.getOrderBy() : null;
                SelectStatement stmt = getSubqueryForOptimizedPlan(select.getHint(), join.tableRefToJoinTableMap.get(table).getDynamicColumns(), table, join.columnRefs, jTable.getPreFiltersCombined(), groupBy, orderBy, join.isWildCardSelect(table));
                QueryPlan plan = context.getConnection().getQueryServices().getOptimizer().optimize(statement, stmt);
                if (!plan.getTableRef().equals(table)) {
                    TableNodeRewriter rewriter = new TableNodeRewriter(plan.getTableRef());
                    jNode.accept(rewriter);
                    newFrom.add(rewriter.getReplacedTableNode());
                    replacement.put(table, plan.getTableRef());
                } else {
                    newFrom.add(jNode);
                }
            }
        }
        // get optimized plan for main table
        TableRef table = join.getMainTable();
        List<ParseNode> groupBy = table.equals(groupByTableRef) ? select.getGroupBy() : null;
        List<OrderByNode> orderBy = table.equals(orderByTableRef) ? select.getOrderBy() : null;
        SelectStatement stmt = getSubqueryForOptimizedPlan(select.getHint(), join.dynamicColumns, table, join.columnRefs, join.getPreFiltersCombined(), groupBy, orderBy, join.isWildCardSelect(table));
        QueryPlan plan = context.getConnection().getQueryServices().getOptimizer().optimize(statement, stmt);
        if (!plan.getTableRef().equals(table)) {
            TableNodeRewriter rewriter = new TableNodeRewriter(plan.getTableRef());
            from.get(0).accept(rewriter);
            newFrom.add(0, rewriter.getReplacedTableNode());
            replacement.put(table, plan.getTableRef());            
        } else {
            newFrom.add(0, from.get(0));
        }
        
        if (replacement.isEmpty()) 
            return select;
        
        return IndexStatementRewriter.translate(NODE_FACTORY.select(select, newFrom), resolver, replacement);        
    }
    
    private static SelectStatement getSubqueryForOptimizedPlan(HintNode hintNode, List<ColumnDef> dynamicCols, TableRef tableRef, Map<ColumnRef, ColumnRefType> columnRefs, ParseNode where, List<ParseNode> groupBy, List<OrderByNode> orderBy, boolean isWildCardSelect) {
        String schemaName = tableRef.getTable().getSchemaName().getString();
        TableName tName = TableName.create(schemaName.length() == 0 ? null : schemaName, tableRef.getTable().getTableName().getString());
        List<AliasedNode> selectList = new ArrayList<AliasedNode>();
        if (isWildCardSelect) {
            selectList.add(NODE_FACTORY.aliasedNode(null, WildcardParseNode.INSTANCE));
        } else {
            for (ColumnRef colRef : columnRefs.keySet()) {
                if (colRef.getTableRef().equals(tableRef)) {
                    ParseNode node = NODE_FACTORY.column(tName, '"' + colRef.getColumn().getName().getString() + '"', null);
                    if (groupBy != null) {
                        node = NODE_FACTORY.function(CountAggregateFunction.NAME, Collections.singletonList(node));
                    }
                    selectList.add(NODE_FACTORY.aliasedNode(null, node));
                }
            }
        }
        String tableAlias = tableRef.getTableAlias();
        List<? extends TableNode> from = Collections.singletonList(NODE_FACTORY.namedTable(tableAlias == null ? null : '"' + tableAlias + '"', tName, dynamicCols));

        return NODE_FACTORY.select(from, hintNode, false, selectList, where, groupBy, null, orderBy, null, 0, false);
    }
    
    public static SelectStatement getSubqueryWithoutJoin(SelectStatement statement, JoinSpec join) {
        return NODE_FACTORY.select(statement.getFrom().subList(0, 1), statement.getHint(), statement.isDistinct(), statement.getSelect(), join.getPreFiltersCombined(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    // Get the last join table select statement with fixed-up select and where nodes.
    // Currently does NOT support last join table as a subquery.
    public static SelectStatement getSubqueryForLastJoinTable(SelectStatement statement, JoinSpec join) throws SQLException {
        List<JoinTable> joinTables = join.getJoinTables();
        int count = joinTables.size();
        assert (count > 0);
        JoinTable lastJoinTable = joinTables.get(count - 1);
        if (lastJoinTable.getSubquery() != null) {
            throw new SQLFeatureNotSupportedException("Subqueries not supported.");
        }
        List<TableNode> from = new ArrayList<TableNode>(1);
        from.add(lastJoinTable.getTableNode());
        
        return NODE_FACTORY.select(from, statement.getHint(), statement.isDistinct(), statement.getSelect(), lastJoinTable.getPreFiltersCombined(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    // Get subquery with fixed select and where nodes
    public static SelectStatement getSubQueryWithoutLastJoin(SelectStatement statement, JoinSpec join) {
        List<TableNode> from = statement.getFrom();
        assert(from.size() > 1);
        List<JoinTable> joinTables = join.getJoinTables();
        int count = joinTables.size();
        assert (count > 0);
        List<AliasedNode> select = new ArrayList<AliasedNode>();
        select.addAll(join.getSelect());
        for (int i = 0; i < count - 1; i++) {
            select.addAll(joinTables.get(i).getSelect());
        }
        
        return NODE_FACTORY.select(from.subList(0, from.size() - 1), statement.getHint(), false, select, join.getPreFiltersCombined(), null, null, null, null, statement.getBindCount(), false);
    }
    
    public static PTableWrapper mergeProjectedTables(PTableWrapper lWrapper, PTableWrapper rWrapper, boolean innerJoin) throws SQLException {
    	PTable left = lWrapper.getTable();
    	PTable right = rWrapper.getTable();
    	List<PColumn> merged = new ArrayList<PColumn>();
    	merged.addAll(left.getColumns());
    	int position = merged.size();
    	for (PColumn c : right.getColumns()) {
    		if (!SchemaUtil.isPKColumn(c)) {
    			PColumnImpl column = new PColumnImpl(c.getName(), 
    					PNameFactory.newName(ScanProjector.VALUE_COLUMN_FAMILY), c.getDataType(), 
    					c.getMaxLength(), c.getScale(), innerJoin ? c.isNullable() : true, position++, 
    					c.getSortOrder(), c.getArraySize(), c.getViewConstant(), c.isViewReferenced());
    			merged.add(column);
    		}
    	}
        if (left.getBucketNum() != null) {
            merged.remove(0);
        }
        PTable t = PTableImpl.makePTable(left.getTenantId(), left.getSchemaName(),
                PNameFactory.newName(SchemaUtil.getTableName(left.getName().getString(), right.getName().getString())), left.getType(), left.getIndexState(), left.getTimeStamp(), left.getSequenceNumber(), left.getPKName(), left.getBucketNum(), merged,
                left.getParentTableName(), left.getIndexes(), left.isImmutableRows(), Collections.<PName>emptyList(), null, null, PTable.DEFAULT_DISABLE_WAL, left.isMultiTenant(), left.getViewType(), left.getViewIndexId());

        ListMultimap<String, String> mergedMap = ArrayListMultimap.<String, String>create();
        mergedMap.putAll(lWrapper.getColumnNameMap());
        mergedMap.putAll(rWrapper.getColumnNameMap());
        
        return new PTableWrapper(t, mergedMap);
    }
    
    public static ScanProjector getScanProjector(ProjectedPTableWrapper table) {
    	return new ScanProjector(table);
    }
    
    public static class PTableWrapper {
    	protected PTable table;
    	protected ListMultimap<String, String> columnNameMap;
    	
    	protected PTableWrapper(PTable table, ListMultimap<String, String> columnNameMap) {
    		this.table = table;
    		this.columnNameMap = columnNameMap;
    	}
    	
    	public PTable getTable() {
    		return table;
    	}
    	
    	public ListMultimap<String, String> getColumnNameMap() {
    		return columnNameMap;
    	}

    	public List<String> getMappedColumnName(String name) {
    		return columnNameMap.get(name);
    	}
    }
    
    public static class ProjectedPTableWrapper extends PTableWrapper {
    	private List<Expression> sourceExpressions;
    	
    	protected ProjectedPTableWrapper(PTable table, ListMultimap<String, String> columnNameMap, List<Expression> sourceExpressions) {
    		super(table, columnNameMap);
    		this.sourceExpressions = sourceExpressions;
    	}
    	
    	public Expression getSourceExpression(PColumn column) {
    		return sourceExpressions.get(column.getPosition() - (table.getBucketNum() == null ? 0 : 1));
    	}
    }
    
    public static class JoinedTableColumnResolver implements ColumnResolver {
    	private PTableWrapper table;
    	private ColumnResolver tableResolver;
    	private TableRef tableRef;
    	
    	private JoinedTableColumnResolver(PTableWrapper table, ColumnResolver tableResolver) {
    		this.table = table;
    		this.tableResolver = tableResolver;
            this.tableRef = new TableRef(null, table.getTable(), 0, false);
    	}
        
        public PTableWrapper getPTableWrapper() {
            return table;
        }

		@Override
		public List<TableRef> getTables() {
			return tableResolver.getTables();
		}

        @Override
        public TableRef resolveTable(String schemaName, String tableName)
                throws SQLException {
            return tableResolver.resolveTable(schemaName, tableName);
        }

		@Override
		public ColumnRef resolveColumn(String schemaName, String tableName,
				String colName) throws SQLException {
			String name = getProjectedColumnName(schemaName, tableName, colName);
			try {
				PColumn column = tableRef.getTable().getColumn(name);
				return new ColumnRef(tableRef, column.getPosition());
			} catch (ColumnNotFoundException e) {
				List<String> names = table.getMappedColumnName(name);
				if (names.size() == 1) {
					PColumn column = tableRef.getTable().getColumn(names.get(0));
					return new ColumnRef(tableRef, column.getPosition());					
				}
				
				if (names.size() > 1) {
					throw new AmbiguousColumnException(name);
				}
				
				throw e;
			}
		}
    }
    
    private static String getProjectedColumnName(String schemaName, String tableName,
			String colName) {
    	return SchemaUtil.getColumnName(SchemaUtil.getTableName(schemaName, tableName), colName);
    }

    private static class DynamicColumnsVisitor implements TableNodeVisitor {
        private List<ColumnDef> dynamicCols;
        
        public DynamicColumnsVisitor() {
        }
        
        public List<ColumnDef> getDynamicColumns() {
            return dynamicCols == null ? Collections.<ColumnDef> emptyList() : dynamicCols;
        }

        @Override
        public void visit(BindTableNode boundTableNode) throws SQLException {
        }

        @Override
        public void visit(JoinTableNode joinNode) throws SQLException {
            // should not expect to see this node.
            assert(false);
        }

        @Override
        public void visit(NamedTableNode namedTableNode)
                throws SQLException {
            this.dynamicCols = namedTableNode.getDynamicColumns();
        }

        @Override
        public void visit(DerivedTableNode subselectNode)
                throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }
    };

}


