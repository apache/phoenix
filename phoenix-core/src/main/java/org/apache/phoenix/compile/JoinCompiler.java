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
import org.apache.phoenix.join.TupleProjector;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.BindTableNode;
import org.apache.phoenix.parse.BooleanParseNodeVisitor;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TableNodeVisitor;
import org.apache.phoenix.parse.TableWildcardParseNode;
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
import com.google.common.collect.Sets;


public class JoinCompiler {
    
    public enum ColumnRefType {
        PREFILTER,
        JOINLOCAL,
        GENERAL,
    }
    
    private final PhoenixStatement statement;
    private final SelectStatement select;
    private final ColumnResolver origResolver;
    private final boolean useStarJoin;
    private final Map<ColumnRef, ColumnRefType> columnRefs;
    
    
    private JoinCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver) {
        this.statement = statement;
        this.select = select;
        this.origResolver = resolver;
        this.useStarJoin = !select.getHint().hasHint(Hint.NO_STAR_JOIN);
        this.columnRefs = new HashMap<ColumnRef, ColumnRefType>();
    }
    
    public static JoinTable compile(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver) throws SQLException {
        JoinCompiler compiler = new JoinCompiler(statement, select, resolver);
        
        List<TableNode> from = select.getFrom();
        if (from.size() > 1) {
            throw new SQLFeatureNotSupportedException("Cross join not supported.");
        }
        
        JoinTableConstructor constructor = compiler.new JoinTableConstructor();
        Pair<Table, List<JoinSpec>> res = from.get(0).accept(constructor);
        JoinTable joinTable = res.getSecond() == null ? compiler.new JoinTable(res.getFirst()) : compiler.new JoinTable(res.getFirst(), res.getSecond());
        if (select.getWhere() != null) {
            joinTable.addFilter(select.getWhere());
        }
        
        ColumnRefParseNodeVisitor generalRefVisitor = new ColumnRefParseNodeVisitor(resolver);
        ColumnRefParseNodeVisitor joinLocalRefVisitor = new ColumnRefParseNodeVisitor(resolver);
        ColumnRefParseNodeVisitor prefilterRefVisitor = new ColumnRefParseNodeVisitor(resolver);
        
        joinTable.pushDownColumnRefVisitors(generalRefVisitor, joinLocalRefVisitor, prefilterRefVisitor);

        for (AliasedNode node : select.getSelect()) {
            node.getNode().accept(generalRefVisitor);
        }
        if (select.getGroupBy() != null) {
            for (ParseNode node : select.getGroupBy()) {
                node.accept(generalRefVisitor);
            }
        }
        if (select.getHaving() != null) {
            select.getHaving().accept(generalRefVisitor);
        }
        if (select.getOrderBy() != null) {
            for (OrderByNode node : select.getOrderBy()) {
                node.getNode().accept(generalRefVisitor);
            }
        }
        
        for (ColumnRef ref : generalRefVisitor.getColumnRefMap().keySet()) {
            compiler.columnRefs.put(ref, ColumnRefType.GENERAL);
        }
        for (ColumnRef ref : joinLocalRefVisitor.getColumnRefMap().keySet()) {
            if (!compiler.columnRefs.containsKey(ref))
                compiler.columnRefs.put(ref, ColumnRefType.JOINLOCAL);
        }
        for (ColumnRef ref : prefilterRefVisitor.getColumnRefMap().keySet()) {
            if (!compiler.columnRefs.containsKey(ref))
                compiler.columnRefs.put(ref, ColumnRefType.PREFILTER);
        }
        
        return joinTable;
    }

    private class JoinTableConstructor implements TableNodeVisitor<Pair<Table, List<JoinSpec>>> {
        
        private TableRef resolveTable(String alias, TableName name) throws SQLException {
            if (alias != null)
                return origResolver.resolveTable(null, alias);
            
            return origResolver.resolveTable(name.getSchemaName(), name.getTableName());
        }

        @Override
        public Pair<Table, List<JoinSpec>> visit(BindTableNode boundTableNode) throws SQLException {
            TableRef tableRef = resolveTable(boundTableNode.getAlias(), boundTableNode.getName());
            List<AliasedNode> selectNodes = extractFromSelect(select.getSelect(), tableRef, origResolver);
            Table table = new Table(boundTableNode, Collections.<ColumnDef>emptyList(), selectNodes, tableRef);
            return new Pair<Table, List<JoinSpec>>(table, null);
        }

        @Override
        public Pair<Table, List<JoinSpec>> visit(JoinTableNode joinNode) throws SQLException {
            Pair<Table, List<JoinSpec>> lhs = joinNode.getLHS().accept(this);
            Pair<Table, List<JoinSpec>> rhs = joinNode.getRHS().accept(this);
            JoinTable joinTable = rhs.getSecond() == null ? new JoinTable(rhs.getFirst()) : new JoinTable(rhs.getFirst(), rhs.getSecond());
            List<JoinSpec> joinSpecs = lhs.getSecond();
            if (joinSpecs == null) {
                joinSpecs = new ArrayList<JoinSpec>();
            }
            joinSpecs.add(new JoinSpec(joinNode.getType(), joinNode.getOnNode(), joinTable, joinNode.isSingleValueOnly(), origResolver));
            
            return new Pair<Table, List<JoinSpec>>(lhs.getFirst(), joinSpecs);
        }

        @Override
        public Pair<Table, List<JoinSpec>> visit(NamedTableNode namedTableNode)
                throws SQLException {
            TableRef tableRef = resolveTable(namedTableNode.getAlias(), namedTableNode.getName());
            List<AliasedNode> selectNodes = extractFromSelect(select.getSelect(), tableRef, origResolver);
            Table table = new Table(namedTableNode, namedTableNode.getDynamicColumns(), selectNodes, tableRef);
            return new Pair<Table, List<JoinSpec>>(table, null);
        }

        @Override
        public Pair<Table, List<JoinSpec>> visit(DerivedTableNode subselectNode)
                throws SQLException {
            TableRef tableRef = resolveTable(subselectNode.getAlias(), null);
            List<AliasedNode> selectNodes = extractFromSelect(select.getSelect(), tableRef, origResolver);
            Table table = new Table(subselectNode, selectNodes, tableRef);
            return new Pair<Table, List<JoinSpec>>(table, null);
        }
    }
    
    public class JoinTable {
        private final Table table;
        private final List<JoinSpec> joinSpecs;
        private final List<ParseNode> postFilters;
        private final List<Table> tables;
        private final List<TableRef> tableRefs;
        private final boolean allLeftJoin;
        private final boolean hasRightJoin;
        private final List<JoinTable> prefilterAcceptedTables;
        
        private JoinTable(Table table) {
            this.table = table;
            this.joinSpecs = Collections.<JoinSpec>emptyList();
            this.postFilters = Collections.<ParseNode>emptyList();
            this.tables = Collections.<Table>singletonList(table);
            this.tableRefs = Collections.<TableRef>singletonList(table.getTableRef());
            this.allLeftJoin = false;
            this.hasRightJoin = false;
            this.prefilterAcceptedTables = Collections.<JoinTable>emptyList();
        }
        
        private JoinTable(Table table, List<JoinSpec> joinSpecs) {
            this.table = table;
            this.joinSpecs = joinSpecs;
            this.postFilters = new ArrayList<ParseNode>();
            this.tables = new ArrayList<Table>();
            this.tableRefs = new ArrayList<TableRef>();
            this.tables.add(table);
            boolean allLeftJoin = true;
            int lastRightJoinIndex = -1;
            for (int i = 0; i < joinSpecs.size(); i++) {
                JoinSpec joinSpec = joinSpecs.get(i);
                this.tables.addAll(joinSpec.getJoinTable().getTables());
                allLeftJoin = allLeftJoin && joinSpec.getType() == JoinType.Left;
                if (joinSpec.getType() == JoinType.Right) {
                    lastRightJoinIndex = i;
                }
            }
            for (Table t : this.tables) {
                this.tableRefs.add(t.getTableRef());
            }
            this.allLeftJoin = allLeftJoin;
            this.hasRightJoin = lastRightJoinIndex > -1;
            this.prefilterAcceptedTables = new ArrayList<JoinTable>();
            for (int i = lastRightJoinIndex == -1 ? 0 : lastRightJoinIndex; i < joinSpecs.size(); i++) {
                JoinSpec joinSpec = joinSpecs.get(i);
                if (joinSpec.getType() != JoinType.Left && joinSpec.getType() != JoinType.Anti) {
                    prefilterAcceptedTables.add(joinSpec.getJoinTable());
                }
            }
        }
        
        public Table getTable() {
            return table;
        }
        
        public List<JoinSpec> getJoinSpecs() {
            return joinSpecs;
        }
        
        public List<Table> getTables() {
            return tables;
        }
        
        public List<TableRef> getTableRefs() {
            return tableRefs;
        }
        
        public boolean isAllLeftJoin() {
            return allLeftJoin;
        }
        
        public SelectStatement getStatement() {
            return select;
        }
        
        public ColumnResolver getOriginalResolver() {
            return origResolver;
        }
        
        public Map<ColumnRef, ColumnRefType> getColumnRefs() {
            return columnRefs;
        }
        
        public void addFilter(ParseNode filter) throws SQLException {
            if (joinSpecs.isEmpty()) {
                table.addFilter(filter);
                return;
            }
            
            WhereNodeVisitor visitor = new WhereNodeVisitor(origResolver, table,
                    postFilters, Collections.<TableRef>singletonList(table.getTableRef()), 
                    hasRightJoin, prefilterAcceptedTables);
            filter.accept(visitor);
        }
        
        public void pushDownColumnRefVisitors(ColumnRefParseNodeVisitor generalRefVisitor, 
                ColumnRefParseNodeVisitor joinLocalRefVisitor, 
                ColumnRefParseNodeVisitor prefilterRefVisitor) throws SQLException {
            for (ParseNode node : table.getPreFilters()) {
                node.accept(prefilterRefVisitor);
            }
            for (ParseNode node : table.getPostFilters()) {
                node.accept(generalRefVisitor);
            }
            for (ParseNode node : postFilters) {
                node.accept(generalRefVisitor);
            }
            for (JoinSpec joinSpec : joinSpecs) {
                JoinTable joinTable = joinSpec.getJoinTable();
                boolean hasSubJoin = !joinTable.getJoinSpecs().isEmpty();
                for (ComparisonParseNode node : joinSpec.getOnConditions()) {
                    node.getLHS().accept(generalRefVisitor);
                    if (hasSubJoin) {
                        node.getRHS().accept(generalRefVisitor);
                    } else {
                        node.getRHS().accept(joinLocalRefVisitor);
                    }
                }
                joinTable.pushDownColumnRefVisitors(generalRefVisitor, joinLocalRefVisitor, prefilterRefVisitor);
            }
        }
        
        public Expression compilePostFilterExpression(StatementContext context, Table table) throws SQLException {
            List<ParseNode> filtersCombined = Lists.<ParseNode> newArrayList(postFilters);
            if (table != null) {
                filtersCombined.addAll(table.getPostFilters());
            }
            return JoinCompiler.compilePostFilterExpression(context, filtersCombined);
        }
        
        /**
         * Returns a boolean vector indicating whether the evaluation of join expressions
         * can be evaluated at an early stage if the input JoinSpec can be taken as a
         * star join. Otherwise returns null.  
         * @return a boolean vector for a star join; or null for non star join.
         */
        public boolean[] getStarJoinVector() {
            int count = joinSpecs.size();
            if (!table.isFlat() ||
                    (!useStarJoin 
                            && count > 1 
                            && joinSpecs.get(count - 1).getType() != JoinType.Left
                            && joinSpecs.get(count - 1).getType() != JoinType.Semi
                            && joinSpecs.get(count - 1).getType() != JoinType.Anti
                            && !joinSpecs.get(count - 1).isSingleValueOnly()))
                return null;

            boolean[] vector = new boolean[count];
            for (int i = 0; i < count; i++) {
                JoinSpec joinSpec = joinSpecs.get(i);
                if (joinSpec.getType() != JoinType.Left 
                        && joinSpec.getType() != JoinType.Inner
                        && joinSpec.getType() != JoinType.Semi
                        && joinSpec.getType() != JoinType.Anti)
                    return null;
                vector[i] = true;
                Iterator<TableRef> iter = joinSpec.getDependencies().iterator();
                while (vector[i] == true && iter.hasNext()) {
                    TableRef tableRef = iter.next();
                    if (!tableRef.equals(table.getTableRef())) {
                        vector[i] = false;
                    }
                }
            }
            
            return vector;
        }
        
        public JoinTable getSubJoinTableWithoutPostFilters() {
            return joinSpecs.size() > 1 ? new JoinTable(table, joinSpecs.subList(0, joinSpecs.size() - 1)) :
                new JoinTable(table);
        }
        
        public SelectStatement getAsSingleSubquery(SelectStatement query, boolean asSubquery) throws SQLException {
            if (!isFlat(query))
                throw new SQLFeatureNotSupportedException("Complex subqueries not supported as left join table.");
            
            if (asSubquery)
                return query;
            
            return NODE_FACTORY.select(query.getFrom(), select.getHint(), select.isDistinct(), select.getSelect(), query.getWhere(), select.getGroupBy(), select.getHaving(), select.getOrderBy(), select.getLimit(), select.getBindCount(), select.isAggregate(), select.hasSequence());            
        }
        
        public boolean hasPostReference() {
            for (Table table : tables) {
                if (table.isWildCardSelect()) {
                    return true;
                }
            }
            
            for (Map.Entry<ColumnRef, ColumnRefType> e : columnRefs.entrySet()) {
                if (e.getValue() == ColumnRefType.GENERAL && tableRefs.contains(e.getKey().getTableRef())) {
                    return true;
                }
            }
            
            return false;
        }
        
        public boolean hasFilters() {
           if (!postFilters.isEmpty())
               return true;
           
           if (!hasRightJoin && table.hasFilters())
               return true;
           
           for (JoinTable joinTable : prefilterAcceptedTables) {
               if (joinTable.hasFilters())
                   return true;
           }
           
           return false;
        }
    }
    
    public static class JoinSpec {
        private final JoinType type;
        private final List<ComparisonParseNode> onConditions;
        private final JoinTable joinTable;
        private final boolean singleValueOnly;
        private Set<TableRef> dependencies;
        
        private JoinSpec(JoinType type, ParseNode onNode, JoinTable joinTable, 
                boolean singleValueOnly, ColumnResolver resolver) throws SQLException {
            this.type = type;
            this.onConditions = new ArrayList<ComparisonParseNode>();
            this.joinTable = joinTable;
            this.singleValueOnly = singleValueOnly;
            this.dependencies = new HashSet<TableRef>();
            OnNodeVisitor visitor = new OnNodeVisitor(resolver, onConditions, dependencies, joinTable);
            onNode.accept(visitor);
            if (onConditions.isEmpty()) {
                visitor.throwUnsupportedJoinConditionException();
            }
        }
        
        public JoinType getType() {
            return type;
        }
        
        public List<ComparisonParseNode> getOnConditions() {
            return onConditions;
        }
        
        public JoinTable getJoinTable() {
            return joinTable;
        }
        
        public boolean isSingleValueOnly() {
            return singleValueOnly;
        }
        
        public Set<TableRef> getDependencies() {
            return dependencies;
        }
        
        public Pair<List<Expression>, List<Expression>> compileJoinConditions(StatementContext context, ColumnResolver leftResolver, ColumnResolver rightResolver) throws SQLException {
            ColumnResolver resolver = context.getResolver();
            List<Pair<Expression, Expression>> compiled = new ArrayList<Pair<Expression, Expression>>(onConditions.size());
            context.setResolver(leftResolver);
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            for (ParseNode condition : onConditions) {
                assert (condition instanceof EqualParseNode);
                EqualParseNode equalNode = (EqualParseNode) condition;
                expressionCompiler.reset();
                Expression left = equalNode.getLHS().accept(expressionCompiler);
                compiled.add(new Pair<Expression, Expression>(left, null));
            }
            context.setResolver(rightResolver);
            expressionCompiler = new ExpressionCompiler(context);
            Iterator<Pair<Expression, Expression>> iter = compiled.iterator();
            for (ParseNode condition : onConditions) {
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
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_MISMATCH)
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
    }
    
    public class Table {
        private final TableNode tableNode;
        private final List<ColumnDef> dynamicColumns;
        private final SelectStatement subselect;
        private final TableRef tableRef;
        private final List<AliasedNode> selectNodes; // all basic nodes related to this table, no aggregation.
        private final List<ParseNode> preFilters;
        private final List<ParseNode> postFilters;
        private final boolean isPostFilterConvertible;
        
        private Table(TableNode tableNode, List<ColumnDef> dynamicColumns, 
                List<AliasedNode> selectNodes, TableRef tableRef) {
            this.tableNode = tableNode;
            this.dynamicColumns = dynamicColumns;
            this.subselect = null;
            this.tableRef = tableRef;
            this.selectNodes = selectNodes;
            this.preFilters = new ArrayList<ParseNode>();
            this.postFilters = Collections.<ParseNode>emptyList();
            this.isPostFilterConvertible = false;
        }
        
        private Table(DerivedTableNode tableNode, 
                List<AliasedNode> selectNodes, TableRef tableRef) throws SQLException {
            this.tableNode = tableNode;
            this.dynamicColumns = Collections.<ColumnDef>emptyList();
            this.subselect = SubselectRewriter.flatten(tableNode.getSelect(), statement.getConnection());
            this.tableRef = tableRef;
            this.selectNodes = selectNodes;
            this.preFilters = new ArrayList<ParseNode>();
            this.postFilters = new ArrayList<ParseNode>();
            this.isPostFilterConvertible = SubselectRewriter.isPostFilterConvertible(subselect);
        }
        
        public TableNode getTableNode() {
            return tableNode;
        }
        
        public List<ColumnDef> getDynamicColumns() {
            return dynamicColumns;
        }
        
        public boolean isSubselect() {
            return subselect != null;
        }
        
        public List<AliasedNode> getSelectNodes() {
            return selectNodes;
        }
        
        public List<ParseNode> getPreFilters() {
            return preFilters;
        }
        
        public List<ParseNode> getPostFilters() {
            return postFilters;
        }
        
        public TableRef getTableRef() {
            return tableRef;
        }
        
        public void addFilter(ParseNode filter) {
            if (!isSubselect() || isPostFilterConvertible) {
                preFilters.add(filter);
            } else {
                postFilters.add(filter);
            }
        }
        
        public ParseNode getPreFiltersCombined() {
            return combine(preFilters);
        }
        
        public Expression compilePostFilterExpression(StatementContext context) throws SQLException {
            return JoinCompiler.compilePostFilterExpression(context, postFilters);
        }
        
        public SelectStatement getAsSubquery() throws SQLException {
            if (isSubselect())
                return SubselectRewriter.applyPostFilters(subselect, preFilters, tableNode.getAlias());
            
            List<TableNode> from = Collections.<TableNode>singletonList(tableNode);
            return NODE_FACTORY.select(from, select.getHint(), false, selectNodes, getPreFiltersCombined(), null, null, null, null, 0, false, select.hasSequence());
        }
        
        public boolean hasFilters() {
            return isSubselect() ? (!postFilters.isEmpty() || subselect.getWhere() != null || subselect.getHaving() != null) : !preFilters.isEmpty();
        }
        
        public boolean isFlat() {
            return subselect == null || JoinCompiler.isFlat(subselect);
        }
        
        protected boolean isWildCardSelect() {
            return (selectNodes.size() == 1 && selectNodes.get(0).getNode() instanceof TableWildcardParseNode);
        }

        public void projectColumns(Scan scan) {
            assert(!isSubselect());
            if (isWildCardSelect()) {
                scan.getFamilyMap().clear();
                return;
            }
            for (ColumnRef columnRef : columnRefs.keySet()) {
                if (columnRef.getTableRef().equals(tableRef)
                        && !SchemaUtil.isPKColumn(columnRef.getColumn())) {
                    scan.addColumn(columnRef.getColumn().getFamilyName().getBytes(), columnRef.getColumn().getName().getBytes());
                }
            }
        }
        
        public ProjectedPTableWrapper createProjectedTable(boolean retainPKColumns) throws SQLException {
            assert(!isSubselect());
            List<PColumn> projectedColumns = new ArrayList<PColumn>();
            List<Expression> sourceExpressions = new ArrayList<Expression>();
            ListMultimap<String, String> columnNameMap = ArrayListMultimap.<String, String>create();
            PTable table = tableRef.getTable();
            boolean hasSaltingColumn = retainPKColumns && table.getBucketNum() != null;
            if (retainPKColumns) {
                for (PColumn column : table.getPKColumns()) {
                    addProjectedColumn(projectedColumns, sourceExpressions, columnNameMap,
                            column, column.getFamilyName(), hasSaltingColumn);
                }
            }
            if (isWildCardSelect()) {
                for (PColumn column : table.getColumns()) {
                    if (!retainPKColumns || !SchemaUtil.isPKColumn(column)) {
                        addProjectedColumn(projectedColumns, sourceExpressions, columnNameMap,
                                column, PNameFactory.newName(TupleProjector.VALUE_COLUMN_FAMILY), hasSaltingColumn);
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
                                column, PNameFactory.newName(TupleProjector.VALUE_COLUMN_FAMILY), hasSaltingColumn);
                    }
                }               
            }
            
            PTable t = PTableImpl.makePTable(table.getTenantId(), PNameFactory.newName(PROJECTED_TABLE_SCHEMA), table.getName(), PTableType.JOIN,
                        table.getIndexState(), table.getTimeStamp(), table.getSequenceNumber(), table.getPKName(),
                        retainPKColumns ? table.getBucketNum() : null, projectedColumns, table.getParentSchemaName(),
                        table.getParentTableName(), table.getIndexes(), table.isImmutableRows(), Collections.<PName>emptyList(), null, null, table.isWALDisabled(), table.isMultiTenant(), table.getViewType(), table.getViewIndexId(), table.getIndexType());
            return new ProjectedPTableWrapper(t, columnNameMap, sourceExpressions);
        }
        
        private void addProjectedColumn(List<PColumn> projectedColumns, List<Expression> sourceExpressions,
                ListMultimap<String, String> columnNameMap, PColumn sourceColumn, PName familyName, boolean hasSaltingColumn) 
        throws SQLException {
            if (sourceColumn == SALTING_COLUMN)
                return;
            
            int position = projectedColumns.size() + (hasSaltingColumn ? 1 : 0);
            PTable table = tableRef.getTable();
            String schemaName = table.getSchemaName().getString();
            String tableName = table.getTableName().getString();
            String colName = sourceColumn.getName().getString();
            String fullName = getProjectedColumnName(schemaName, tableName, colName);
            String aliasedName = tableRef.getTableAlias() == null ? fullName : getProjectedColumnName(null, tableRef.getTableAlias(), colName);
            
            columnNameMap.put(colName, aliasedName);
            if (!fullName.equals(aliasedName)) {
                columnNameMap.put(fullName, aliasedName);
            }
            
            PName name = PNameFactory.newName(aliasedName);
            PColumnImpl column = new PColumnImpl(name, familyName, sourceColumn.getDataType(), 
                    sourceColumn.getMaxLength(), sourceColumn.getScale(), sourceColumn.isNullable(), 
                    position, sourceColumn.getSortOrder(), sourceColumn.getArraySize(), sourceColumn.getViewConstant(), sourceColumn.isViewReferenced());
            Expression sourceExpression = new ColumnRef(tableRef, sourceColumn.getPosition()).newColumnExpression();
            projectedColumns.add(column);
            sourceExpressions.add(sourceExpression);
        }
        
        public ProjectedPTableWrapper createProjectedTable(RowProjector rowProjector) throws SQLException {
            assert(isSubselect());
            List<PColumn> projectedColumns = new ArrayList<PColumn>();
            List<Expression> sourceExpressions = new ArrayList<Expression>();
            ListMultimap<String, String> columnNameMap = ArrayListMultimap.<String, String>create();
            PTable table = tableRef.getTable();
            for (PColumn column : table.getColumns()) {
                String colName = getProjectedColumnName(null, tableRef.getTableAlias(), column.getName().getString());
                Expression sourceExpression = rowProjector.getColumnProjector(column.getPosition()).getExpression();
                PColumnImpl projectedColumn = new PColumnImpl(PNameFactory.newName(colName), PNameFactory.newName(TupleProjector.VALUE_COLUMN_FAMILY), 
                        sourceExpression.getDataType(), sourceExpression.getMaxLength(), sourceExpression.getScale(), sourceExpression.isNullable(), 
                        column.getPosition(), sourceExpression.getSortOrder(), column.getArraySize(), column.getViewConstant(), column.isViewReferenced());                
                projectedColumns.add(projectedColumn);
                sourceExpressions.add(sourceExpression);
            }
            PTable t = PTableImpl.makePTable(table.getTenantId(), PNameFactory.newName(PROJECTED_TABLE_SCHEMA), table.getName(), PTableType.JOIN,
                        table.getIndexState(), table.getTimeStamp(), table.getSequenceNumber(), table.getPKName(),
                        null, projectedColumns, table.getParentSchemaName(),
                        table.getParentTableName(), table.getIndexes(), table.isImmutableRows(), Collections.<PName>emptyList(), null, null, table.isWALDisabled(), table.isMultiTenant(), table.getViewType(), table.getViewIndexId(), table.getIndexType());
            return new ProjectedPTableWrapper(t, columnNameMap, sourceExpressions);
        }
    }
    
    private static class WhereNodeVisitor extends BooleanParseNodeVisitor<Void> {
        private Table table;
        private List<ParseNode> postFilters;
        private List<TableRef> selfTableRefs;
        private boolean hasRightJoin;
        private List<JoinTable> prefilterAcceptedTables;
        ColumnRefParseNodeVisitor columnRefVisitor;
        
        public WhereNodeVisitor(ColumnResolver resolver, Table table,
                List<ParseNode> postFilters, List<TableRef> selfTableRefs, boolean hasRightJoin, 
                List<JoinTable> prefilterAcceptedTables) {
            this.table = table;
            this.postFilters = postFilters;
            this.selfTableRefs = selfTableRefs;
            this.hasRightJoin = hasRightJoin;
            this.prefilterAcceptedTables = prefilterAcceptedTables;
            this.columnRefVisitor = new ColumnRefParseNodeVisitor(resolver);
        }
        
        @Override
        protected boolean enterBooleanNode(ParseNode node) throws SQLException {
            return false;
        }
        
        @Override
        protected Void leaveBooleanNode(ParseNode node,
                List<Void> l) throws SQLException {
            columnRefVisitor.reset();
            node.accept(columnRefVisitor);
            ColumnRefParseNodeVisitor.ColumnRefType type = columnRefVisitor.getContentType(selfTableRefs);
            switch (type) {
            case NONE:
            case SELF_ONLY:
                if (!hasRightJoin) {
                    table.addFilter(node);
                } else {
                    postFilters.add(node);
                }
                break;
            case FOREIGN_ONLY:
                JoinTable matched = null;
                for (JoinTable joinTable : prefilterAcceptedTables) {
                    if (columnRefVisitor.getContentType(joinTable.getTableRefs()) == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                        matched = joinTable;
                        break;
                    }
                }
                if (matched != null) {
                    matched.addFilter(node);
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
        protected boolean enterNonBooleanNode(ParseNode node) throws SQLException {
            return false;
        }
        
        @Override
        protected Void leaveNonBooleanNode(ParseNode node, List<Void> l) throws SQLException {
            return null;
        }
        
        @Override
        public boolean visitEnter(AndParseNode node) throws SQLException {
            return true;
        }

        @Override
        public Void visitLeave(AndParseNode node, List<Void> l) throws SQLException {
            return null;
        }
    }
    
    private static class OnNodeVisitor extends BooleanParseNodeVisitor<Void> {
        private List<ComparisonParseNode> onConditions;
        private Set<TableRef> dependencies;
        private JoinTable joinTable;
        private ColumnRefParseNodeVisitor columnRefVisitor;
        
        public OnNodeVisitor(ColumnResolver resolver, List<ComparisonParseNode> onConditions, 
                Set<TableRef> dependencies, JoinTable joinTable) {
            this.onConditions = onConditions;
            this.dependencies = dependencies;
            this.joinTable = joinTable;
            this.columnRefVisitor = new ColumnRefParseNodeVisitor(resolver);
        }
        
        @Override
        protected boolean enterBooleanNode(ParseNode node) throws SQLException {
            return false;
        }
        
        @Override
        protected Void leaveBooleanNode(ParseNode node,
                List<Void> l) throws SQLException {
            columnRefVisitor.reset();
            node.accept(columnRefVisitor);
            ColumnRefParseNodeVisitor.ColumnRefType type = columnRefVisitor.getContentType(joinTable.getTableRefs());
            if (type == ColumnRefParseNodeVisitor.ColumnRefType.NONE 
                    || type == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                joinTable.addFilter(node);
            } else {
                throwUnsupportedJoinConditionException();
            }
            return null;
        }
        
        @Override
        protected boolean enterNonBooleanNode(ParseNode node) throws SQLException {
            return false;
        }
        
        @Override
        protected Void leaveNonBooleanNode(ParseNode node, List<Void> l) throws SQLException {
            return null;
        }
        
        @Override
        public boolean visitEnter(AndParseNode node) throws SQLException {
            return true;
        }

        @Override
        public Void visitLeave(AndParseNode node, List<Void> l) throws SQLException {
            return null;
        }
        
        @Override
        public Void visitLeave(ComparisonParseNode node, List<Void> l) 
                throws SQLException {
            if (!(node instanceof EqualParseNode))
                return leaveBooleanNode(node, l);
            columnRefVisitor.reset();
            node.getLHS().accept(columnRefVisitor);
            ColumnRefParseNodeVisitor.ColumnRefType lhsType = columnRefVisitor.getContentType(joinTable.getTableRefs());
            Set<TableRef> lhsTableRefSet = Sets.newHashSet(columnRefVisitor.getTableRefSet());
            columnRefVisitor.reset();
            node.getRHS().accept(columnRefVisitor);
            ColumnRefParseNodeVisitor.ColumnRefType rhsType = columnRefVisitor.getContentType(joinTable.getTableRefs());
            Set<TableRef> rhsTableRefSet = Sets.newHashSet(columnRefVisitor.getTableRefSet());
            if ((lhsType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY || lhsType == ColumnRefParseNodeVisitor.ColumnRefType.NONE)
                    && (rhsType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY || rhsType == ColumnRefParseNodeVisitor.ColumnRefType.NONE)) {
                joinTable.addFilter(node);
            } else if (lhsType == ColumnRefParseNodeVisitor.ColumnRefType.FOREIGN_ONLY 
                    && rhsType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                onConditions.add(node);
                dependencies.addAll(lhsTableRefSet);
            } else if (rhsType == ColumnRefParseNodeVisitor.ColumnRefType.FOREIGN_ONLY 
                    && lhsType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                onConditions.add(NODE_FACTORY.equal(node.getRHS(), node.getLHS()));
                dependencies.addAll(rhsTableRefSet);
            } else {
                throwUnsupportedJoinConditionException();
            }
            return null;
        }

        /*
         * Conditions in the ON clause can only be:
         * 1) an equal test between a self table expression and a foreign 
         *    table expression.
         * 2) a boolean condition referencing to the self table only.
         * Otherwise, it can be ambiguous.
         */
        public void throwUnsupportedJoinConditionException() 
                throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException("Does not support non-standard or non-equi join conditions.");
        }           
    }

    private static class ColumnRefParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
        public enum ColumnRefType {NONE, SELF_ONLY, FOREIGN_ONLY, COMPLEX};

        private ColumnResolver resolver;
        private final Set<TableRef> tableRefSet;
        private final Map<ColumnRef, ColumnParseNode> columnRefMap;

        public ColumnRefParseNodeVisitor(ColumnResolver resolver) {
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

        public ColumnRefType getContentType(List<TableRef> selfTableRefs) {
            if (tableRefSet.isEmpty())
                return ColumnRefType.NONE;

            ColumnRefType ret = ColumnRefType.NONE;
            for (TableRef tRef : tableRefSet) {
                boolean isSelf = selfTableRefs.contains(tRef);
                switch (ret) {
                case NONE:
                    ret = isSelf ? ColumnRefType.SELF_ONLY : ColumnRefType.FOREIGN_ONLY;
                    break;
                case SELF_ONLY:
                    ret = isSelf ? ColumnRefType.SELF_ONLY : ColumnRefType.COMPLEX;
                    break;
                case FOREIGN_ONLY:
                    ret = isSelf ? ColumnRefType.COMPLEX : ColumnRefType.FOREIGN_ONLY;
                    break;
                default: // COMPLEX do nothing
                    break;    
                }

                if (ret == ColumnRefType.COMPLEX) {
                    break;
                }
            }

            return ret;
        }
    }
    
    private static String PROJECTED_TABLE_SCHEMA = ".";
    // for creation of new statements
    private static ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    private static boolean isFlat(SelectStatement select) {
        return !select.isJoin() 
                && !select.isAggregate() 
                && !select.isDistinct() 
                && !(select.getFrom().get(0) instanceof DerivedTableNode)
                && select.getLimit() == null;
    }
    
    private static ParseNode combine(List<ParseNode> nodes) {
        if (nodes.isEmpty())
            return null;
        
        if (nodes.size() == 1)
            return nodes.get(0);
        
        return NODE_FACTORY.and(nodes);
    }
    
    private static List<AliasedNode> extractFromSelect(List<AliasedNode> select, TableRef tableRef, ColumnResolver resolver) throws SQLException {
        List<AliasedNode> ret = new ArrayList<AliasedNode>();
        ColumnRefParseNodeVisitor visitor = new ColumnRefParseNodeVisitor(resolver);
        for (AliasedNode aliasedNode : select) {
            ParseNode node = aliasedNode.getNode();
            if (node instanceof TableWildcardParseNode) {
                TableName tableName = ((TableWildcardParseNode) node).getTableName();
                if (tableRef.equals(resolver.resolveTable(tableName.getSchemaName(), tableName.getTableName()))) {
                    ret.clear();
                    ret.add(aliasedNode);
                    return ret;
                }
                continue;
            }
            
            node.accept(visitor);
            ColumnRefParseNodeVisitor.ColumnRefType type = visitor.getContentType(Collections.singletonList(tableRef));
            if (type == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                ret.add(aliasedNode);
            } else if (type == ColumnRefParseNodeVisitor.ColumnRefType.COMPLEX) {
                for (Map.Entry<ColumnRef, ColumnParseNode> entry : visitor.getColumnRefMap().entrySet()) {
                    if (entry.getKey().getTableRef().equals(tableRef)) {
                        ret.add(NODE_FACTORY.aliasedNode(null, entry.getValue()));
                    }
                }
            }
            visitor.reset();
        }
        return ret;
    }
    
    private static Expression compilePostFilterExpression(StatementContext context, List<ParseNode> postFilters) throws SQLException {
        if (postFilters.isEmpty())
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
    
    public static SelectStatement optimize(PhoenixStatement statement, SelectStatement select, final ColumnResolver resolver) throws SQLException {
        TableRef groupByTableRef = null;
        TableRef orderByTableRef = null;
        if (select.getGroupBy() != null && !select.getGroupBy().isEmpty()) {
            ColumnRefParseNodeVisitor groupByVisitor = new ColumnRefParseNodeVisitor(resolver);
            for (ParseNode node : select.getGroupBy()) {
                node.accept(groupByVisitor);
            }
            Set<TableRef> set = groupByVisitor.getTableRefSet();
            if (set.size() == 1) {
                groupByTableRef = set.iterator().next();
            }
        } else if (select.getOrderBy() != null && !select.getOrderBy().isEmpty()) {
            ColumnRefParseNodeVisitor orderByVisitor = new ColumnRefParseNodeVisitor(resolver);
            for (OrderByNode node : select.getOrderBy()) {
                node.getNode().accept(orderByVisitor);
            }
            Set<TableRef> set = orderByVisitor.getTableRefSet();
            if (set.size() == 1) {
                orderByTableRef = set.iterator().next();
            }
        }
        JoinTable join = compile(statement, select, resolver);
        if (groupByTableRef != null || orderByTableRef != null) {
            QueryCompiler compiler = new QueryCompiler(statement, select, resolver);
            List<Object> binds = statement.getParameters();
            StatementContext ctx = new StatementContext(statement, resolver, new Scan(), new SequenceManager(statement));
            QueryPlan plan = compiler.compileJoinQuery(ctx, binds, join, false);
            TableRef table = plan.getTableRef();
            if (groupByTableRef != null && !groupByTableRef.equals(table)) {
                groupByTableRef = null;
            }
            if (orderByTableRef != null && !orderByTableRef.equals(table)) {
                orderByTableRef = null;
            }            
        }
        
        final Map<TableRef, TableRef> replacement = new HashMap<TableRef, TableRef>();
        
        for (Table table : join.getTables()) {
            if (table.isSubselect())
                continue;
            TableRef tableRef = table.getTableRef();
            List<ParseNode> groupBy = tableRef.equals(groupByTableRef) ? select.getGroupBy() : null;
            List<OrderByNode> orderBy = tableRef.equals(orderByTableRef) ? select.getOrderBy() : null;
            SelectStatement stmt = getSubqueryForOptimizedPlan(select.getHint(), table.getDynamicColumns(), tableRef, join.getColumnRefs(), table.getPreFiltersCombined(), groupBy, orderBy, table.isWildCardSelect(), select.hasSequence());
            QueryPlan plan = statement.getConnection().getQueryServices().getOptimizer().optimize(statement, stmt);
            if (!plan.getTableRef().equals(tableRef)) {
                replacement.put(tableRef, plan.getTableRef());
            }            
        }
        
        if (replacement.isEmpty()) 
            return select;
        
        List<TableNode> from = select.getFrom();
        List<TableNode> newFrom = Lists.newArrayListWithExpectedSize(from.size());
        for (TableNode node : from) {
            newFrom.add(node.accept(new TableNodeVisitor<TableNode>() {
                private TableRef resolveTable(String alias, TableName name) throws SQLException {
                    if (alias != null)
                        return resolver.resolveTable(null, alias);

                    return resolver.resolveTable(name.getSchemaName(), name.getTableName());
                }

                private TableName getReplacedTableName(TableRef tableRef) {
                    String schemaName = tableRef.getTable().getSchemaName().getString();
                    return TableName.create(schemaName.length() == 0 ? null : schemaName, tableRef.getTable().getTableName().getString());
                }

                @Override
                public TableNode visit(BindTableNode boundTableNode) throws SQLException {
                    TableRef tableRef = resolveTable(boundTableNode.getAlias(), boundTableNode.getName());
                    TableRef replaceRef = replacement.get(tableRef);
                    if (replaceRef == null)
                        return boundTableNode;

                    String alias = boundTableNode.getAlias();
                    return NODE_FACTORY.bindTable(alias == null ? null : '"' + alias + '"', getReplacedTableName(replaceRef));
                }

                @Override
                public TableNode visit(JoinTableNode joinNode) throws SQLException {
                    TableNode lhs = joinNode.getLHS();
                    TableNode rhs = joinNode.getRHS();
                    TableNode lhsReplace = lhs.accept(this);
                    TableNode rhsReplace = rhs.accept(this);
                    if (lhs == lhsReplace && rhs == rhsReplace)
                        return joinNode;

                    return NODE_FACTORY.join(joinNode.getType(), lhsReplace, rhsReplace, joinNode.getOnNode(), joinNode.isSingleValueOnly());
                }

                @Override
                public TableNode visit(NamedTableNode namedTableNode)
                        throws SQLException {
                    TableRef tableRef = resolveTable(namedTableNode.getAlias(), namedTableNode.getName());
                    TableRef replaceRef = replacement.get(tableRef);
                    if (replaceRef == null)
                        return namedTableNode;

                    String alias = namedTableNode.getAlias();
                    return NODE_FACTORY.namedTable(alias == null ? null : '"' + alias + '"', getReplacedTableName(replaceRef), namedTableNode.getDynamicColumns());
                }

                @Override
                public TableNode visit(DerivedTableNode subselectNode)
                        throws SQLException {
                    return subselectNode;
                }
            }));
        }
        
        return IndexStatementRewriter.translate(NODE_FACTORY.select(select, newFrom), resolver, replacement);        
    }
    
    private static SelectStatement getSubqueryForOptimizedPlan(HintNode hintNode, List<ColumnDef> dynamicCols, TableRef tableRef, Map<ColumnRef, ColumnRefType> columnRefs, ParseNode where, List<ParseNode> groupBy,
            List<OrderByNode> orderBy, boolean isWildCardSelect, boolean hasSequence) {
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

        // TODO: review, as it seems like we're potentially losing if the select statement is an aggregate (i.e. if it's an ungrouped aggregate for example)
        return NODE_FACTORY.select(from, hintNode, false, selectList, where, groupBy, null, orderBy, null, 0, groupBy != null, hasSequence);
    }
    
    public class PTableWrapper {
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
        
        public ColumnResolver createColumnResolver() {
            return new JoinedTableColumnResolver(this, origResolver);
        }
        
        public PTableWrapper mergeProjectedTables(PTableWrapper rWrapper, boolean innerJoin) throws SQLException {
            PTable left = this.getTable();
            PTable right = rWrapper.getTable();
            List<PColumn> merged = new ArrayList<PColumn>();
            merged.addAll(left.getColumns());
            int position = merged.size();
            for (PColumn c : right.getColumns()) {
                if (!SchemaUtil.isPKColumn(c)) {
                    PColumnImpl column = new PColumnImpl(c.getName(), 
                            PNameFactory.newName(TupleProjector.VALUE_COLUMN_FAMILY), c.getDataType(), 
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
                    left.getParentSchemaName(), left.getParentTableName(), left.getIndexes(), left.isImmutableRows(), Collections.<PName>emptyList(), null, null, PTable.DEFAULT_DISABLE_WAL, left.isMultiTenant(), left.getViewType(), left.getViewIndexId(), left.getIndexType());

            ListMultimap<String, String> mergedMap = ArrayListMultimap.<String, String>create();
            mergedMap.putAll(this.getColumnNameMap());
            mergedMap.putAll(rWrapper.getColumnNameMap());
            
            return new PTableWrapper(t, mergedMap);
        }
    }
    
    public class ProjectedPTableWrapper extends PTableWrapper {
    	private List<Expression> sourceExpressions;
    	
    	protected ProjectedPTableWrapper(PTable table, ListMultimap<String, String> columnNameMap, List<Expression> sourceExpressions) {
    		super(table, columnNameMap);
    		this.sourceExpressions = sourceExpressions;
    	}
    	
    	public Expression getSourceExpression(PColumn column) {
    		return sourceExpressions.get(column.getPosition() - (table.getBucketNum() == null ? 0 : 1));
    	}
        
        public TupleProjector createTupleProjector() {
            return new TupleProjector(this);
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

}


