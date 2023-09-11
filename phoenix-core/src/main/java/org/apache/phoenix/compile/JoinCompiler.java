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

import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.MinAggregateFunction;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AndBooleanParseNodeVisitor;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.AndRewriterBooleanParseNodeVisitor;
import org.apache.phoenix.parse.BindTableNode;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.ConcreteTableNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.EqualParseNode;
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
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.IndexUncoveredDataColumnRef;
import org.apache.phoenix.schema.MetaDataEntityNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ParseNodeUtil;
import org.apache.phoenix.util.ParseNodeUtil.RewriteResult;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;


public class JoinCompiler {

    public enum Strategy {
        HASH_BUILD_LEFT,
        HASH_BUILD_RIGHT,
        SORT_MERGE,
    }

    public enum ColumnRefType {
        JOINLOCAL,
        GENERAL,
    }

    private final PhoenixStatement phoenixStatement;
    /**
     * The original join sql for current {@link JoinCompiler}.
     */
    private final SelectStatement originalJoinSelectStatement;
    private final ColumnResolver origResolver;
    private final boolean useStarJoin;
    private final Map<ColumnRef, ColumnRefType> columnRefs;
    private final Map<ColumnRef, ColumnParseNode> columnNodes;
    private final boolean useSortMergeJoin;

    private JoinCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver) {
        this.phoenixStatement = statement;
        this.originalJoinSelectStatement = select;
        this.origResolver = resolver;
        this.useStarJoin = !select.getHint().hasHint(Hint.NO_STAR_JOIN);
        this.columnRefs = new HashMap<ColumnRef, ColumnRefType>();
        this.columnNodes = new HashMap<ColumnRef, ColumnParseNode>();
        this.useSortMergeJoin = select.getHint().hasHint(Hint.USE_SORT_MERGE_JOIN);
    }

    /**
     * After this method is called, the inner state of the parameter resolver may be changed by
     * {@link FromCompiler#refreshDerivedTableNode} because of some sql optimization,
     * see also {@link Table#pruneSubselectAliasedNodes()}.
     * @param statement
     * @param select
     * @param resolver
     * @return
     * @throws SQLException
     */
    public static JoinTable compile(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver) throws SQLException {
        JoinCompiler compiler = new JoinCompiler(statement, select, resolver);
        JoinTableConstructor constructor = compiler.new JoinTableConstructor();
        Pair<Table, List<JoinSpec>> res = select.getFrom().accept(constructor);
        JoinTable joinTable = res.getSecond() == null ? compiler.new JoinTable(res.getFirst()) : compiler.new JoinTable(res.getFirst(), res.getSecond());
        if (select.getWhere() != null) {
            joinTable.pushDownFilter(select.getWhere());
        }

        ColumnRefParseNodeVisitor generalRefVisitor = new ColumnRefParseNodeVisitor(resolver, statement.getConnection());
        ColumnRefParseNodeVisitor joinLocalRefVisitor = new ColumnRefParseNodeVisitor(resolver, statement.getConnection());

        joinTable.pushDownColumnRefVisitors(generalRefVisitor, joinLocalRefVisitor);

        ParseNodeUtil.applyParseNodeVisitor(select, generalRefVisitor, false);

        compiler.columnNodes.putAll(joinLocalRefVisitor.getColumnRefMap());
        compiler.columnNodes.putAll(generalRefVisitor.getColumnRefMap());

        for (ColumnRef ref : generalRefVisitor.getColumnRefMap().keySet()) {
            compiler.columnRefs.put(ref, ColumnRefType.GENERAL);
        }
        for (ColumnRef ref : joinLocalRefVisitor.getColumnRefMap().keySet()) {
            if (!compiler.columnRefs.containsKey(ref))
                compiler.columnRefs.put(ref, ColumnRefType.JOINLOCAL);
        }

        /**
         * After {@link ColumnRefParseNodeVisitor} is pushed down,
         * pruning columns for each {@link JoinCompiler.Table} if
         * {@link @link JoinCompiler.Table#isSubselect()}.
         */
        joinTable.pruneSubselectAliasedNodes();
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
            boolean isWildCard = isWildCardSelectForTable(originalJoinSelectStatement.getSelect(), tableRef, origResolver);
            Table table = new Table(boundTableNode, isWildCard, Collections.<ColumnDef>emptyList(), boundTableNode.getTableSamplingRate(), tableRef);
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
            boolean isWildCard = isWildCardSelectForTable(originalJoinSelectStatement.getSelect(), tableRef, origResolver);
            Table table = new Table(namedTableNode, isWildCard, namedTableNode.getDynamicColumns(), namedTableNode.getTableSamplingRate(), tableRef);
            return new Pair<Table, List<JoinSpec>>(table, null);
        }

        @Override
        public Pair<Table, List<JoinSpec>> visit(DerivedTableNode subselectNode)
                throws SQLException {
            TableRef tableRef = resolveTable(subselectNode.getAlias(), null);
            boolean isWildCard = isWildCardSelectForTable(originalJoinSelectStatement.getSelect(), tableRef, origResolver);
            Table table = new Table(subselectNode, isWildCard, tableRef);
            return new Pair<Table, List<JoinSpec>>(table, null);
        }
    }

    public class JoinTable {
        private final Table leftTable;
        private final List<JoinSpec> joinSpecs;
        private List<ParseNode> postFilters;
        private final List<Table> allTables;
        private final List<TableRef> allTableRefs;
        private final boolean allLeftJoin;
        private final boolean isPrefilterAccepted;
        private final List<JoinSpec> prefilterAcceptedTables;

        private JoinTable(Table table) {
            this.leftTable = table;
            this.joinSpecs = Collections.<JoinSpec>emptyList();
            this.postFilters = Collections.EMPTY_LIST;
            this.allTables = Collections.<Table>singletonList(table);
            this.allTableRefs = Collections.<TableRef>singletonList(table.getTableRef());
            this.allLeftJoin = false;
            this.isPrefilterAccepted = true;
            this.prefilterAcceptedTables = Collections.<JoinSpec>emptyList();
        }

        private JoinTable(Table table, List<JoinSpec> joinSpecs) {
            this.leftTable = table;
            this.joinSpecs = joinSpecs;
            this.postFilters = new ArrayList<ParseNode>();
            this.allTables = new ArrayList<Table>();
            this.allTableRefs = new ArrayList<TableRef>();
            this.allTables.add(table);
            boolean allLeftJoin = true;
            int lastRightJoinIndex = -1;
            boolean hasFullJoin = false;
            for (int i = 0; i < joinSpecs.size(); i++) {
                JoinSpec joinSpec = joinSpecs.get(i);
                this.allTables.addAll(joinSpec.getRhsJoinTable().getAllTables());
                allLeftJoin = allLeftJoin && joinSpec.getType() == JoinType.Left;
                hasFullJoin = hasFullJoin || joinSpec.getType() == JoinType.Full;
                if (joinSpec.getType() == JoinType.Right) {
                    lastRightJoinIndex = i;
                }
            }
            for (Table t : this.allTables) {
                this.allTableRefs.add(t.getTableRef());
            }
            this.allLeftJoin = allLeftJoin;
            this.isPrefilterAccepted = !hasFullJoin && lastRightJoinIndex == -1;
            this.prefilterAcceptedTables = new ArrayList<JoinSpec>();
            for (int i = lastRightJoinIndex == -1 ? 0 : lastRightJoinIndex; i < joinSpecs.size(); i++) {
                JoinSpec joinSpec = joinSpecs.get(i);
                if (joinSpec.getType() != JoinType.Left && joinSpec.getType() != JoinType.Anti && joinSpec.getType() != JoinType.Full) {
                    prefilterAcceptedTables.add(joinSpec);
                }
            }
        }

        public Table getLeftTable() {
            return leftTable;
        }

        public List<JoinSpec> getJoinSpecs() {
            return joinSpecs;
        }

        public List<Table> getAllTables() {
            return allTables;
        }

        public List<TableRef> getAllTableRefs() {
            return allTableRefs;
        }

        public List<TableRef> getLeftTableRef() {
            return Collections.<TableRef>singletonList(leftTable.getTableRef());
        }

        public boolean isAllLeftJoin() {
            return allLeftJoin;
        }

        public SelectStatement getOriginalJoinSelectStatement() {
            return originalJoinSelectStatement;
        }

        public ColumnResolver getOriginalResolver() {
            return origResolver;
        }

        public Map<ColumnRef, ColumnRefType> getColumnRefs() {
            return columnRefs;
        }

        public ParseNode getPostFiltersCombined() {
            return combine(postFilters);
        }

        public void addPostJoinFilter(ParseNode parseNode) {
            if(this.postFilters == Collections.EMPTY_LIST) {
                this.postFilters = new ArrayList<ParseNode>();
            }
            this.postFilters.add(parseNode);
        }

        public void addLeftTableFilter(ParseNode parseNode) throws SQLException {
            if (isPrefilterAccepted) {
                leftTable.addFilter(parseNode);
            } else {
                addPostJoinFilter(parseNode);
            }
        }

        public List<JoinSpec> getPrefilterAcceptedJoinSpecs() {
            return this.prefilterAcceptedTables;
        }

        /**
         * try to decompose filter and push down to single table.
         * @param filter
         * @throws SQLException
         */
        public void pushDownFilter(ParseNode filter) throws SQLException {
            if (joinSpecs.isEmpty()) {
                leftTable.addFilter(filter);
                return;
            }

            WhereNodeVisitor visitor = new WhereNodeVisitor(
                    origResolver,
                    this,
                    phoenixStatement.getConnection());
            filter.accept(visitor);
        }

        public void pushDownColumnRefVisitors(
                ColumnRefParseNodeVisitor generalRefVisitor,
                ColumnRefParseNodeVisitor joinLocalRefVisitor) throws SQLException {
            for (ParseNode node : leftTable.getPostFilterParseNodes()) {
                node.accept(generalRefVisitor);
            }
            for (ParseNode node : postFilters) {
                node.accept(generalRefVisitor);
            }
            for (JoinSpec joinSpec : joinSpecs) {
                JoinTable joinTable = joinSpec.getRhsJoinTable();
                boolean hasSubJoin = !joinTable.getJoinSpecs().isEmpty();
                for (EqualParseNode node : joinSpec.getOnConditions()) {
                    node.getLHS().accept(generalRefVisitor);
                    if (hasSubJoin) {
                        node.getRHS().accept(generalRefVisitor);
                    } else {
                        node.getRHS().accept(joinLocalRefVisitor);
                    }
                }
                joinTable.pushDownColumnRefVisitors(generalRefVisitor, joinLocalRefVisitor);
            }
        }

        /**
         * Pruning columns for each {@link JoinCompiler.Table} if
         * {@link JoinCompiler.Table#isSubselect()}.
         * @throws SQLException
         */
        public void pruneSubselectAliasedNodes() throws SQLException {
            this.leftTable.pruneSubselectAliasedNodes();
            for (JoinSpec joinSpec : joinSpecs) {
                JoinTable rhsJoinTablesContext = joinSpec.getRhsJoinTable();;
                rhsJoinTablesContext.pruneSubselectAliasedNodes();
            }
        }

        public Expression compilePostFilterExpression(StatementContext context) throws SQLException {
            List<ParseNode> filtersCombined = Lists.<ParseNode> newArrayList(postFilters);
            return JoinCompiler.compilePostFilterExpression(context, filtersCombined);
        }

        /**
         * Return a list of all applicable join strategies. The order of the strategies in the
         * returned list is based on the static rule below. However, the caller can decide on
         * an optimal join strategy by evaluating and comparing the costs.
         * 1. If hint USE_SORT_MERGE_JOIN is specified,
         *    return a singleton list containing only SORT_MERGE.
         * 2. If 1) matches pattern "A LEFT/INNER/SEMI/ANTI JOIN B"; or
         *       2) matches pattern "A LEFT/INNER/SEMI/ANTI JOIN B (LEFT/INNER/SEMI/ANTI JOIN C)+"
         *          and hint NO_STAR_JOIN is not specified,
         *    add BUILD_RIGHT to the returned list.
         * 3. If matches pattern "A RIGHT/INNER JOIN B", where B is either a named table reference
         *    or a flat sub-query,
         *    add BUILD_LEFT to the returned list.
         * 4. add SORT_MERGE to the returned list.
         * @throws SQLException
         */
        public List<Strategy> getApplicableJoinStrategies() throws SQLException {
            List<Strategy> strategies = Lists.newArrayList();
            if (useSortMergeJoin) {
                strategies.add(Strategy.SORT_MERGE);
            } else {
                if (getStarJoinVector() != null) {
                    strategies.add(Strategy.HASH_BUILD_RIGHT);
                }
                JoinSpec lastJoinSpec = joinSpecs.get(joinSpecs.size() - 1);
                JoinType type = lastJoinSpec.getType();
                if ((type == JoinType.Right || type == JoinType.Inner)
                        && lastJoinSpec.getRhsJoinTable().getJoinSpecs().isEmpty()
                        && lastJoinSpec.getRhsJoinTable().getLeftTable().isCouldPushToServerAsHashJoinProbeSide()) {
                    strategies.add(Strategy.HASH_BUILD_LEFT);
                }
                strategies.add(Strategy.SORT_MERGE);
            }

            return strategies;
        }

        /**
         * Returns a boolean vector indicating whether the evaluation of join expressions
         * can be evaluated at an early stage if the input JoinSpec can be taken as a
         * star join. Otherwise returns null.
         * @return a boolean vector for a star join; or null for non star join.
         * @throws SQLException
         */
        public boolean[] getStarJoinVector() throws SQLException {
            int count = joinSpecs.size();
            if (!leftTable.isCouldPushToServerAsHashJoinProbeSide() ||
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
                Iterator<TableRef> iter = joinSpec.getDependentTableRefs().iterator();
                while (vector[i] == true && iter.hasNext()) {
                    TableRef tableRef = iter.next();
                    if (!tableRef.equals(leftTable.getTableRef())) {
                        vector[i] = false;
                    }
                }
            }

            return vector;
        }

        /**
         * create a new {@link JoinTable} exclude the last {@link JoinSpec},
         * and try to push {@link #postFilters} to the new {@link JoinTable}.
         * @param phoenixConnection
         * @return
         * @throws SQLException
         */
        public JoinTable createSubJoinTable(
                PhoenixConnection phoenixConnection) throws SQLException {
            assert joinSpecs.size() > 0;
            JoinTable newJoinTablesContext = joinSpecs.size() > 1 ?
                    new JoinTable(leftTable, joinSpecs.subList(0, joinSpecs.size() - 1)) :
                    new JoinTable(leftTable);
            JoinType rightmostJoinType = joinSpecs.get(joinSpecs.size() - 1).getType();
            if(rightmostJoinType == JoinType.Right || rightmostJoinType == JoinType.Full) {
                return newJoinTablesContext;
            }

            if(this.postFilters.isEmpty()) {
                return newJoinTablesContext;
            }

            PushDownPostFilterParseNodeVisitor pushDownPostFilterNodeVistor =
                    new PushDownPostFilterParseNodeVisitor(
                            JoinCompiler.this.origResolver,
                            newJoinTablesContext,
                            phoenixConnection);
            int index = 0;
            List<ParseNode> newPostFilterParseNodes = null;
            for(ParseNode postFilterParseNode : this.postFilters) {
                ParseNode newPostFilterParseNode =
                        postFilterParseNode.accept(pushDownPostFilterNodeVistor);
                if(newPostFilterParseNode != postFilterParseNode &&
                   newPostFilterParseNodes == null) {
                    newPostFilterParseNodes =
                            new ArrayList<ParseNode>(this.postFilters.subList(0, index));
                }
                if(newPostFilterParseNodes != null && newPostFilterParseNode != null) {
                    newPostFilterParseNodes.add(newPostFilterParseNode);
                }
                index++;
            }
            if(newPostFilterParseNodes != null) {
                this.postFilters = newPostFilterParseNodes;
            }
            return newJoinTablesContext;
        }

        public SelectStatement getAsSingleSubquery(SelectStatement query, boolean asSubquery) throws SQLException {
            assert (isCouldPushToServerAsHashJoinProbeSide(query));

            if (asSubquery)
                return query;

            return NODE_FACTORY.select(originalJoinSelectStatement, query.getFrom(), query.getWhere());
        }

        public boolean hasPostReference() {
            for (Table table : allTables) {
                if (table.isWildCardSelect()) {
                    return true;
                }
            }

            for (Map.Entry<ColumnRef, ColumnRefType> e : columnRefs.entrySet()) {
                if (e.getValue() == ColumnRefType.GENERAL &&
                    allTableRefs.contains(e.getKey().getTableRef())) {
                    return true;
                }
            }

            return false;
        }

        public boolean hasFilters() {
           if (!postFilters.isEmpty())
               return true;

           if (isPrefilterAccepted && leftTable.hasFilters())
               return true;

           for (JoinSpec joinSpec : prefilterAcceptedTables) {
               if (joinSpec.getRhsJoinTable().hasFilters())
                   return true;
           }

           return false;
        }
    }

    public class JoinSpec {
        private final JoinType type;
        private final List<EqualParseNode> onConditions;
        private final JoinTable rhsJoinTable;
        private final boolean singleValueOnly;
        private Set<TableRef> dependentTableRefs;
        private OnNodeVisitor onNodeVisitor;

        private JoinSpec(JoinType type, ParseNode onNode, JoinTable joinTable,
                boolean singleValueOnly, ColumnResolver resolver) throws SQLException {
            this.type = type;
            this.onConditions = new ArrayList<EqualParseNode>();
            this.rhsJoinTable = joinTable;
            this.singleValueOnly = singleValueOnly;
            this.dependentTableRefs = new HashSet<TableRef>();
            this.onNodeVisitor = new OnNodeVisitor(resolver, this, phoenixStatement.getConnection());
            if (onNode != null) {
                this.pushDownOnCondition(onNode);
            }
        }

        /**
         * <pre>
         * 1.in {@link JoinSpec} ctor,try to push the filter in join on clause to where clause,
         *   eg. for "a join b on a.id = b.id and b.code = 1 where a.name is not null", try to
         *   push "b.code =1" in join on clause to where clause.
         * 2.in{@link WhereNodeVisitor#visitLeave(ComparisonParseNode, List)}, for inner join,
         *   try to push the join on condition in where clause to join on clause，
         *   eg. for "a join b on a.id = b.id where a.name = b.name", try to push "a.name=b.name"
         *   in where clause to join on clause.
         * </pre>
         * @param node
         * @throws SQLException
         */
        public void pushDownOnCondition(ParseNode node) throws SQLException {
            node.accept(onNodeVisitor);
        }

        public JoinType getType() {
            return type;
        }

        public List<EqualParseNode> getOnConditions() {
            return onConditions;
        }

        public JoinTable getRhsJoinTable() {
            return rhsJoinTable;
        }

        public List<TableRef>  getRhsJoinTableRefs() {
            return this.rhsJoinTable.getAllTableRefs();
        }

        public void pushDownFilterToRhsJoinTable(ParseNode parseNode) throws SQLException {
             this.rhsJoinTable.pushDownFilter(parseNode);
        }

        public void addOnCondition(EqualParseNode equalParseNode) {
            this.onConditions.add(equalParseNode);
        }

        public void addDependentTableRefs(Collection<TableRef> tableRefs) {
            this.dependentTableRefs.addAll(tableRefs);
        }

        public boolean isSingleValueOnly() {
            return singleValueOnly;
        }

        public Set<TableRef> getDependentTableRefs() {
            return dependentTableRefs;
        }

        public Pair<List<Expression>, List<Expression>> compileJoinConditions(StatementContext lhsCtx, StatementContext rhsCtx, Strategy strategy) throws SQLException {
            if (onConditions.isEmpty()) {
                return new Pair<List<Expression>, List<Expression>>(
                        Collections.<Expression> singletonList(LiteralExpression.newConstant(1)),
                        Collections.<Expression> singletonList(LiteralExpression.newConstant(1)));
            }

            List<Pair<Expression, Expression>> compiled = Lists.<Pair<Expression, Expression>> newArrayListWithExpectedSize(onConditions.size());
            ExpressionCompiler lhsCompiler = new ExpressionCompiler(lhsCtx);
            ExpressionCompiler rhsCompiler = new ExpressionCompiler(rhsCtx);
            for (EqualParseNode condition : onConditions) {
                lhsCompiler.reset();
                Expression left = condition.getLHS().accept(lhsCompiler);
                rhsCompiler.reset();
                Expression right = condition.getRHS().accept(rhsCompiler);
                PDataType toType = getCommonType(left.getDataType(), right.getDataType());
                SortOrder toSortOrder = strategy == Strategy.SORT_MERGE ? SortOrder.ASC : (strategy == Strategy.HASH_BUILD_LEFT ? right.getSortOrder() : left.getSortOrder());
                if (left.getDataType() != toType || left.getSortOrder() != toSortOrder) {
                    left = CoerceExpression.create(left, toType, toSortOrder, left.getMaxLength());
                }
                if (right.getDataType() != toType || right.getSortOrder() != toSortOrder) {
                    right = CoerceExpression.create(right, toType, toSortOrder, right.getMaxLength());
                }
                compiled.add(new Pair<Expression, Expression>(left, right));
            }
            // TODO PHOENIX-4618:
            // For Stategy.SORT_MERGE, we probably need to re-order the join keys based on the
            // specific ordering required by the join's parent, or re-order the following way
            // to align with group-by expressions' re-ordering.
            if (strategy != Strategy.SORT_MERGE) {
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
            }
            List<Expression> lConditions = Lists.<Expression> newArrayListWithExpectedSize(compiled.size());
            List<Expression> rConditions = Lists.<Expression> newArrayListWithExpectedSize(compiled.size());
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

            if (lType.isCoercibleTo(PTinyint.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PTinyint.INSTANCE))) {
              return lType; // to preserve UNSIGNED type
            }
            if (lType.isCoercibleTo(PSmallint.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PSmallint.INSTANCE))) {
              return lType; // to preserve UNSIGNED type
            }
            if (lType.isCoercibleTo(PInteger.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PInteger.INSTANCE))) {
              return lType; // to preserve UNSIGNED type
            }
            if (lType.isCoercibleTo(PLong.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PLong.INSTANCE))) {
              return lType; // to preserve UNSIGNED type
            }
            if (lType.isCoercibleTo(PDouble.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PDouble.INSTANCE))) {
              return lType; // to preserve UNSIGNED type
            }
            if (lType.isCoercibleTo(PDecimal.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PDecimal.INSTANCE))) {
              return PDecimal.INSTANCE;
            }
            if (lType.isCoercibleTo(PDate.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PDate.INSTANCE))) {
              return lType;
            }
            if (lType.isCoercibleTo(PTimestamp.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PTimestamp.INSTANCE))) {
              return lType;
            }
            if (lType.isCoercibleTo(PVarchar.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PVarchar.INSTANCE))) {
              return PVarchar.INSTANCE;
            }
            if (lType.isCoercibleTo(PBoolean.INSTANCE)
                && (rType == null || rType.isCoercibleTo(PBoolean.INSTANCE))) {
              return PBoolean.INSTANCE;
            }
            return PVarbinary.INSTANCE;
        }
    }

    public class Table {
        private TableNode tableNode;
        private final boolean isWildcard;
        private final List<ColumnDef> dynamicColumns;
        private final Double tableSamplingRate;
        private SelectStatement subselectStatement;
        private TableRef tableRef;
        /**
         * Which could as this {@link Table}'s where conditions.
         * Note: for {@link #isSubselect()}, added preFilterParseNode
         * is at first rewritten by
         * {@link SubselectRewriter#rewritePreFilterForSubselect}.
         */
        private final List<ParseNode> preFilterParseNodes;
        /**
         * Only make sense for {@link #isSubselect()}.
         * {@link #postFilterParseNodes} could not as this
         * {@link Table}'s where conditions, but need to filter after
         * {@link #getSelectStatementByApplyPreFiltersForSubselect()}
         * is executed.
         */
        private final List<ParseNode> postFilterParseNodes;
        /**
         * Determined by {@link SubselectRewriter#isFilterCanPushDownToSelect}.
         * Only make sense for {@link #isSubselect()},
         */
        private final boolean filterCanPushDownToSubselect;

        private Table(TableNode tableNode, boolean isWildcard, List<ColumnDef> dynamicColumns,
                      Double tableSamplingRate, TableRef tableRef) {
            this.tableNode = tableNode;
            this.isWildcard = isWildcard;
            this.dynamicColumns = dynamicColumns;
            this.tableSamplingRate=tableSamplingRate;
            this.subselectStatement = null;
            this.tableRef = tableRef;
            this.preFilterParseNodes = new ArrayList<ParseNode>();
            this.postFilterParseNodes = Collections.<ParseNode>emptyList();
            this.filterCanPushDownToSubselect = false;
        }

        private Table(DerivedTableNode tableNode, boolean isWildcard, TableRef tableRef) throws SQLException {
            this.tableNode = tableNode;
            this.isWildcard = isWildcard;
            this.dynamicColumns = Collections.<ColumnDef>emptyList();
            this.tableSamplingRate=ConcreteTableNode.DEFAULT_TABLE_SAMPLING_RATE;
            this.subselectStatement = SubselectRewriter.flatten(tableNode.getSelect(), phoenixStatement.getConnection());
            this.tableRef = tableRef;
            this.preFilterParseNodes = new ArrayList<ParseNode>();
            this.postFilterParseNodes = new ArrayList<ParseNode>();
            this.filterCanPushDownToSubselect = SubselectRewriter.isFilterCanPushDownToSelect(subselectStatement);
        }

        public TableNode getTableNode() {
            return tableNode;
        }

        public List<ColumnDef> getDynamicColumns() {
            return dynamicColumns;
        }
        
        public Double getTableSamplingRate() {
            return tableSamplingRate;
        }

        public boolean isSubselect() {
            return subselectStatement != null;
        }

        public SelectStatement getSubselectStatement() {
            return this.subselectStatement;
        }

        /**
         * Pruning columns if {@link #isSubselect()}.
         * Note: If some columns are pruned, the {@link JoinCompiler#origResolver} should be refreshed.
         * @throws SQLException
         */
        public void pruneSubselectAliasedNodes() throws SQLException {
            if(!this.isSubselect()) {
                return;
            }
            Set<String> referencedColumnNames = this.getReferencedColumnNames();
            SelectStatement newSubselectStatement =
                    SubselectRewriter.pruneSelectAliasedNodes(
                            this.subselectStatement,
                            referencedColumnNames,
                            phoenixStatement.getConnection());
            if(!newSubselectStatement.getSelect().equals(this.subselectStatement.getSelect())) {
                /**
                 * The columns are pruned, so {@link ColumnResolver} should be refreshed.
                 */
                DerivedTableNode newDerivedTableNode =
                        NODE_FACTORY.derivedTable(this.tableNode.getAlias(), newSubselectStatement);
                TableRef newTableRef =
                        FromCompiler.refreshDerivedTableNode(origResolver, newDerivedTableNode);
                assert newTableRef != null;
                this.subselectStatement = newSubselectStatement;
                this.tableRef = newTableRef;
                this.tableNode = newDerivedTableNode;
            }
        }

        /**
         * Collect the referenced columns of this {@link Table}
         * according to {@link JoinCompiler#columnNodes}.
         * @return
         * @throws SQLException
         */
        private Set<String> getReferencedColumnNames() throws SQLException {
            assert(this.isSubselect());
            if (isWildCardSelect()) {
                return null;
            }
            Set<String> referencedColumnNames = new HashSet<String>();
            for (Map.Entry<ColumnRef, ColumnParseNode> entry : columnNodes.entrySet()) {
                if (tableRef.equals(entry.getKey().getTableRef())) {
                    ColumnParseNode columnParseNode = entry.getValue();
                    String normalizedColumnName = SchemaUtil.getNormalizedColumnName(columnParseNode);
                    referencedColumnNames.add(normalizedColumnName);
                }
            }
            return referencedColumnNames;
        }

        /**
         * Returns all the basic select nodes, no aggregation.
         */
        public List<AliasedNode> getSelectAliasedNodes() {
            if (isWildCardSelect()) {
                return Collections.singletonList(NODE_FACTORY.aliasedNode(null, NODE_FACTORY.wildcard()));
            }

            List<AliasedNode> ret = new ArrayList<AliasedNode>();
            for (Map.Entry<ColumnRef, ColumnParseNode> entry : columnNodes.entrySet()) {
                if (tableRef.equals(entry.getKey().getTableRef())) {
                    ret.add(NODE_FACTORY.aliasedNode(null, entry.getValue()));
                }
            }
            if (ret.isEmpty()) {
                ret.add(NODE_FACTORY.aliasedNode(null, NODE_FACTORY.literal(1)));
            }
            return ret;
        }

        public List<ParseNode> getPreFilterParseNodes() {
            return preFilterParseNodes;
        }

        public List<ParseNode> getPostFilterParseNodes() {
            return postFilterParseNodes;
        }

        public TableRef getTableRef() {
            return tableRef;
        }

        public void addFilter(ParseNode filter) throws SQLException {
            if (!isSubselect() || filterCanPushDownToSubselect) {
                this.addPreFilter(filter);
            } else {
                postFilterParseNodes.add(filter);
            }
        }

        /**
         * If {@link #isSubselect()}, preFilterParseNode is at first rewritten by
         * {@link SubselectRewriter#rewritePreFilterForSubselect}
         * @param preFilterParseNode
         * @throws SQLException
         */
        private void addPreFilter(ParseNode preFilterParseNode) throws SQLException {
            if(this.isSubselect()) {
                preFilterParseNode =
                        SubselectRewriter.rewritePreFilterForSubselect(
                                preFilterParseNode,
                                this.subselectStatement,
                                tableNode.getAlias());
            }
            preFilterParseNodes.add(preFilterParseNode);
        }

        public ParseNode getCombinedPreFilterParseNodes() {
            return combine(preFilterParseNodes);
        }

        /**
         * Get this {@link Table}'s new {@link SelectStatement} by applying {@link #preFilterParseNodes},
         * {@link #postFilterParseNodes} and additional newOrderByNodes.
         * @param newOrderByNodes
         * @return
         * @throws SQLException
         */
        public SelectStatement getAsSubquery(List<OrderByNode> newOrderByNodes) throws SQLException {
            if (isSubselect()) {
                return SubselectRewriter.applyOrderByAndPostFilters(
                        this.getSelectStatementByApplyPreFiltersForSubselect(),
                        newOrderByNodes,
                        tableNode.getAlias(),
                        postFilterParseNodes);
            }

            /**
             * For flat table, {@link #postFilterParseNodes} is empty , because it can safely pushed down as
             * {@link #preFilterParseNodes}.
             */
            assert postFilterParseNodes == null || postFilterParseNodes.isEmpty();
            return NODE_FACTORY.select(
                    tableNode,
                    originalJoinSelectStatement.getHint(),
                    false,
                    getSelectAliasedNodes(),
                    getCombinedPreFilterParseNodes(),
                    null,
                    null,
                    newOrderByNodes,
                    null,
                    null,
                    0,
                    false,
                    originalJoinSelectStatement.hasSequence(),
                    Collections.<SelectStatement> emptyList(),
                    originalJoinSelectStatement.getUdfParseNodes());
        }

        public SelectStatement getAsSubqueryForOptimization(boolean applyGroupByOrOrderBy) throws SQLException {
            assert (!isSubselect());

            SelectStatement query = getAsSubquery(null);
            if (!applyGroupByOrOrderBy)
                return query;

            boolean addGroupBy = false;
            boolean addOrderBy = false;
            if (originalJoinSelectStatement.getGroupBy() != null && !originalJoinSelectStatement.getGroupBy().isEmpty()) {
                ColumnRefParseNodeVisitor groupByVisitor = new ColumnRefParseNodeVisitor(origResolver, phoenixStatement.getConnection());
                for (ParseNode node : originalJoinSelectStatement.getGroupBy()) {
                    node.accept(groupByVisitor);
                }
                Set<TableRef> set = groupByVisitor.getTableRefSet();
                if (set.size() == 1 && tableRef.equals(set.iterator().next())) {
                    addGroupBy = true;
                }
            } else if (originalJoinSelectStatement.getOrderBy() != null && !originalJoinSelectStatement.getOrderBy().isEmpty()) {
                ColumnRefParseNodeVisitor orderByVisitor = new ColumnRefParseNodeVisitor(origResolver, phoenixStatement.getConnection());
                for (OrderByNode node : originalJoinSelectStatement.getOrderBy()) {
                    node.getNode().accept(orderByVisitor);
                }
                Set<TableRef> set = orderByVisitor.getTableRefSet();
                if (set.size() == 1 && tableRef.equals(set.iterator().next())) {
                    addOrderBy = true;
                }
            }

            if (!addGroupBy && !addOrderBy)
                return query;

            List<AliasedNode> selectList = query.getSelect();
            if (addGroupBy) {
                assert (!isWildCardSelect());
                selectList = new ArrayList<AliasedNode>(query.getSelect().size());
                for (AliasedNode aliasedNode : query.getSelect()) {
                    ParseNode node = NODE_FACTORY.function(
                            MinAggregateFunction.NAME, Collections.singletonList(aliasedNode.getNode()));
                    selectList.add(NODE_FACTORY.aliasedNode(null, node));
                }
            }

            return NODE_FACTORY.select(query.getFrom(), query.getHint(), query.isDistinct(), selectList,
                    query.getWhere(), addGroupBy ? originalJoinSelectStatement.getGroupBy() : query.getGroupBy(),
                    addGroupBy ? null : query.getHaving(), addOrderBy ? originalJoinSelectStatement.getOrderBy() : query.getOrderBy(),
                    query.getLimit(), query.getOffset(), query.getBindCount(), addGroupBy, query.hasSequence(),
                    query.getSelects(), query.getUdfParseNodes());
        }

        public boolean hasFilters() {
            return isSubselect() ?
                   (!postFilterParseNodes.isEmpty() || subselectStatement.getWhere() != null || subselectStatement.getHaving() != null) :
                    !preFilterParseNodes.isEmpty();
        }

        /**
         * Check if this {@link Table} could be pushed to RegionServer
         * {@link HashJoinRegionScanner} as the probe side of Hash join.
         * @return
         * @throws SQLException
         */
        public boolean isCouldPushToServerAsHashJoinProbeSide() throws SQLException {
            /**
             * If {@link #postFilterParseNodes} is not empty, obviously this {@link Table}
             * should execute {@link #postFilterParseNodes} before join.
             */
            if(this.postFilterParseNodes != null && !this.postFilterParseNodes.isEmpty()) {
                return false;
            }

            SelectStatement selectStatementToUse = this.getAsSubquery(null);
            RewriteResult rewriteResult =
                    ParseNodeUtil.rewrite(selectStatementToUse, phoenixStatement.getConnection());
            return JoinCompiler.isCouldPushToServerAsHashJoinProbeSide(
                    rewriteResult.getRewrittenSelectStatement());
        }

        /**
         * Get this {@link Table}'s new {@link SelectStatement} only applying
         * {@link #preFilterParseNodes} for {@link #isSubselect()}.
         * @return
         */
        private SelectStatement getSelectStatementByApplyPreFiltersForSubselect() {
            return SubselectRewriter.applyPreFiltersForSubselect(
                    subselectStatement,
                    preFilterParseNodes,
                    tableNode.getAlias());

        }

        protected boolean isWildCardSelect() {
            return isWildcard;
        }

        public void projectColumns(Scan scan) {
            assert(!isSubselect());
            if (isWildCardSelect()) {
                scan.getFamilyMap().clear();
                return;
            }
            for (ColumnRef columnRef : columnRefs.keySet()) {
                if (columnRef.getTableRef().equals(tableRef)
                        && !SchemaUtil.isPKColumn(columnRef.getColumn())
                        && !(columnRef instanceof LocalIndexColumnRef)) {
                	EncodedColumnsUtil.setColumns(columnRef.getColumn(), tableRef.getTable(), scan);
                }
            }
        }

        public PTable createProjectedTable(boolean retainPKColumns, StatementContext context) throws SQLException {
            assert(!isSubselect());
            List<ColumnRef> sourceColumns = new ArrayList<ColumnRef>();
            PTable table = tableRef.getTable();
            if (retainPKColumns) {
                for (PColumn column : table.getPKColumns()) {
                    sourceColumns.add(new ColumnRef(tableRef, column.getPosition()));
                }
            }
            if (isWildCardSelect()) {
                for (PColumn column : table.getColumns()) {
                    if (!retainPKColumns || !SchemaUtil.isPKColumn(column)) {
                        sourceColumns.add(new ColumnRef(tableRef, column.getPosition()));
                    }
                }
            } else {
                for (Map.Entry<ColumnRef, ColumnRefType> e : columnRefs.entrySet()) {
                    ColumnRef columnRef = e.getKey();
                    if (columnRef.getTableRef().equals(tableRef)
                            && (!retainPKColumns || !SchemaUtil.isPKColumn(columnRef.getColumn()))) {
                        if (columnRef instanceof LocalIndexColumnRef) {
                            sourceColumns.add(new IndexUncoveredDataColumnRef(context, tableRef,
                                    IndexUtil.getIndexColumnName(columnRef.getColumn())));
                        } else {
                            sourceColumns.add(columnRef);
                        }
                    }
                }
            }

            return TupleProjectionCompiler.createProjectedTable(tableRef, sourceColumns, retainPKColumns);
        }

        public PTable createProjectedTable(RowProjector rowProjector) throws SQLException {
            assert(isSubselect());
            TableRef tableRef = FromCompiler.getResolverForCompiledDerivedTable(phoenixStatement.getConnection(), this.tableRef, rowProjector).getTables().get(0);
            List<ColumnRef> sourceColumns = new ArrayList<ColumnRef>();
            PTable table = tableRef.getTable();
            for (PColumn column : table.getColumns()) {
                sourceColumns.add(new ColumnRef(tableRef, column.getPosition()));
            }
            return TupleProjectionCompiler.createProjectedTable(tableRef, sourceColumns, false);
        }
    }

    /**
     * Push down {@link JoinTable#postFilters} of Outermost-JoinTable to
     * {@link JoinTable#postFilters} of Sub-JoinTable
     */
    private static class PushDownPostFilterParseNodeVisitor extends AndRewriterBooleanParseNodeVisitor {
        private ColumnRefParseNodeVisitor columnRefParseNodeVisitor;
        /**
         * Sub-JoinTable to accept pushed down PostFilters.
         */
        private JoinTable joinTable;

        public PushDownPostFilterParseNodeVisitor(
                ColumnResolver resolver,
                JoinTable joinTablesContext,
                PhoenixConnection connection) {
            super(NODE_FACTORY);
            this.joinTable = joinTablesContext;
            this.columnRefParseNodeVisitor = new ColumnRefParseNodeVisitor(resolver, connection);
        }

        @Override
        protected ParseNode leaveBooleanNode(
                ParseNode parentParseNode, List<ParseNode> childParseNodes) throws SQLException {
            columnRefParseNodeVisitor.reset();
            parentParseNode.accept(columnRefParseNodeVisitor);
            ColumnRefParseNodeVisitor.ColumnRefType columnRefType =
                    columnRefParseNodeVisitor.getContentType(
                            this.joinTable.getAllTableRefs());
            if(columnRefType == ColumnRefParseNodeVisitor.ColumnRefType.NONE ||
               columnRefType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY){
                this.joinTable.postFilters.add(parentParseNode);
                return null;
            }
            return parentParseNode;
        }
    }

    private static class WhereNodeVisitor extends AndBooleanParseNodeVisitor<Void> {
        private ColumnRefParseNodeVisitor columnRefVisitor;
        private JoinTable joinTable;

        public WhereNodeVisitor(
                ColumnResolver resolver,
                JoinTable joinTablesContext,
                PhoenixConnection connection) {
            this.joinTable = joinTablesContext;
            this.columnRefVisitor = new ColumnRefParseNodeVisitor(resolver, connection);
        }

        @Override
        protected Void leaveBooleanNode(ParseNode node,
                List<Void> l) throws SQLException {
            columnRefVisitor.reset();
            node.accept(columnRefVisitor);
            ColumnRefParseNodeVisitor.ColumnRefType type =
                    columnRefVisitor.getContentType(this.joinTable.getLeftTableRef());
            switch (type) {
            case NONE:
            case SELF_ONLY:
                this.joinTable.addLeftTableFilter(node);
                break;
            case FOREIGN_ONLY:
                JoinTable matched = null;
                for (JoinSpec joinSpec : this.joinTable.getPrefilterAcceptedJoinSpecs()) {
                    if (columnRefVisitor.getContentType(
                            joinSpec.getRhsJoinTable().getAllTableRefs()) ==
                        ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                        matched = joinSpec.getRhsJoinTable();
                        break;
                    }
                }
                if (matched != null) {
                    matched.pushDownFilter(node);
                } else {
                    this.joinTable.addPostJoinFilter(node);
                }
                break;
            default:
                this.joinTable.addPostJoinFilter(node);
                break;
            }
            return null;
        }

        @Override
        protected Void leaveNonBooleanNode(ParseNode node, List<Void> l) throws SQLException {
            return null;
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

            List<JoinSpec> prefilterAcceptedJoinSpecs =
                    this.joinTable.getPrefilterAcceptedJoinSpecs();
            ListIterator<JoinSpec> iter =
                    prefilterAcceptedJoinSpecs.listIterator(prefilterAcceptedJoinSpecs.size());
            while (iter.hasPrevious()) {
                JoinSpec joinSpec = iter.previous();
                if (joinSpec.getType() != JoinType.Inner || joinSpec.isSingleValueOnly()) {
                    continue;
                }

                try {
                    joinSpec.pushDownOnCondition(node);
                    return null;
                } catch (SQLException e) {
                }
            }

            return leaveBooleanNode(node, l);
        }
    }

    private static class OnNodeVisitor extends AndBooleanParseNodeVisitor<Void> {
        private final ColumnRefParseNodeVisitor columnRefVisitor;
        private final JoinSpec joinSpec;

        public OnNodeVisitor(
                ColumnResolver resolver, JoinSpec joinSpec, PhoenixConnection connection) {
            this.joinSpec = joinSpec;
            this.columnRefVisitor = new ColumnRefParseNodeVisitor(resolver, connection);
        }

        @Override
        protected Void leaveBooleanNode(ParseNode node,
                List<Void> l) throws SQLException {
            columnRefVisitor.reset();
            node.accept(columnRefVisitor);
            ColumnRefParseNodeVisitor.ColumnRefType type =
                    columnRefVisitor.getContentType(this.joinSpec.getRhsJoinTableRefs());
            if (type == ColumnRefParseNodeVisitor.ColumnRefType.NONE
                    || type == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                this.joinSpec.pushDownFilterToRhsJoinTable(node);
            } else {
                throwAmbiguousJoinConditionException();
            }
            return null;
        }

        @Override
        protected Void leaveNonBooleanNode(ParseNode node, List<Void> l) throws SQLException {
            return null;
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
            ColumnRefParseNodeVisitor.ColumnRefType lhsType =
                    columnRefVisitor.getContentType(this.joinSpec.getRhsJoinTableRefs());
            Set<TableRef> lhsTableRefSet = Sets.newHashSet(columnRefVisitor.getTableRefSet());
            columnRefVisitor.reset();
            node.getRHS().accept(columnRefVisitor);
            ColumnRefParseNodeVisitor.ColumnRefType rhsType =
                    columnRefVisitor.getContentType(this.joinSpec.getRhsJoinTableRefs());
            Set<TableRef> rhsTableRefSet = Sets.newHashSet(columnRefVisitor.getTableRefSet());
            if ((lhsType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY || lhsType == ColumnRefParseNodeVisitor.ColumnRefType.NONE)
                    && (rhsType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY || rhsType == ColumnRefParseNodeVisitor.ColumnRefType.NONE)) {
                this.joinSpec.pushDownFilterToRhsJoinTable(node);
            } else if (lhsType == ColumnRefParseNodeVisitor.ColumnRefType.FOREIGN_ONLY
                    && rhsType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                this.joinSpec.addOnCondition((EqualParseNode) node);
                this.joinSpec.addDependentTableRefs(lhsTableRefSet);
            } else if (rhsType == ColumnRefParseNodeVisitor.ColumnRefType.FOREIGN_ONLY
                    && lhsType == ColumnRefParseNodeVisitor.ColumnRefType.SELF_ONLY) {
                this.joinSpec.addOnCondition(NODE_FACTORY.equal(node.getRHS(), node.getLHS()));
                this.joinSpec.addDependentTableRefs(rhsTableRefSet);
            } else {
                throwAmbiguousJoinConditionException();
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
        public void throwAmbiguousJoinConditionException() throws SQLException {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.AMBIGUOUS_JOIN_CONDITION).build().buildException();
        }
    }

    private static class LocalIndexColumnRef extends ColumnRef {
        private final TableRef indexTableRef;

        public LocalIndexColumnRef(TableRef tableRef, String familyName,
                String columnName, TableRef indexTableRef) throws MetaDataEntityNotFoundException {
            super(tableRef, familyName, columnName);
            this.indexTableRef = indexTableRef;
        }

        @Override
        public TableRef getTableRef() {
            return indexTableRef;
        }
    }

    private static class ColumnRefParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
        public enum ColumnRefType {NONE, SELF_ONLY, FOREIGN_ONLY, COMPLEX};

        private final ColumnResolver resolver;
        private final PhoenixConnection connection;
        private final Set<TableRef> tableRefSet;
        private final Map<ColumnRef, ColumnParseNode> columnRefMap;

        public ColumnRefParseNodeVisitor(ColumnResolver resolver, PhoenixConnection connection) {
            this.resolver = resolver;
            this.tableRefSet = new HashSet<TableRef>();
            this.columnRefMap = new HashMap<ColumnRef, ColumnParseNode>();
            this.connection = connection;
        }

        public void reset() {
            this.tableRefSet.clear();
            this.columnRefMap.clear();
        }

        @Override
        public Void visit(ColumnParseNode node) throws SQLException {
            ColumnRef columnRef = null;
            try {
                columnRef = resolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
            } catch (ColumnNotFoundException e) {
                // This could be an IndexUncoveredDataColumnRef. If so, the table name must have
                // been appended by the IndexStatementRewriter, and we can convert it into.
                TableRef tableRef = resolver.resolveTable(node.getSchemaName(), node.getTableName());
                if (IndexUtil.shouldIndexBeUsedForUncoveredQuery(tableRef)) {
                    TableRef parentTableRef = FromCompiler.getResolver(
                            NODE_FACTORY.namedTable(null, TableName.create(tableRef.getTable()
                                    .getSchemaName().getString(), tableRef.getTable()
                                    .getParentTableName().getString())), connection).resolveTable(
                            tableRef.getTable().getSchemaName().getString(),
                            tableRef.getTable().getParentTableName().getString());
                    columnRef = new LocalIndexColumnRef(parentTableRef,
                            IndexUtil.getDataColumnFamilyName(node.getName()),
                            IndexUtil.getDataColumnName(node.getName()), tableRef);
                } else {
                    throw e;
                }
            }
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

    // for creation of new statements
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    /**
     * Check if this {@link Table} could be pushed to RegionServer
     * {@link HashJoinRegionScanner} as the probe side of Hash join.
     * Note: the {@link SelectStatement} parameter must be rewritten by
     * {@link ParseNodeUtil#rewrite} before this method.
     * {@link SelectStatement} parameter could has NonCorrelated subquery,
     * but for Correlated subquery, {@link ParseNodeUtil#rewrite} rewrite
     * it as join.
     * Note: {@link SelectStatement} could also have {@link OrderBy},but we
     * could ignore the {@link OrderBy} because we do not guarantee the {@link OrderBy}
     * after join.
     * @param selectStatement
     * @return
     */
    private static boolean isCouldPushToServerAsHashJoinProbeSide(SelectStatement selectStatement) {
        return !selectStatement.isJoin()
                && !selectStatement.isAggregate()
                && !selectStatement.isDistinct()
                && !(selectStatement.getFrom() instanceof DerivedTableNode)
                && selectStatement.getLimit() == null
                && selectStatement.getOffset() == null;
    }

    private static ParseNode combine(List<ParseNode> nodes) {
        if (nodes.isEmpty())
            return null;

        if (nodes.size() == 1)
            return nodes.get(0);

        return NODE_FACTORY.and(nodes);
    }

    private boolean isWildCardSelectForTable(List<AliasedNode> select, TableRef tableRef, ColumnResolver resolver) throws SQLException {
        for (AliasedNode aliasedNode : select) {
            ParseNode node = aliasedNode.getNode();
            if (node instanceof TableWildcardParseNode) {
                TableName tableName = ((TableWildcardParseNode) node).getTableName();
                if (tableRef.equals(resolver.resolveTable(tableName.getSchemaName(), tableName.getTableName()))) {
                    return true;
                }

            }
        }
        return false;
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

    public static PTable joinProjectedTables(PTable left, PTable right, JoinType type) throws SQLException {
        Preconditions.checkArgument(left.getType() == PTableType.PROJECTED);
        Preconditions.checkArgument(right.getType() == PTableType.PROJECTED);
        List<PColumn> merged = Lists.<PColumn> newArrayList();
        int startingPosition = left.getBucketNum() == null ? 0 : 1;
        if (type == JoinType.Full) {
            for (int i = startingPosition; i < left.getColumns().size(); i++) {
                PColumn c  = left.getColumns().get(i);
                merged.add(new ProjectedColumn(c.getName(), c.getFamilyName(),
                        c.getPosition(), true, ((ProjectedColumn) c).getSourceColumnRef(), SchemaUtil.isPKColumn(c) ? null : c.getName().getBytes()));
            }
        } else {
            merged.addAll(left.getColumns());
            if (left.getBucketNum() != null) {
                merged.remove(0);
            }
        }
        int position = merged.size() + startingPosition;
        for (PColumn c : right.getColumns()) {
            if (!SchemaUtil.isPKColumn(c)) {
                PColumn column = new ProjectedColumn(c.getName(), c.getFamilyName(), 
                        position++, type == JoinType.Inner ? c.isNullable() : true, 
                        ((ProjectedColumn) c).getSourceColumnRef(), c.getName().getBytes());
                merged.add(column);
            }
        }
        return new PTableImpl.Builder()
                .setType(left.getType())
                .setState(left.getIndexState())
                .setTimeStamp(left.getTimeStamp())
                .setIndexDisableTimestamp(left.getIndexDisableTimestamp())
                .setSequenceNumber(left.getSequenceNumber())
                .setImmutableRows(left.isImmutableRows())
                .setDisableWAL(PTable.DEFAULT_DISABLE_WAL)
                .setMultiTenant(left.isMultiTenant())
                .setStoreNulls(left.getStoreNulls())
                .setViewType(left.getViewType())
                .setViewIndexIdType(left.getviewIndexIdType())
                .setViewIndexId(left.getViewIndexId())
                .setIndexType(left.getIndexType())
                .setTransactionProvider(left.getTransactionProvider())
                .setUpdateCacheFrequency(left.getUpdateCacheFrequency())
                .setNamespaceMapped(left.isNamespaceMapped())
                .setAutoPartitionSeqName(left.getAutoPartitionSeqName())
                .setAppendOnlySchema(left.isAppendOnlySchema())
                .setImmutableStorageScheme(ONE_CELL_PER_COLUMN)
                .setQualifierEncodingScheme(NON_ENCODED_QUALIFIERS)
                .setBaseColumnCount(BASE_TABLE_BASE_COLUMN_COUNT)
                .setEncodedCQCounter(PTable.EncodedCQCounter.NULL_COUNTER)
                .setUseStatsForParallelization(left.useStatsForParallelization())
                .setExcludedColumns(ImmutableList.of())
                .setTenantId(left.getTenantId())
                .setSchemaName(left.getSchemaName())
                .setTableName(PNameFactory.newName(SchemaUtil.getTableName(left.getName().getString(),
                        right.getName().getString())))
                .setPkName(left.getPKName())
                .setRowKeyOrderOptimizable(left.rowKeyOrderOptimizable())
                .setBucketNum(left.getBucketNum())
                .setIndexes(left.getIndexes() == null ? Collections.emptyList() : left.getIndexes())
                .setParentSchemaName(left.getParentSchemaName())
                .setParentTableName(left.getParentTableName())
                .setPhysicalNames(ImmutableList.<PName>of())
                .setColumns(merged)
                .build();
    }

}
