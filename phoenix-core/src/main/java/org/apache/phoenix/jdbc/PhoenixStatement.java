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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SELECT_SQL_COUNTER;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.call.CallRunner;
import org.apache.phoenix.compile.BaseMutationPlan;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.CreateFunctionCompiler;
import org.apache.phoenix.compile.CreateIndexCompiler;
import org.apache.phoenix.compile.CreateSchemaCompiler;
import org.apache.phoenix.compile.CreateSequenceCompiler;
import org.apache.phoenix.compile.CreateTableCompiler;
import org.apache.phoenix.compile.DeleteCompiler;
import org.apache.phoenix.compile.DropSequenceCompiler;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.ListJarsQueryPlan;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementNormalizer;
import org.apache.phoenix.compile.StatementPlan;
import org.apache.phoenix.compile.SubqueryRewriter;
import org.apache.phoenix.compile.SubselectRewriter;
import org.apache.phoenix.compile.TraceQueryPlan;
import org.apache.phoenix.compile.UpsertCompiler;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.BatchUpdateExecution;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.exception.UpgradeRequiredException;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.iterate.MaterializedResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.parse.AddColumnStatement;
import org.apache.phoenix.parse.AddJarsStatement;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.AlterSessionStatement;
import org.apache.phoenix.parse.BindableStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateFunctionStatement;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateSchemaStatement;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DeleteJarStatement;
import org.apache.phoenix.parse.DeleteStatement;
import org.apache.phoenix.parse.DropColumnStatement;
import org.apache.phoenix.parse.DropFunctionStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropSchemaStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.ExecuteUpgradeStatement;
import org.apache.phoenix.parse.ExplainStatement;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.IndexKeyConstraint;
import org.apache.phoenix.parse.LimitNode;
import org.apache.phoenix.parse.ListJarsStatement;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.NamedNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.OffsetNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TraceStatement;
import org.apache.phoenix.parse.UDFParseNode;
import org.apache.phoenix.parse.UpdateStatisticsStatement;
import org.apache.phoenix.parse.UpsertStatement;
import org.apache.phoenix.parse.UseSchemaStatement;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ExecuteQueryNotApplicableException;
import org.apache.phoenix.schema.ExecuteUpdateNotApplicableException;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.StatisticsCollectionScope;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixContextExecutor;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
/**
 * 
 * JDBC Statement implementation of Phoenix.
 * Currently only the following methods are supported:
 * - {@link #executeQuery(String)}
 * - {@link #executeUpdate(String)}
 * - {@link #execute(String)}
 * - {@link #getResultSet()}
 * - {@link #getUpdateCount()}
 * - {@link #close()}
 * The Statement only supports the following options:
 * - ResultSet.FETCH_FORWARD
 * - ResultSet.TYPE_FORWARD_ONLY
 * - ResultSet.CLOSE_CURSORS_AT_COMMIT
 * 
 * 
 * @since 0.1
 */
public class PhoenixStatement implements Statement, SQLCloseable {
	
    private static final Logger logger = LoggerFactory.getLogger(PhoenixStatement.class);
    
    public enum Operation {
        QUERY("queried", false),
        DELETE("deleted", true),
        UPSERT("upserted", true),
        UPGRADE("upgrade", true);
        
        private final String toString;
        private final boolean isMutation;
        Operation(String toString, boolean isMutation) {
            this.toString = toString;
            this.isMutation = isMutation;
        }
        
        public boolean isMutation() {
            return isMutation;
        }
        
        @Override
        public String toString() {
            return toString;
        }
    };

    protected final PhoenixConnection connection;
    private static final int NO_UPDATE = -1;
    private List<PhoenixResultSet> resultSets = new ArrayList<PhoenixResultSet>();
    private QueryPlan lastQueryPlan;
    private PhoenixResultSet lastResultSet;
    private int lastUpdateCount = NO_UPDATE;
    private Operation lastUpdateOperation;
    private boolean isClosed = false;
    private int maxRows;
    private int fetchSize = -1;
    private int queryTimeoutMillis;
    
    public PhoenixStatement(PhoenixConnection connection) {
        this.connection = connection;
        this.queryTimeoutMillis = getDefaultQueryTimeoutMillis();
    }

    /**
     * Internally to Phoenix we allow callers to set the query timeout in millis
     * via the phoenix.query.timeoutMs. Therefore we store the time in millis.
     */
    private int getDefaultQueryTimeoutMillis() {
        return connection.getQueryServices().getProps().getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, 
            QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
    }

    protected List<PhoenixResultSet> getResultSets() {
        return resultSets;
    }
    
    public PhoenixResultSet newResultSet(ResultIterator iterator, RowProjector projector, StatementContext context) throws SQLException {
        return new PhoenixResultSet(iterator, projector, context);
    }
    
    protected boolean execute(final CompilableStatement stmt) throws SQLException {
        if (stmt.getOperation().isMutation()) {
            executeMutation(stmt);
            return false;
        }
        executeQuery(stmt);
        return true;
    }
    
    protected QueryPlan optimizeQuery(CompilableStatement stmt) throws SQLException {
        QueryPlan plan = stmt.compilePlan(this, Sequence.ValueOp.VALIDATE_SEQUENCE);
        return connection.getQueryServices().getOptimizer().optimize(this, plan);
    }
    
    protected PhoenixResultSet executeQuery(final CompilableStatement stmt) throws SQLException {
        GLOBAL_SELECT_SQL_COUNTER.increment();
        try {
            return CallRunner.run(
                new CallRunner.CallableThrowable<PhoenixResultSet, SQLException>() {
                @Override
                    public PhoenixResultSet call() throws SQLException {
                    final long startTime = System.currentTimeMillis();
                    try {
                        PhoenixConnection conn = getConnection();
                        if (conn.getQueryServices().isUpgradeRequired() && !conn.isRunningUpgrade()
                                && stmt.getOperation() != Operation.UPGRADE) {
                            throw new UpgradeRequiredException();
                        }
                        QueryPlan plan = stmt.compilePlan(PhoenixStatement.this, Sequence.ValueOp.VALIDATE_SEQUENCE);
                        // Send mutations to hbase, so they are visible to subsequent reads.
                        // Use original plan for data table so that data and immutable indexes will be sent
                        // TODO: for joins, we need to iterate through all tables, but we need the original table,
                        // not the projected table, so plan.getContext().getResolver().getTables() won't work.
                        Iterator<TableRef> tableRefs = plan.getSourceRefs().iterator();
                        connection.getMutationState().sendUncommitted(tableRefs);
                        plan = connection.getQueryServices().getOptimizer().optimize(PhoenixStatement.this, plan);
                         // this will create its own trace internally, so we don't wrap this
                         // whole thing in tracing
                        ResultIterator resultIterator = plan.iterator();
                        if (logger.isDebugEnabled()) {
                            String explainPlan = QueryUtil.getExplainPlan(resultIterator);
                            logger.debug(LogUtil.addCustomAnnotations("Explain plan: " + explainPlan, connection));
                        }
                        StatementContext context = plan.getContext();
                        context.getOverallQueryMetrics().startQuery();
                        PhoenixResultSet rs = newResultSet(resultIterator, plan.getProjector(), context);
                        resultSets.add(rs);
                        setLastQueryPlan(plan);
                        setLastResultSet(rs);
                        setLastUpdateCount(NO_UPDATE);
                        setLastUpdateOperation(stmt.getOperation());
                        // If transactional, this will move the read pointer forward
                        if (connection.getAutoCommit()) {
                            connection.commit();
                        }
                        connection.incrementStatementExecutionCounter();
                        return rs;
                    } catch (RuntimeException e) {
                        // FIXME: Expression.evaluate does not throw SQLException
                        // so this will unwrap throws from that.
                        if (e.getCause() instanceof SQLException) {
                            throw (SQLException) e.getCause();
                        }
                        throw e;
                    } finally {
                        // Regardless of whether the query was successfully handled or not, 
                        // update the time spent so far. If needed, we can separate out the
                        // success times and failure times.
                        GLOBAL_QUERY_TIME.update(System.currentTimeMillis() - startTime);
                    }
                }
                }, PhoenixContextExecutor.inContext());
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, SQLException.class);
            Throwables.propagate(e);
            throw new IllegalStateException(); // Can't happen as Throwables.propagate() always throws
        }
    }
    
    protected int executeMutation(final CompilableStatement stmt) throws SQLException {
	 if (connection.isReadOnly()) {
            throw new SQLExceptionInfo.Builder(
                SQLExceptionCode.READ_ONLY_CONNECTION).
                build().buildException();
        }
	    GLOBAL_MUTATION_SQL_COUNTER.increment();
        try {
            return CallRunner
                    .run(
                        new CallRunner.CallableThrowable<Integer, SQLException>() {
                        @Override
                            public Integer call() throws SQLException {
                            try {
                                PhoenixConnection conn = getConnection();
                                if (conn.getQueryServices().isUpgradeRequired() && !conn.isRunningUpgrade()
                                        && stmt.getOperation() != Operation.UPGRADE) {
                                    throw new UpgradeRequiredException();
                                }
                                MutationState state = connection.getMutationState();
                                MutationPlan plan = stmt.compilePlan(PhoenixStatement.this, Sequence.ValueOp.VALIDATE_SEQUENCE);
                                if (plan.getTargetRef() != null && plan.getTargetRef().getTable() != null && plan.getTargetRef().getTable().isTransactional()) {
                                    state.startTransaction();
                                }
                                Iterator<TableRef> tableRefs = plan.getSourceRefs().iterator();
                                state.sendUncommitted(tableRefs);
                                state.checkpointIfNeccessary(plan);
                                MutationState lastState = plan.execute();
                                state.join(lastState);
                                if (connection.getAutoCommit()) {
                                    connection.commit();
                                }
                                setLastResultSet(null);
                                setLastQueryPlan(null);
                                // Unfortunately, JDBC uses an int for update count, so we
                                // just max out at Integer.MAX_VALUE
                                int lastUpdateCount = (int) Math.min(Integer.MAX_VALUE, lastState.getUpdateCount());
                                setLastUpdateCount(lastUpdateCount);
                                setLastUpdateOperation(stmt.getOperation());
                                connection.incrementStatementExecutionCounter();
                                return lastUpdateCount;
                            } catch (RuntimeException e) {
                                // FIXME: Expression.evaluate does not throw SQLException
                                // so this will unwrap throws from that.
                                if (e.getCause() instanceof SQLException) {
                                    throw (SQLException) e.getCause();
                                }
                                throw e;
                            }
                        }
                    }, PhoenixContextExecutor.inContext(),
                        Tracing.withTracing(connection, this.toString()));
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, SQLException.class);
            Throwables.propagate(e);
            throw new IllegalStateException(); // Can't happen as Throwables.propagate() always throws
        }
    }

    protected static interface CompilableStatement extends BindableStatement {
        public <T extends StatementPlan> T compilePlan (PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException;
    }
    
    private static class ExecutableSelectStatement extends SelectStatement implements CompilableStatement {
        private ExecutableSelectStatement(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
                List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit,OffsetNode offset, int bindCount, boolean isAggregate, boolean hasSequence, Map<String, UDFParseNode> udfParseNodes) {
            this(from, hint, isDistinct, select, where, groupBy, having, orderBy, limit,offset, bindCount, isAggregate, hasSequence, Collections.<SelectStatement>emptyList(), udfParseNodes);
        }

        private ExecutableSelectStatement(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
                List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit,OffsetNode offset, int bindCount, boolean isAggregate,
                boolean hasSequence, List<SelectStatement> selects, Map<String, UDFParseNode> udfParseNodes) {
            super(from, hint, isDistinct, select, where, groupBy, having, orderBy, limit, offset, bindCount, isAggregate, hasSequence, selects, udfParseNodes);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public QueryPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            if(!getUdfParseNodes().isEmpty()) {
                stmt.throwIfUnallowedUserDefinedFunctions(getUdfParseNodes());
            }
            SelectStatement select = SubselectRewriter.flatten(this, stmt.getConnection());
            ColumnResolver resolver = FromCompiler.getResolverForQuery(select, stmt.getConnection());
            select = StatementNormalizer.normalize(select, resolver);
            SelectStatement transformedSelect = SubqueryRewriter.transform(select, resolver, stmt.getConnection());
            if (transformedSelect != select) {
                resolver = FromCompiler.getResolverForQuery(transformedSelect, stmt.getConnection());
                select = StatementNormalizer.normalize(transformedSelect, resolver);
            }
            QueryPlan plan = new QueryCompiler(stmt, select, resolver, Collections.<PDatum>emptyList(), stmt.getConnection().getIteratorFactory(), new SequenceManager(stmt), true).compile();
            plan.getContext().getSequenceManager().validateSequences(seqAction);
            return plan;
        }

    }
    
    private static final byte[] EXPLAIN_PLAN_FAMILY = QueryConstants.SINGLE_COLUMN_FAMILY;
    private static final byte[] EXPLAIN_PLAN_COLUMN = PVarchar.INSTANCE.toBytes("Plan");
    private static final String EXPLAIN_PLAN_ALIAS = "PLAN";
    private static final String EXPLAIN_PLAN_TABLE_NAME = "PLAN_TABLE";
    private static final PDatum EXPLAIN_PLAN_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }
        @Override
        public PDataType getDataType() {
            return PVarchar.INSTANCE;
        }
        @Override
        public Integer getMaxLength() {
            return null;
        }
        @Override
        public Integer getScale() {
            return null;
        }
        @Override
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }
    };

    private static final RowProjector EXPLAIN_PLAN_ROW_PROJECTOR = new RowProjector(Arrays.<ColumnProjector>asList(
            new ExpressionProjector(EXPLAIN_PLAN_ALIAS, EXPLAIN_PLAN_TABLE_NAME, 
                    new RowKeyColumnExpression(EXPLAIN_PLAN_DATUM,
                            new RowKeyValueAccessor(Collections.<PDatum>singletonList(EXPLAIN_PLAN_DATUM), 0)), false)
            ), 0, true);
    private static class ExecutableExplainStatement extends ExplainStatement implements CompilableStatement {

        public ExecutableExplainStatement(BindableStatement statement) {
            super(statement);
        }

        @Override
        public CompilableStatement getStatement() {
            return (CompilableStatement) super.getStatement();
        }
        
        @Override
        public int getBindCount() {
            return getStatement().getBindCount();
        }

        @SuppressWarnings("unchecked")
        @Override
        public QueryPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            CompilableStatement compilableStmt = getStatement();
            final StatementPlan plan = compilableStmt.compilePlan(stmt, Sequence.ValueOp.VALIDATE_SEQUENCE);
            List<String> planSteps = plan.getExplainPlan().getPlanSteps();
            List<Tuple> tuples = Lists.newArrayListWithExpectedSize(planSteps.size());
            for (String planStep : planSteps) {
                Tuple tuple = new SingleKeyValueTuple(KeyValueUtil.newKeyValue(PVarchar.INSTANCE.toBytes(planStep), EXPLAIN_PLAN_FAMILY, EXPLAIN_PLAN_COLUMN, MetaDataProtocol.MIN_TABLE_TIMESTAMP, ByteUtil.EMPTY_BYTE_ARRAY));
                tuples.add(tuple);
            }
            final ResultIterator iterator = new MaterializedResultIterator(tuples);
            return new QueryPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("EXPLAIN PLAN"));
                }

                @Override
                public ResultIterator iterator() throws SQLException {
                    return iterator;
                }
                
                @Override
                public ResultIterator iterator(ParallelScanGrouper scanGrouper) throws SQLException {
                    return iterator;
                }

                @Override
                public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
                    return iterator;
                }

                @Override
                public long getEstimatedSize() {
                    return 0;
                }

                @Override
                public TableRef getTableRef() {
                    return null;
                }

                @Override
                public Set<TableRef> getSourceRefs() {
                    return Collections.emptySet();
                }

                @Override
                public RowProjector getProjector() {
                    return EXPLAIN_PLAN_ROW_PROJECTOR;
                }

                @Override
                public Integer getLimit() {
                    return null;
                }

                @Override
                public Integer getOffset() {
                    return null;
                }

                @Override
                public OrderBy getOrderBy() {
                    return OrderBy.EMPTY_ORDER_BY;
                }

                @Override
                public GroupBy getGroupBy() {
                    return GroupBy.EMPTY_GROUP_BY;
                }

                @Override
                public List<KeyRange> getSplits() {
                    return Collections.emptyList();
                }

                @Override
                public List<List<Scan>> getScans() {
                    return Collections.emptyList();
                }

                @Override
                public StatementContext getContext() {
                    return plan.getContext();
                }

                @Override
                public FilterableStatement getStatement() {
                    return null;
                }

                @Override
                public boolean isDegenerate() {
                    return false;
                }

                @Override
                public boolean isRowKeyOrdered() {
                    return true;
                }

				@Override
				public Operation getOperation() {
					return this.getOperation();
				}
                @Override
                public boolean useRoundRobinIterator() throws SQLException {
                    return false;
                }
                
            };
        }
    }

    private static class ExecutableUpsertStatement extends UpsertStatement implements CompilableStatement {
        private ExecutableUpsertStatement(NamedTableNode table, HintNode hintNode, List<ColumnName> columns,
                List<ParseNode> values, SelectStatement select, int bindCount, Map<String, UDFParseNode> udfParseNodes,
                List<Pair<ColumnName,ParseNode>> onDupKeyPairs) {
            super(table, hintNode, columns, values, select, bindCount, udfParseNodes, onDupKeyPairs);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            if(!getUdfParseNodes().isEmpty()) {
                stmt.throwIfUnallowedUserDefinedFunctions(getUdfParseNodes());
            }
			UpsertCompiler compiler = new UpsertCompiler(stmt, this.getOperation());
            MutationPlan plan = compiler.compile(this);
            plan.getContext().getSequenceManager().validateSequences(seqAction);
            return plan;
        }
    }
    
    private static class ExecutableDeleteStatement extends DeleteStatement implements CompilableStatement {
        private ExecutableDeleteStatement(NamedTableNode table, HintNode hint, ParseNode whereNode, List<OrderByNode> orderBy, LimitNode limit, int bindCount, Map<String, UDFParseNode> udfParseNodes) {
            super(table, hint, whereNode, orderBy, limit, bindCount, udfParseNodes);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            if(!getUdfParseNodes().isEmpty()) {
                stmt.throwIfUnallowedUserDefinedFunctions(getUdfParseNodes());
            }
		    DeleteCompiler compiler = new DeleteCompiler(stmt, this.getOperation());
            MutationPlan plan = compiler.compile(this);
            plan.getContext().getSequenceManager().validateSequences(seqAction);
            return plan;
        }
    }
    
    private static class ExecutableCreateTableStatement extends CreateTableStatement implements CompilableStatement {
        ExecutableCreateTableStatement(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columnDefs,
                PrimaryKeyConstraint pkConstraint, List<ParseNode> splitNodes, PTableType tableType, boolean ifNotExists,
                TableName baseTableName, ParseNode tableTypeIdNode, int bindCount, Boolean immutableRows) {
            super(tableName, props, columnDefs, pkConstraint, splitNodes, tableType, ifNotExists, baseTableName, tableTypeIdNode, bindCount, immutableRows);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            CreateTableCompiler compiler = new CreateTableCompiler(stmt, this.getOperation());
            return compiler.compile(this);
        }
    }

    private static class ExecutableCreateSchemaStatement extends CreateSchemaStatement implements CompilableStatement {
        ExecutableCreateSchemaStatement(String schemaName, boolean ifNotExists) {
            super(schemaName, ifNotExists);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            CreateSchemaCompiler compiler = new CreateSchemaCompiler(stmt);
            return compiler.compile(this);
        }
    }

    private static class ExecutableCreateFunctionStatement extends CreateFunctionStatement implements CompilableStatement {

        public ExecutableCreateFunctionStatement(PFunction functionInfo, boolean temporary, boolean isReplace) {
            super(functionInfo, temporary, isReplace);
        }


        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            stmt.throwIfUnallowedUserDefinedFunctions(Collections.EMPTY_MAP);
            CreateFunctionCompiler compiler = new CreateFunctionCompiler(stmt);
            return compiler.compile(this);
        }
    }

    private static class ExecutableDropFunctionStatement extends DropFunctionStatement implements CompilableStatement {

        public ExecutableDropFunctionStatement(String functionName, boolean ifNotExists) {
            super(functionName, ifNotExists);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
                final StatementContext context = new StatementContext(stmt);
                return new BaseMutationPlan(context, this.getOperation()) {

                    @Override
                    public ParameterMetaData getParameterMetaData() {
                        return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                    }

                    @Override
                    public ExplainPlan getExplainPlan() throws SQLException {
                        return new ExplainPlan(Collections.singletonList("DROP FUNCTION"));
                    }

                    @Override
                    public MutationState execute() throws SQLException {
                        MetaDataClient client = new MetaDataClient(getContext().getConnection());
                        return client.dropFunction(ExecutableDropFunctionStatement.this);
                    }
                };
        }
    }
    
    private static class ExecutableAddJarsStatement extends AddJarsStatement implements CompilableStatement {

        public ExecutableAddJarsStatement(List<LiteralParseNode> jarPaths) {
            super(jarPaths);
        }


        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ADD JARS"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    String dynamicJarsDir = stmt.getConnection().getQueryServices().getProps().get(QueryServices.DYNAMIC_JARS_DIR_KEY);
                    if(dynamicJarsDir == null) {
                        throw new SQLException(QueryServices.DYNAMIC_JARS_DIR_KEY+" is not configured for placing the jars.");
                    }
                    dynamicJarsDir =
                            dynamicJarsDir.endsWith("/") ? dynamicJarsDir : dynamicJarsDir + '/';
                    Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
                    Path dynamicJarsDirPath = new Path(dynamicJarsDir);
                    for (LiteralParseNode jarPath : getJarPaths()) {
                        String jarPathStr = (String)jarPath.getValue();
                        if(!jarPathStr.endsWith(".jar")) {
                            throw new SQLException(jarPathStr + " is not a valid jar file path.");
                        }
                    }

                    try {
                        FileSystem fs = dynamicJarsDirPath.getFileSystem(conf);
                        List<LiteralParseNode> jarPaths = getJarPaths();
                        for (LiteralParseNode jarPath : jarPaths) {
                            File f = new File((String) jarPath.getValue());
                            fs.copyFromLocalFile(new Path(f.getAbsolutePath()), new Path(
                                    dynamicJarsDir + f.getName()));
                        }
                    } catch(IOException e) {
                        throw new SQLException(e);
                    }
                    return new MutationState(0, context.getConnection());
                }
            };
            
        }
    }

    private static class ExecutableDeleteJarStatement extends DeleteJarStatement implements CompilableStatement {

        public ExecutableDeleteJarStatement(LiteralParseNode jarPath) {
            super(jarPath);
        }


        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("DELETE JAR"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    String dynamicJarsDir = stmt.getConnection().getQueryServices().getProps().get(QueryServices.DYNAMIC_JARS_DIR_KEY);
                    if (dynamicJarsDir == null) {
                        throw new SQLException(QueryServices.DYNAMIC_JARS_DIR_KEY
                                + " is not configured.");
                    }
                    dynamicJarsDir =
                            dynamicJarsDir.endsWith("/") ? dynamicJarsDir : dynamicJarsDir + '/';
                    Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
                    Path dynamicJarsDirPath = new Path(dynamicJarsDir);
                    try {
                        FileSystem fs = dynamicJarsDirPath.getFileSystem(conf);
                        String jarPathStr = (String)getJarPath().getValue();
                        if(!jarPathStr.endsWith(".jar")) {
                            throw new SQLException(jarPathStr + " is not a valid jar file path.");
                        }
                        Path p = new Path(jarPathStr);
                        if(fs.exists(p)) {
                            fs.delete(p, false);
                        }
                    } catch(IOException e) {
                        throw new SQLException(e);
                    }
                    return new MutationState(0, context.getConnection());
                }
            };
            
        }
    }

    private static class ExecutableListJarsStatement extends ListJarsStatement implements CompilableStatement {

        public ExecutableListJarsStatement() {
            super();
        }


        @SuppressWarnings("unchecked")
        @Override
        public QueryPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            return new ListJarsQueryPlan(stmt);
        }
    }

    private static class ExecutableCreateIndexStatement extends CreateIndexStatement implements CompilableStatement {

        public ExecutableCreateIndexStatement(NamedNode indexName, NamedTableNode dataTable, IndexKeyConstraint ikConstraint, List<ColumnName> includeColumns, List<ParseNode> splits,
                ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, IndexType indexType, boolean async, int bindCount, Map<String, UDFParseNode> udfParseNodes) {
            super(indexName, dataTable, ikConstraint, includeColumns, splits, props, ifNotExists, indexType, async , bindCount, udfParseNodes);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            if(!getUdfParseNodes().isEmpty()) {
                stmt.throwIfUnallowedUserDefinedFunctions(getUdfParseNodes());
            }
			CreateIndexCompiler compiler = new CreateIndexCompiler(stmt, this.getOperation());
            return compiler.compile(this);
        }
    }
    
    private static class ExecutableCreateSequenceStatement extends	CreateSequenceStatement implements CompilableStatement {

        public ExecutableCreateSequenceStatement(TableName sequenceName, ParseNode startWith,
                ParseNode incrementBy, ParseNode cacheSize, ParseNode minValue, ParseNode maxValue,
                boolean cycle, boolean ifNotExists, int bindCount) {
            super(sequenceName, startWith, incrementBy, cacheSize, minValue, maxValue, cycle,
                    ifNotExists, bindCount);
        }

		@SuppressWarnings("unchecked")
        @Override
		public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
		    CreateSequenceCompiler compiler = new CreateSequenceCompiler(stmt, this.getOperation());
            return compiler.compile(this);
		}
	}

    private static class ExecutableDropSequenceStatement extends DropSequenceStatement implements CompilableStatement {


        public ExecutableDropSequenceStatement(TableName sequenceName, boolean ifExists, int bindCount) {
            super(sequenceName, ifExists, bindCount);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            DropSequenceCompiler compiler = new DropSequenceCompiler(stmt, this.getOperation());
            return compiler.compile(this);
        }
    }

    private static class ExecutableDropTableStatement extends DropTableStatement implements CompilableStatement {

        ExecutableDropTableStatement(TableName tableName, PTableType tableType, boolean ifExists, boolean cascade) {
            super(tableName, tableType, ifExists, cascade);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("DROP TABLE"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getContext().getConnection());
                    return client.dropTable(ExecutableDropTableStatement.this);
                }
            };
        }
    }

    private static class ExecutableDropSchemaStatement extends DropSchemaStatement implements CompilableStatement {

        ExecutableDropSchemaStatement(String schemaName, boolean ifExists, boolean cascade) {
            super(schemaName, ifExists, cascade);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("DROP SCHEMA"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getContext().getConnection());
                    return client.dropSchema(ExecutableDropSchemaStatement.this);
                }
            };
        }
    }

    private static class ExecutableUseSchemaStatement extends UseSchemaStatement implements CompilableStatement {

        ExecutableUseSchemaStatement(String schemaName) {
            super(schemaName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("USE SCHEMA"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getContext().getConnection());
                    return client.useSchema(ExecutableUseSchemaStatement.this);
                }
            };
        }
    }

    private static class ExecutableDropIndexStatement extends DropIndexStatement implements CompilableStatement {

        public ExecutableDropIndexStatement(NamedNode indexName, TableName tableName, boolean ifExists) {
            super(indexName, tableName, ifExists);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("DROP INDEX"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getContext().getConnection());
                    return client.dropIndex(ExecutableDropIndexStatement.this);
                }
            };
        }
    }

    private static class ExecutableAlterIndexStatement extends AlterIndexStatement implements CompilableStatement {

        public ExecutableAlterIndexStatement(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState state, boolean async) {
            super(indexTableNode, dataTableName, ifExists, state, async);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {
                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ALTER INDEX"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getContext().getConnection());
                    return client.alterIndex(ExecutableAlterIndexStatement.this);
                }
            };
        }
    }
    
    private static class ExecutableTraceStatement extends TraceStatement implements CompilableStatement {

        public ExecutableTraceStatement(boolean isTraceOn, double samplingRate) {
            super(isTraceOn, samplingRate);
        }

        @SuppressWarnings("unchecked")
        @Override
        public QueryPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            return new TraceQueryPlan(this, stmt);
        }
    }

    private static class ExecutableAlterSessionStatement extends AlterSessionStatement implements CompilableStatement {

        public ExecutableAlterSessionStatement(Map<String,Object> props) {
            super(props);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public StatementContext getContext() {
                    return context;
                }

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ALTER SESSION"));
                }


                @Override
                public MutationState execute() throws SQLException {
                    Object consistency = getProps().get(PhoenixRuntime.CONSISTENCY_ATTRIB.toUpperCase());
                    if(consistency != null) {
                        if (((String)consistency).equalsIgnoreCase(Consistency.TIMELINE.toString())){
                            getContext().getConnection().setConsistency(Consistency.TIMELINE);
                        } else {
                        	getContext().getConnection().setConsistency(Consistency.STRONG);
                        }
                    }
                    return new MutationState(0, context.getConnection());
                }
            };
        }
    }

    private static class ExecutableUpdateStatisticsStatement extends UpdateStatisticsStatement implements
            CompilableStatement {
        public ExecutableUpdateStatisticsStatement(NamedTableNode table, StatisticsCollectionScope scope, Map<String,Object> props) {
            super(table, scope, props);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("UPDATE STATISTICS"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getContext().getConnection());
                    return client.updateStatistics(ExecutableUpdateStatisticsStatement.this);
                }
            };
        }

    }
    
    private static class ExecutableExecuteUpgradeStatement extends ExecuteUpgradeStatement implements CompilableStatement {
        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            return new MutationPlan() {
                
                @Override
                public Set<TableRef> getSourceRefs() {
                    return Collections.emptySet();
                }
                
                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }
                
                @Override
                public Operation getOperation() {
                    return Operation.UPGRADE;
                }
                
                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("EXECUTE UPGRADE"));
                }
                
                @Override
                public StatementContext getContext() {
                    return new StatementContext(stmt);
                }
                
                @Override
                public TableRef getTargetRef() {
                    return TableRef.EMPTY_TABLE_REF;
                }
                
                @Override
                public MutationState execute() throws SQLException {
                    PhoenixConnection phxConn = stmt.getConnection();
                    Properties props = new Properties();
                    phxConn.getQueryServices().upgradeSystemTables(phxConn.getURL(), props);
                    return MutationState.emptyMutationState(-1, phxConn);
                }
            };
        }
    }

    private static class ExecutableAddColumnStatement extends AddColumnStatement implements CompilableStatement {

        ExecutableAddColumnStatement(NamedTableNode table, PTableType tableType, List<ColumnDef> columnDefs, boolean ifNotExists, ListMultimap<String,Pair<String,Object>> props) {
            super(table, tableType, columnDefs, ifNotExists, props);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ALTER " + getTableType() + " ADD COLUMN"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getContext().getConnection());
                    return client.addColumn(ExecutableAddColumnStatement.this);
                }
            };
        }
    }

    private static class ExecutableDropColumnStatement extends DropColumnStatement implements CompilableStatement {

        ExecutableDropColumnStatement(NamedTableNode table, PTableType tableType, List<ColumnName> columnRefs, boolean ifExists) {
            super(table, tableType, columnRefs, ifExists);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new BaseMutationPlan(context, this.getOperation()) {

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ALTER " + getTableType() + " DROP COLUMN"));
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getContext().getConnection());
                    return client.dropColumn(ExecutableDropColumnStatement.this);
                }
            };
        }
    }

    protected static class ExecutableNodeFactory extends ParseNodeFactory {
        @Override
        public ExecutableSelectStatement select(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
                List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, OffsetNode offset, int bindCount, boolean isAggregate,
                boolean hasSequence, List<SelectStatement> selects, Map<String, UDFParseNode> udfParseNodes) {
            return new ExecutableSelectStatement(from, hint, isDistinct, select, where, groupBy == null ? Collections.<ParseNode>emptyList() : groupBy,
                    having, orderBy == null ? Collections.<OrderByNode>emptyList() : orderBy, limit, offset, bindCount, isAggregate, hasSequence, selects == null ? Collections.<SelectStatement>emptyList() : selects, udfParseNodes);
        }

        @Override
        public ExecutableUpsertStatement upsert(NamedTableNode table, HintNode hintNode, List<ColumnName> columns, List<ParseNode> values, SelectStatement select, int bindCount, 
                Map<String, UDFParseNode> udfParseNodes, List<Pair<ColumnName,ParseNode>> onDupKeyPairs) {
            return new ExecutableUpsertStatement(table, hintNode, columns, values, select, bindCount, udfParseNodes, onDupKeyPairs);
        }
        
        @Override
        public ExecutableDeleteStatement delete(NamedTableNode table, HintNode hint, ParseNode whereNode, List<OrderByNode> orderBy, LimitNode limit, int bindCount, Map<String, UDFParseNode> udfParseNodes) {
            return new ExecutableDeleteStatement(table, hint, whereNode, orderBy, limit, bindCount, udfParseNodes);
        }
        
        @Override
        public CreateTableStatement createTable(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columns, PrimaryKeyConstraint pkConstraint,
                List<ParseNode> splits, PTableType tableType, boolean ifNotExists, TableName baseTableName, ParseNode tableTypeIdNode, int bindCount, Boolean immutableRows) {
            return new ExecutableCreateTableStatement(tableName, props, columns, pkConstraint, splits, tableType, ifNotExists, baseTableName, tableTypeIdNode, bindCount, immutableRows);
        }

        @Override
        public CreateSchemaStatement createSchema(String schemaName, boolean ifNotExists) {
            return new ExecutableCreateSchemaStatement(schemaName, ifNotExists);
        }

        @Override
        public CreateSequenceStatement createSequence(TableName tableName, ParseNode startsWith,
                ParseNode incrementBy, ParseNode cacheSize, ParseNode minValue, ParseNode maxValue,
                boolean cycle, boolean ifNotExists, int bindCount) {
            return new ExecutableCreateSequenceStatement(tableName, startsWith, incrementBy,
                    cacheSize, minValue, maxValue, cycle, ifNotExists, bindCount);
        }
        
        @Override
        public CreateFunctionStatement createFunction(PFunction functionInfo, boolean temporary, boolean isReplace) {
            return new ExecutableCreateFunctionStatement(functionInfo, temporary, isReplace);
        }

        @Override
        public AddJarsStatement addJars(List<LiteralParseNode> jarPaths) {
            return new ExecutableAddJarsStatement(jarPaths);
        }

        @Override
        public DeleteJarStatement deleteJar(LiteralParseNode jarPath)  {
            return new ExecutableDeleteJarStatement(jarPath);
        }

        @Override
        public ListJarsStatement listJars() {
            return new ExecutableListJarsStatement();
        }


        @Override
        public DropSequenceStatement dropSequence(TableName tableName, boolean ifExists, int bindCount){
            return new ExecutableDropSequenceStatement(tableName, ifExists, bindCount);
        }
        
        @Override
        public CreateIndexStatement createIndex(NamedNode indexName, NamedTableNode dataTable, IndexKeyConstraint ikConstraint, List<ColumnName> includeColumns, List<ParseNode> splits,
                ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, IndexType indexType, boolean async, int bindCount, Map<String, UDFParseNode> udfParseNodes) {
            return new ExecutableCreateIndexStatement(indexName, dataTable, ikConstraint, includeColumns, splits, props, ifNotExists, indexType, async, bindCount, udfParseNodes);
        }
        
        @Override
        public AddColumnStatement addColumn(NamedTableNode table,  PTableType tableType, List<ColumnDef> columnDefs, boolean ifNotExists, ListMultimap<String,Pair<String,Object>> props) {
            return new ExecutableAddColumnStatement(table, tableType, columnDefs, ifNotExists, props);
        }
        
        @Override
        public DropColumnStatement dropColumn(NamedTableNode table,  PTableType tableType, List<ColumnName> columnNodes, boolean ifExists) {
            return new ExecutableDropColumnStatement(table, tableType, columnNodes, ifExists);
        }
        
        @Override
        public DropTableStatement dropTable(TableName tableName, PTableType tableType, boolean ifExists, boolean cascade) {
            return new ExecutableDropTableStatement(tableName, tableType, ifExists, cascade);
        }

        @Override
        public DropSchemaStatement dropSchema(String schemaName, boolean ifExists, boolean cascade) {
            return new ExecutableDropSchemaStatement(schemaName, ifExists, cascade);
        }

        @Override
        public UseSchemaStatement useSchema(String schemaName) {
            return new ExecutableUseSchemaStatement(schemaName);
        }

        @Override
        public DropFunctionStatement dropFunction(String functionName, boolean ifExists) {
            return new ExecutableDropFunctionStatement(functionName, ifExists);
        }
        
        @Override
        public DropIndexStatement dropIndex(NamedNode indexName, TableName tableName, boolean ifExists) {
            return new ExecutableDropIndexStatement(indexName, tableName, ifExists);
        }
        
        @Override
        public AlterIndexStatement alterIndex(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState state, boolean async) {
            return new ExecutableAlterIndexStatement(indexTableNode, dataTableName, ifExists, state, async);
        }

        @Override
        public TraceStatement trace(boolean isTraceOn, double samplingRate) {
            return new ExecutableTraceStatement(isTraceOn, samplingRate);
        }

        @Override
        public AlterSessionStatement alterSession(Map<String, Object> props) {
            return new ExecutableAlterSessionStatement(props);
        }

        @Override
        public ExplainStatement explain(BindableStatement statement) {
            return new ExecutableExplainStatement(statement);
        }

        @Override
        public UpdateStatisticsStatement updateStatistics(NamedTableNode table, StatisticsCollectionScope scope, Map<String,Object> props) {
            return new ExecutableUpdateStatisticsStatement(table, scope, props);
        }
        
        @Override
        public ExecuteUpgradeStatement executeUpgrade() {
            return new ExecutableExecuteUpgradeStatement();
        }
    }
    
    static class PhoenixStatementParser extends SQLParser {
        PhoenixStatementParser(String query, ParseNodeFactory nodeFactory) throws IOException {
            super(query, nodeFactory);
        }

        PhoenixStatementParser(Reader reader) throws IOException {
            super(reader);
        }
        
        @Override
        public CompilableStatement nextStatement(ParseNodeFactory nodeFactory) throws SQLException {
            return (CompilableStatement) super.nextStatement(nodeFactory);
        }

        @Override
        public CompilableStatement parseStatement() throws SQLException {
            return (CompilableStatement) super.parseStatement();
        }
    }
    
    public Format getFormatter(PDataType type) {
        return connection.getFormatter(type);
    }
    
    protected final List<PhoenixPreparedStatement> batch = Lists.newArrayList();
    
    @Override
    public void addBatch(String sql) throws SQLException {
        batch.add(new PhoenixPreparedStatement(connection, sql));
    }

    @Override
    public void clearBatch() throws SQLException {
        batch.clear();
    }

    /**
     * Execute the current batch of statements. If any exception occurs
     * during execution, a {@link org.apache.phoenix.exception.BatchUpdateException}
     * is thrown which includes the index of the statement within the
     * batch when the exception occurred.
     */
    @Override
    public int[] executeBatch() throws SQLException {
        int i = 0;
        try {
            int[] returnCodes = new int [batch.size()];
            for (i = 0; i < returnCodes.length; i++) {
                PhoenixPreparedStatement statement = batch.get(i);
                returnCodes[i] = statement.execute(true) ? Statement.SUCCESS_NO_INFO : statement.getUpdateCount();
            }
            // Flush all changes in batch if auto flush is true
            flushIfNecessary();
            // If we make it all the way through, clear the batch
            clearBatch();
            return returnCodes;
        } catch (Throwable t) {
            throw new BatchUpdateExecution(t,i);
        }
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void close() throws SQLException {
        try {
            List<PhoenixResultSet> resultSets = this.resultSets;
            // Create new list so that remove of the PhoenixResultSet
            // during closeAll doesn't needless do a linear search
            // on this list.
            this.resultSets = Lists.newArrayList();
            SQLCloseables.closeAll(resultSets);
        } finally {
            try {
                connection.removeStatement(this);
            } finally {
                isClosed = true;
            }
        }
    }

    public List<Object> getParameters() {
        return Collections.<Object>emptyList();
    }
    
    protected CompilableStatement parseStatement(String sql) throws SQLException {
        PhoenixStatementParser parser = null;
        try {
            parser = new PhoenixStatementParser(sql, new ExecutableNodeFactory());
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
        CompilableStatement statement = parser.parseStatement();
        return statement;
    }
    
    public QueryPlan optimizeQuery(String sql) throws SQLException {
        QueryPlan plan = compileQuery(sql);
        return connection.getQueryServices().getOptimizer().optimize(this, plan);
    }

    public QueryPlan compileQuery(String sql) throws SQLException {
        CompilableStatement stmt = parseStatement(sql);
        return compileQuery(stmt, sql);
    }

    public QueryPlan compileQuery(CompilableStatement stmt, String query) throws SQLException {
        if (stmt.getOperation().isMutation()) {
            throw new ExecuteQueryNotApplicableException(query);
        }
        return stmt.compilePlan(this, Sequence.ValueOp.VALIDATE_SEQUENCE);
    }

    public MutationPlan compileMutation(CompilableStatement stmt, String query) throws SQLException {
        if (!stmt.getOperation().isMutation()) {
            throw new ExecuteUpdateNotApplicableException(query);
        }
        return stmt.compilePlan(this, Sequence.ValueOp.VALIDATE_SEQUENCE);
    }

    public MutationPlan compileMutation(String sql) throws SQLException {
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("Execute update: " + sql, connection));
        }
        CompilableStatement stmt = parseStatement(sql);
        return compileMutation(stmt, sql);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (logger.isDebugEnabled()) {
            logger.debug(LogUtil.addCustomAnnotations("Execute query: " + sql, connection));
        }
        CompilableStatement stmt = parseStatement(sql);
        if (stmt.getOperation().isMutation()) {
            throw new ExecuteQueryNotApplicableException(sql);
        }
        return executeQuery(stmt);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        CompilableStatement stmt = parseStatement(sql);
        if (!stmt.getOperation().isMutation) {
            throw new ExecuteUpdateNotApplicableException(sql);
        }
        if (!batch.isEmpty()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.EXECUTE_UPDATE_WITH_NON_EMPTY_BATCH)
            .build().buildException();
        }
        int updateCount = executeMutation(stmt);
        flushIfNecessary();
        return updateCount;
    }

    private void flushIfNecessary() throws SQLException {
        if (connection.getAutoFlush()) {
            connection.flush();
        }
    }
    
    @Override
    public boolean execute(String sql) throws SQLException {
        CompilableStatement stmt = parseStatement(sql);
        if (stmt.getOperation().isMutation()) {
            if (!batch.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.EXECUTE_UPDATE_WITH_NON_EMPTY_BATCH)
                .build().buildException();
            }
            executeMutation(stmt);
            flushIfNecessary();
            return false;
        }
        executeQuery(stmt);
        return true;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public PhoenixConnection getConnection() {
        return connection;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getFetchSize() throws SQLException {
	if (fetchSize>0)
                return fetchSize;
        else
        	return connection.getQueryServices().getProps().getInt(QueryServices.SCAN_CACHE_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_SCAN_CACHE_SIZE);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0; // TODO: 4000?
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    // For testing
    public QueryPlan getQueryPlan() {
        return getLastQueryPlan();
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet rs = getLastResultSet();
        setLastResultSet(null);
        return rs;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO: not sure this matters
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public Operation getUpdateOperation() {
        return getLastUpdateOperation();
    }
    
    @Override
    public int getUpdateCount() throws SQLException {
        int updateCount = getLastUpdateCount();
        // Only first call can get the update count, otherwise
        // some SQL clients get into an infinite loop when an
        // update occurs.
        setLastUpdateCount(NO_UPDATE);
        return updateCount;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // TODO: any escaping we need to do?
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        // TODO: map to Scan.setBatch() ?
        this.fetchSize = fetchSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (poolable) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    /**
     * When setting the query timeout via JDBC timeouts must be expressed in seconds. Therefore
     * we need to convert the default timeout to milliseconds for internal use. 
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            this.queryTimeoutMillis = getDefaultQueryTimeoutMillis();
        } else if (seconds == 0) {
            this.queryTimeoutMillis = Integer.MAX_VALUE;
        } else {
            this.queryTimeoutMillis = seconds * 1000;
        }
    }

    @Override
    /**
     * When getting the query timeout via JDBC timeouts must be expressed in seconds. Therefore
     * we need to convert the default millisecond timeout to seconds. 
     */
    public int getQueryTimeout() throws SQLException {
        // Convert milliseconds to seconds by taking the CEIL up to the next second
        int scaledValue;
        try {
            scaledValue = IntMath.checkedAdd(queryTimeoutMillis, 999);
        } catch (ArithmeticException e) {
            scaledValue = Integer.MAX_VALUE;
        }
        return scaledValue / 1000;
    }
    
    /**
     * Returns the configured timeout in milliseconds. This
     * internally enables the of use millisecond timeout granularity
     * and honors the exact value configured by phoenix.query.timeoutMs.
     */
    public int getQueryTimeoutInMillis() {
        return queryTimeoutMillis;
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private PhoenixResultSet getLastResultSet() {
        return lastResultSet;
    }

    private void setLastResultSet(PhoenixResultSet lastResultSet) {
        this.lastResultSet = lastResultSet;
    }

    private int getLastUpdateCount() {
        return lastUpdateCount;
    }

    private void setLastUpdateCount(int lastUpdateCount) {
        this.lastUpdateCount = lastUpdateCount;
    }

    private Operation getLastUpdateOperation() {
        return lastUpdateOperation;
    }

    private void setLastUpdateOperation(Operation lastUpdateOperation) {
        this.lastUpdateOperation = lastUpdateOperation;
    }

    private QueryPlan getLastQueryPlan() {
        return lastQueryPlan;
    }

    private void setLastQueryPlan(QueryPlan lastQueryPlan) {
        this.lastQueryPlan = lastQueryPlan;
    }
    
    private void throwIfUnallowedUserDefinedFunctions(Map<String, UDFParseNode> udfParseNodes) throws SQLException {
        if (!connection
                .getQueryServices()
                .getProps()
                .getBoolean(QueryServices.ALLOW_USER_DEFINED_FUNCTIONS_ATTRIB,
                    QueryServicesOptions.DEFAULT_ALLOW_USER_DEFINED_FUNCTIONS)) {
            if(udfParseNodes.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNALLOWED_USER_DEFINED_FUNCTIONS)
                .build().buildException();
            }
            throw new FunctionNotFoundException(udfParseNodes.keySet().toString());
        }
    }

}
