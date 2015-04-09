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

import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.MUTATION_COUNT;
import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.QUERY_COUNT;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.QUERY_TIME;

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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.call.CallRunner;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.CreateIndexCompiler;
import org.apache.phoenix.compile.CreateSequenceCompiler;
import org.apache.phoenix.compile.CreateTableCompiler;
import org.apache.phoenix.compile.DeleteCompiler;
import org.apache.phoenix.compile.DropSequenceCompiler;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
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
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.iterate.MaterializedResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.parse.AddColumnStatement;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.AlterSessionStatement;
import org.apache.phoenix.parse.BindableStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DeleteStatement;
import org.apache.phoenix.parse.DropColumnStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.ExplainStatement;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.IndexKeyConstraint;
import org.apache.phoenix.parse.LimitNode;
import org.apache.phoenix.parse.NamedNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TraceStatement;
import org.apache.phoenix.parse.UpdateStatisticsStatement;
import org.apache.phoenix.parse.UpsertStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ExecuteQueryNotApplicableException;
import org.apache.phoenix.schema.ExecuteUpdateNotApplicableException;
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
public class PhoenixStatement implements Statement, SQLCloseable, org.apache.phoenix.jdbc.Jdbc7Shim.Statement {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixStatement.class);
    
    public enum Operation {
        QUERY("queried", false),
        DELETE("deleted", true),
        UPSERT("upserted", true);
        
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
    private int queryTimeout;
    
    public PhoenixStatement(PhoenixConnection connection) {
        this.connection = connection;
        this.queryTimeout = getDefaultQueryTimeout();
    }
    
    private int getDefaultQueryTimeout() {
        // Convert milliseconds to seconds by taking the CEIL up to the next second
        return (connection.getQueryServices().getProps().getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, 
            QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS) + 999) / 1000;
    }

    protected List<PhoenixResultSet> getResultSets() {
        return resultSets;
    }
    
    protected PhoenixResultSet newResultSet(ResultIterator iterator, RowProjector projector) throws SQLException {
        return new PhoenixResultSet(iterator, projector, this);
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
        QueryPlan plan = stmt.compilePlan(this, Sequence.ValueOp.RESERVE_SEQUENCE);
        return connection.getQueryServices().getOptimizer().optimize(this, plan);
    }
    
    protected PhoenixResultSet executeQuery(final CompilableStatement stmt) throws SQLException {
        QUERY_COUNT.increment();
        try {
            return CallRunner.run(
                new CallRunner.CallableThrowable<PhoenixResultSet, SQLException>() {
                @Override
                    public PhoenixResultSet call() throws SQLException {
                    final long startTime = System.currentTimeMillis();
                    try {
                        QueryPlan plan = stmt.compilePlan(PhoenixStatement.this, Sequence.ValueOp.RESERVE_SEQUENCE);
                        plan = connection.getQueryServices().getOptimizer().optimize(
                                PhoenixStatement.this, plan);
                         // this will create its own trace internally, so we don't wrap this
                         // whole thing in tracing
                        ResultIterator resultIterator = plan.iterator();
                        if (logger.isDebugEnabled()) {
                            String explainPlan = QueryUtil.getExplainPlan(resultIterator);
                            logger.debug(LogUtil.addCustomAnnotations("Explain plan: " + explainPlan, connection));
                        }
                        PhoenixResultSet rs = newResultSet(resultIterator, plan.getProjector());
                        resultSets.add(rs);
                        setLastQueryPlan(plan);
                        setLastResultSet(rs);
                        setLastUpdateCount(NO_UPDATE);
                        setLastUpdateOperation(stmt.getOperation());
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
                        QUERY_TIME.update(System.currentTimeMillis() - startTime);
                    }
                }
                }, PhoenixContextExecutor.inContext());
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, SQLException.class);
            throw Throwables.propagate(e);
        }
    }
    
    protected int executeMutation(final CompilableStatement stmt) throws SQLException {
	 if (connection.isReadOnly()) {
            throw new SQLExceptionInfo.Builder(
                SQLExceptionCode.READ_ONLY_CONNECTION).
                build().buildException();
        }
	    MUTATION_COUNT.increment();
        try {
            return CallRunner
                    .run(
                        new CallRunner.CallableThrowable<Integer, SQLException>() {
                        @Override
                            public Integer call() throws SQLException {
                            // Note that the upsert select statements will need to commit any open transaction here,
                            // since they'd update data directly from coprocessors, and should thus operate on
                            // the latest state
                            try {
                                MutationPlan plan = stmt.compilePlan(PhoenixStatement.this, Sequence.ValueOp.RESERVE_SEQUENCE);
                                MutationState state = plan.execute();
                                connection.getMutationState().join(state);
                                if (connection.getAutoCommit()) {
                                    connection.commit();
                                }
                                setLastResultSet(null);
                                setLastQueryPlan(null);
                                // Unfortunately, JDBC uses an int for update count, so we
                                // just max out at Integer.MAX_VALUE
                                int lastUpdateCount = (int) Math.min(Integer.MAX_VALUE, state.getUpdateCount());
                                setLastUpdateCount(lastUpdateCount);
                                setLastUpdateOperation(stmt.getOperation());
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
            throw Throwables.propagate(e);
        }
    }

    protected static interface CompilableStatement extends BindableStatement {
        public <T extends StatementPlan> T compilePlan (PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException;
    }
    
    private static class ExecutableSelectStatement extends SelectStatement implements CompilableStatement {
        private ExecutableSelectStatement(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
                List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate, boolean hasSequence) {
            this(from, hint, isDistinct, select, where, groupBy, having, orderBy, limit, bindCount, isAggregate, hasSequence, Collections.<SelectStatement>emptyList());
        }

        private ExecutableSelectStatement(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
                List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate,
                boolean hasSequence, List<SelectStatement> selects) {
            super(from, hint, isDistinct, select, where, groupBy, having, orderBy, limit, bindCount, isAggregate, hasSequence, selects);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public QueryPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            SelectStatement select = SubselectRewriter.flatten(this, stmt.getConnection());
            ColumnResolver resolver = FromCompiler.getResolverForQuery(select, stmt.getConnection());
            select = StatementNormalizer.normalize(select, resolver);
            SelectStatement transformedSelect = SubqueryRewriter.transform(select, resolver, stmt.getConnection());
            if (transformedSelect != select) {
                resolver = FromCompiler.getResolverForQuery(transformedSelect, stmt.getConnection());
                select = StatementNormalizer.normalize(transformedSelect, resolver);
            }
            QueryPlan plan = new QueryCompiler(stmt, select, resolver).compile();
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
                public long getEstimatedSize() {
                    return 0;
                }

                @Override
                public TableRef getTableRef() {
                    return null;
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
            };
        }
    }

    private static class ExecutableUpsertStatement extends UpsertStatement implements CompilableStatement {
        private ExecutableUpsertStatement(NamedTableNode table, HintNode hintNode, List<ColumnName> columns, List<ParseNode> values, SelectStatement select, int bindCount) {
            super(table, hintNode, columns, values, select, bindCount);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            UpsertCompiler compiler = new UpsertCompiler(stmt);
            MutationPlan plan = compiler.compile(this);
            plan.getContext().getSequenceManager().validateSequences(seqAction);
            return plan;
        }
    }
    
    private static class ExecutableDeleteStatement extends DeleteStatement implements CompilableStatement {
        private ExecutableDeleteStatement(NamedTableNode table, HintNode hint, ParseNode whereNode, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {
            super(table, hint, whereNode, orderBy, limit, bindCount);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            DeleteCompiler compiler = new DeleteCompiler(stmt);
            MutationPlan plan = compiler.compile(this);
            plan.getContext().getSequenceManager().validateSequences(seqAction);
            return plan;
        }
    }
    
    private static class ExecutableCreateTableStatement extends CreateTableStatement implements CompilableStatement {
        ExecutableCreateTableStatement(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columnDefs,
                PrimaryKeyConstraint pkConstraint, List<ParseNode> splitNodes, PTableType tableType, boolean ifNotExists,
                TableName baseTableName, ParseNode tableTypeIdNode, int bindCount) {
            super(tableName, props, columnDefs, pkConstraint, splitNodes, tableType, ifNotExists, baseTableName, tableTypeIdNode, bindCount);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            CreateTableCompiler compiler = new CreateTableCompiler(stmt);
            return compiler.compile(this);
        }
    }

    private static class ExecutableCreateIndexStatement extends CreateIndexStatement implements CompilableStatement {

        public ExecutableCreateIndexStatement(NamedNode indexName, NamedTableNode dataTable, IndexKeyConstraint ikConstraint, List<ColumnName> includeColumns, List<ParseNode> splits,
                ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, IndexType indexType, boolean async, int bindCount) {
            super(indexName, dataTable, ikConstraint, includeColumns, splits, props, ifNotExists, indexType, async , bindCount);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            CreateIndexCompiler compiler = new CreateIndexCompiler(stmt);
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
		    CreateSequenceCompiler compiler = new CreateSequenceCompiler(stmt);
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
            DropSequenceCompiler compiler = new DropSequenceCompiler(stmt);
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
            return new MutationPlan() {

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
                    return new ExplainPlan(Collections.singletonList("DROP TABLE"));
                }

                @Override
                public PhoenixConnection getConnection() {
                    return stmt.getConnection();
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getConnection());
                    return client.dropTable(ExecutableDropTableStatement.this);
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
            return new MutationPlan() {
                
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
                    return new ExplainPlan(Collections.singletonList("DROP INDEX"));
                }

                @Override
                public PhoenixConnection getConnection() {
                    return stmt.getConnection();
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getConnection());
                    return client.dropIndex(ExecutableDropIndexStatement.this);
                }
            };
        }
    }

    private static class ExecutableAlterIndexStatement extends AlterIndexStatement implements CompilableStatement {

        public ExecutableAlterIndexStatement(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState state) {
            super(indexTableNode, dataTableName, ifExists, state);
        }

        @SuppressWarnings("unchecked")
        @Override
        public MutationPlan compilePlan(final PhoenixStatement stmt, Sequence.ValueOp seqAction) throws SQLException {
            final StatementContext context = new StatementContext(stmt);
            return new MutationPlan() {
                
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
                    return new ExplainPlan(Collections.singletonList("ALTER INDEX"));
                }

                @Override
                public PhoenixConnection getConnection() {
                    return stmt.getConnection();
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getConnection());
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
            return new MutationPlan() {

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
                public PhoenixConnection getConnection() {
                    return stmt.getConnection();
                }

                @Override
                public MutationState execute() throws SQLException {
                    Object consistency = getProps().get(PhoenixRuntime.CONSISTENCY_ATTRIB.toUpperCase());
                    if(consistency != null) {
                        if (((String)consistency).equalsIgnoreCase(Consistency.TIMELINE.toString())){
                            getConnection().setConsistency(Consistency.TIMELINE);
                        } else {
                            getConnection().setConsistency(Consistency.STRONG);
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
            return new MutationPlan() {

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
                    return new ExplainPlan(Collections.singletonList("UPDATE STATISTICS"));
                }

                @Override
                public PhoenixConnection getConnection() {
                    return stmt.getConnection();
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getConnection());
                    return client.updateStatistics(ExecutableUpdateStatisticsStatement.this);
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
            return new MutationPlan() {

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
                    return new ExplainPlan(Collections.singletonList("ALTER " + getTableType() + " ADD COLUMN"));
                }

                @Override
                public PhoenixConnection getConnection() {
                    return stmt.getConnection();
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getConnection());
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
            return new MutationPlan() {

                @Override
                public StatementContext getContext() {
                    return context;
                }

               @Override
                public ParameterMetaData getParameterMetaData() {
                    return new PhoenixParameterMetaData(0);
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ALTER " + getTableType() + " DROP COLUMN"));
                }

                @Override
                public PhoenixConnection getConnection() {
                    return stmt.getConnection();
                }

                @Override
                public MutationState execute() throws SQLException {
                    MetaDataClient client = new MetaDataClient(getConnection());
                    return client.dropColumn(ExecutableDropColumnStatement.this);
                }
            };
        }
    }

    protected static class ExecutableNodeFactory extends ParseNodeFactory {
        @Override
        public ExecutableSelectStatement select(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
                List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate,
                boolean hasSequence, List<SelectStatement> selects) {
            return new ExecutableSelectStatement(from, hint, isDistinct, select, where, groupBy == null ? Collections.<ParseNode>emptyList() : groupBy,
                    having, orderBy == null ? Collections.<OrderByNode>emptyList() : orderBy, limit, bindCount, isAggregate, hasSequence, selects == null ? Collections.<SelectStatement>emptyList() : selects);
        }

        @Override
        public ExecutableUpsertStatement upsert(NamedTableNode table, HintNode hintNode, List<ColumnName> columns, List<ParseNode> values, SelectStatement select, int bindCount) {
            return new ExecutableUpsertStatement(table, hintNode, columns, values, select, bindCount);
        }
        
        @Override
        public ExecutableDeleteStatement delete(NamedTableNode table, HintNode hint, ParseNode whereNode, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {
            return new ExecutableDeleteStatement(table, hint, whereNode, orderBy, limit, bindCount);
        }
        
        @Override
        public CreateTableStatement createTable(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columns, PrimaryKeyConstraint pkConstraint,
                List<ParseNode> splits, PTableType tableType, boolean ifNotExists, TableName baseTableName, ParseNode tableTypeIdNode, int bindCount) {
            return new ExecutableCreateTableStatement(tableName, props, columns, pkConstraint, splits, tableType, ifNotExists, baseTableName, tableTypeIdNode, bindCount);
        }
        
        @Override
        public CreateSequenceStatement createSequence(TableName tableName, ParseNode startsWith,
                ParseNode incrementBy, ParseNode cacheSize, ParseNode minValue, ParseNode maxValue,
                boolean cycle, boolean ifNotExists, int bindCount) {
            return new ExecutableCreateSequenceStatement(tableName, startsWith, incrementBy,
                    cacheSize, minValue, maxValue, cycle, ifNotExists, bindCount);
        }
        
        @Override
        public DropSequenceStatement dropSequence(TableName tableName, boolean ifExists, int bindCount){
            return new ExecutableDropSequenceStatement(tableName, ifExists, bindCount);
        }
        
        @Override
        public CreateIndexStatement createIndex(NamedNode indexName, NamedTableNode dataTable, IndexKeyConstraint ikConstraint, List<ColumnName> includeColumns, List<ParseNode> splits,
                ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, IndexType indexType, boolean async, int bindCount) {
            return new ExecutableCreateIndexStatement(indexName, dataTable, ikConstraint, includeColumns, splits, props, ifNotExists, indexType, async, bindCount);
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
        public DropIndexStatement dropIndex(NamedNode indexName, TableName tableName, boolean ifExists) {
            return new ExecutableDropIndexStatement(indexName, tableName, ifExists);
        }
        
        @Override
        public AlterIndexStatement alterIndex(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState state) {
            return new ExecutableAlterIndexStatement(indexTableNode, dataTableName, ifExists, state);
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
        return stmt.compilePlan(this, Sequence.ValueOp.RESERVE_SEQUENCE);
    }

    public MutationPlan compileMutation(CompilableStatement stmt, String query) throws SQLException {
        if (!stmt.getOperation().isMutation()) {
            throw new ExecuteUpdateNotApplicableException(query);
        }
        return stmt.compilePlan(this, Sequence.ValueOp.RESERVE_SEQUENCE);
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
        return executeMutation(stmt);
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
            return false;
        }
        executeQuery(stmt);
        return true;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            this.queryTimeout = getDefaultQueryTimeout();
        } else if (seconds == 0) {
            this.queryTimeout = Integer.MAX_VALUE;
        } else {
            this.queryTimeout = seconds;
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
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
}
