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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.htrace.Sampler;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixParameterMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.TraceStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SizedUtil;

public class TraceQueryPlan implements QueryPlan {

    private TraceStatement traceStatement = null;
    private PhoenixStatement stmt = null;
    private StatementContext context = null;
    private boolean first = true;

    private static final RowProjector TRACE_PROJECTOR;
    static {
        List<ExpressionProjector> projectedColumns = new ArrayList<ExpressionProjector>();
        PColumn column =
                new PColumnImpl(PNameFactory.newName(MetricInfo.TRACE.columnName), null,
                        PLong.INSTANCE, null, null, false, 0, SortOrder.getDefault(), 0, null,
                        false, null);
        List<PColumn> columns = new ArrayList<PColumn>();
        columns.add(column);
        Expression expression =
                new RowKeyColumnExpression(column, new RowKeyValueAccessor(columns, 0));
        projectedColumns.add(new ExpressionProjector(MetricInfo.TRACE.columnName, "", expression,
                true));
        int estimatedByteSize = SizedUtil.KEY_VALUE_SIZE + PLong.INSTANCE.getByteSize();
        TRACE_PROJECTOR = new RowProjector(projectedColumns, estimatedByteSize, false);
    }

    public TraceQueryPlan(TraceStatement traceStatement, PhoenixStatement stmt) {
        this.traceStatement = traceStatement;
        this.stmt = stmt;
        this.context = new StatementContext(stmt);
    }

    @Override
    public StatementContext getContext() {
        return this.context;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        return ExplainPlan.EMPTY_PLAN;
    }

    @Override
    public ResultIterator iterator() throws SQLException {
        final PhoenixConnection conn = stmt.getConnection();
        if (conn.getTraceScope() == null && !traceStatement.isTraceOn()) {
            return ResultIterator.EMPTY_ITERATOR;
        }
        return new ResultIterator() {

            @Override
            public void close() throws SQLException {
            }

            @Override
            public Tuple next() throws SQLException {
                if(!first) return null;
                TraceScope traceScope = conn.getTraceScope();
                if (traceStatement.isTraceOn()) {
                    conn.setSampler(Tracing.getConfiguredSampler(traceStatement));
                    if (conn.getSampler() == Sampler.NEVER) {
                        closeTraceScope(conn);
                    }
                    if (traceScope == null && !conn.getSampler().equals(Sampler.NEVER)) {
                        traceScope = Tracing.startNewSpan(conn, "Enabling trace");
                        if (traceScope.getSpan() != null) {
                            conn.setTraceScope(traceScope);
                        } else {
                            closeTraceScope(conn);
                        }
                    }
                } else {
                    closeTraceScope(conn);
                    conn.setSampler(Sampler.NEVER);
                }
                if (traceScope == null || traceScope.getSpan() == null) return null;
                first = false;
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                ParseNodeFactory factory = new ParseNodeFactory();
                LiteralParseNode literal =
                        factory.literal(traceScope.getSpan().getTraceId());
                LiteralExpression expression =
                        LiteralExpression.newConstant(literal.getValue(), PLong.INSTANCE,
                            Determinism.ALWAYS);
                expression.evaluate(null, ptr);
                byte[] rowKey = ByteUtil.copyKeyBytesIfNecessary(ptr);
                Cell cell =
                        CellUtil.createCell(rowKey, HConstants.EMPTY_BYTE_ARRAY,
                            HConstants.EMPTY_BYTE_ARRAY, System.currentTimeMillis(),
                            Type.Put.getCode(), HConstants.EMPTY_BYTE_ARRAY);
                List<Cell> cells = new ArrayList<Cell>(1);
                cells.add(cell);
                return new ResultTuple(Result.create(cells));
            }

            private void closeTraceScope(final PhoenixConnection conn) {
                if(conn.getTraceScope()!=null) {
                    conn.getTraceScope().close();
                    conn.setTraceScope(null);
                }
            }

            @Override
            public void explain(List<String> planSteps) {
            }
        };
    }

    @Override
    public long getEstimatedSize() {
        return PLong.INSTANCE.getByteSize();
    }

    @Override
    public TableRef getTableRef() {
        return null;
    }

    @Override
    public RowProjector getProjector() {
        return TRACE_PROJECTOR;
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
    public FilterableStatement getStatement() {
        return null;
    }

    @Override
    public boolean isDegenerate() {
        return false;
    }

    @Override
    public boolean isRowKeyOrdered() {
        return false;
    }
}