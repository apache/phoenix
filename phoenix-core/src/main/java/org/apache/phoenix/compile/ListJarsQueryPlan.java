/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile;

import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.iterate.DefaultParallelScanGrouper;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixParameterMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SizedUtil;

public class ListJarsQueryPlan implements QueryPlan {

    private PhoenixStatement stmt = null;
    private StatementContext context = null;
    private boolean first = true;

    private static final RowProjector JARS_PROJECTOR;
    
    static {
        List<ExpressionProjector> projectedColumns = new ArrayList<ExpressionProjector>();
        PName colName = PNameFactory.newName("jar_location");
        PColumn column =
                new PColumnImpl(colName, null,
                        PVarchar.INSTANCE, null, null, false, 0, SortOrder.getDefault(), 0, null,
                        false, null, false, false, colName.getBytes());
        List<PColumn> columns = new ArrayList<PColumn>();
        columns.add(column);
        Expression expression =
                new RowKeyColumnExpression(column, new RowKeyValueAccessor(columns, 0));
        projectedColumns.add(new ExpressionProjector("jar_location", "", expression,
                true));
        int estimatedByteSize = SizedUtil.KEY_VALUE_SIZE;
        JARS_PROJECTOR = new RowProjector(projectedColumns, estimatedByteSize, false);
    }

    public ListJarsQueryPlan(PhoenixStatement stmt) {
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
        return iterator(DefaultParallelScanGrouper.getInstance());
    }

    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan s) throws SQLException {
        return iterator(scanGrouper);
    }
    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper) throws SQLException {
        return new ResultIterator() {
            private RemoteIterator<LocatedFileStatus> listFiles = null;

            @Override
            public void close() throws SQLException {
                
            }
            
            @Override
            public Tuple next() throws SQLException {
                try {
                    if(first) {
                        String dynamicJarsDir =
                                stmt.getConnection().getQueryServices().getProps()
                                        .get(QueryServices.DYNAMIC_JARS_DIR_KEY);
                        if(dynamicJarsDir == null) {
                            throw new SQLException(QueryServices.DYNAMIC_JARS_DIR_KEY
                                    + " is not configured for the listing the jars.");
                        }
                        dynamicJarsDir =
                                dynamicJarsDir.endsWith("/") ? dynamicJarsDir : dynamicJarsDir + '/';
                        Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
                        Path dynamicJarsDirPath = new Path(dynamicJarsDir);
                        FileSystem fs = dynamicJarsDirPath.getFileSystem(conf);
                        listFiles = fs.listFiles(dynamicJarsDirPath, true);
                        first = false;
                    }
                    if(listFiles == null || !listFiles.hasNext()) return null;
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                    ParseNodeFactory factory = new ParseNodeFactory();
                    LiteralParseNode literal =
                            factory.literal(listFiles.next().getPath().toString());
                    LiteralExpression expression =
                            LiteralExpression.newConstant(literal.getValue(), PVarchar.INSTANCE,
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
                } catch (IOException e) {
                    throw new SQLException(e);
                }  
            }
            
            @Override
            public void explain(List<String> planSteps) {
            }
        };
    }
    
    @Override
    public long getEstimatedSize() {
        return PVarchar.INSTANCE.getByteSize();
    }

    @Override
    public TableRef getTableRef() {
        return null;
    }

    @Override
    public RowProjector getProjector() {
        return JARS_PROJECTOR;
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
    
    @Override
    public boolean useRoundRobinIterator() {
        return false;
    }

	@Override
	public Set<TableRef> getSourceRefs() {
		return Collections.<TableRef>emptySet();
	}

	@Override
	public Operation getOperation() {
		return stmt.getUpdateOperation();
	}
}