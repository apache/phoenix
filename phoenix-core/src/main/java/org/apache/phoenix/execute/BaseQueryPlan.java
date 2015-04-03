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
package org.apache.phoenix.execute;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.trace.TracingIterator;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ScanUtil;
import org.apache.htrace.TraceScope;

import com.google.common.collect.Lists;



/**
 *
 * Query plan that has no child plans
 *
 * 
 * @since 0.1
 */
public abstract class BaseQueryPlan implements QueryPlan {
	private static final Log LOG = LogFactory.getLog(BaseQueryPlan.class);
    protected static final long DEFAULT_ESTIMATED_SIZE = 10 * 1024; // 10 K
    
    protected final TableRef tableRef;
    protected final StatementContext context;
    protected final FilterableStatement statement;
    protected final RowProjector projection;
    protected final ParameterMetaData paramMetaData;
    protected final Integer limit;
    protected final OrderBy orderBy;
    protected final GroupBy groupBy;
    protected final ParallelIteratorFactory parallelIteratorFactory;

    protected BaseQueryPlan(
            StatementContext context, FilterableStatement statement, TableRef table,
            RowProjector projection, ParameterMetaData paramMetaData, Integer limit, OrderBy orderBy,
            GroupBy groupBy, ParallelIteratorFactory parallelIteratorFactory) {
        this.context = context;
        this.statement = statement;
        this.tableRef = table;
        this.projection = projection;
        this.paramMetaData = paramMetaData;
        this.limit = limit;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.parallelIteratorFactory = parallelIteratorFactory;
    }

    @Override
    public boolean isDegenerate() {
        return context.getScanRanges() == ScanRanges.NOTHING;

    }
    
    @Override
    public GroupBy getGroupBy() {
        return groupBy;
    }

    
    @Override
    public OrderBy getOrderBy() {
        return orderBy;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }

    @Override
    public RowProjector getProjector() {
        return projection;
    }

//    /**
//     * Sets up an id used to do round robin queue processing on the server
//     * @param scan
//     */
//    private void setProducer(Scan scan) {
//        byte[] producer = Bytes.toBytes(UUID.randomUUID().toString());
//        scan.setAttribute(HBaseServer.CALL_QUEUE_PRODUCER_ATTRIB_NAME, producer);
//    }
    
    @Override
    public final ResultIterator iterator() throws SQLException {
        return iterator(Collections.<SQLCloseable>emptyList());
    }

    public final ResultIterator iterator(final List<? extends SQLCloseable> dependencies) throws SQLException {
        if (context.getScanRanges() == ScanRanges.NOTHING) {
            return ResultIterator.EMPTY_ITERATOR;
        }
        
        // Set miscellaneous scan attributes. This is the last chance to set them before we
        // clone the scan for each parallelized chunk.
        Scan scan = context.getScan();
        
        if (OrderBy.REV_ROW_KEY_ORDER_BY.equals(orderBy)) {
            ScanUtil.setReversed(scan);
        }
        
        if (statement.getHint().hasHint(Hint.SMALL)) {
            scan.setSmall(true);
        }
        
        // Set producer on scan so HBase server does round robin processing
        //setProducer(scan);
        // Set the time range on the scan so we don't get back rows newer than when the statement was compiled
        // The time stamp comes from the server at compile time when the meta data
        // is resolved.
        // TODO: include time range in explain plan?
        PhoenixConnection connection = context.getConnection();

        // set read consistency
        if (context.getCurrentTable() != null
                && context.getCurrentTable().getTable().getType() != PTableType.SYSTEM) {
            scan.setConsistency(connection.getConsistency());
        }
        if (context.getScanTimeRange() == null) {
          Long scn = connection.getSCN();
          if (scn == null) {
            scn = context.getCurrentTime();
          }
          ScanUtil.setTimeRange(scan, scn);
        } else {
            ScanUtil.setTimeRange(scan, context.getScanTimeRange());
        }
        ScanUtil.setTenantId(scan, connection.getTenantId() == null ? null : connection.getTenantId().getBytes());
        String customAnnotations = LogUtil.customAnnotationsToString(connection);
        ScanUtil.setCustomAnnotations(scan, customAnnotations == null ? null : customAnnotations.getBytes());
        // Set local index related scan attributes. 
        if (context.getCurrentTable().getTable().getIndexType() == IndexType.LOCAL) {
            ScanUtil.setLocalIndex(scan);
            Set<PColumn> dataColumns = context.getDataColumns();
            // If any data columns to join back from data table are present then we set following attributes
            // 1. data columns to be projected and their key value schema.
            // 2. index maintainer and view constants if exists to build data row key from index row key. 
            // TODO: can have an hint to skip joining back to data table, in that case if any column to
            // project is not present in the index then we need to skip this plan.
            if (!dataColumns.isEmpty()) {
                // Set data columns to be join back from data table.
                serializeDataTableColumnsToJoin(scan, dataColumns);
                KeyValueSchema schema = ProjectedColumnExpression.buildSchema(dataColumns);
                // Set key value schema of the data columns.
                serializeSchemaIntoScan(scan, schema);
                String parentSchema = context.getCurrentTable().getTable().getParentSchemaName().getString();
                String parentTable = context.getCurrentTable().getTable().getParentTableName().getString();
                final ParseNodeFactory FACTORY = new ParseNodeFactory();
                TableRef dataTableRef =
                        FromCompiler.getResolver(
                            FACTORY.namedTable(null, TableName.create(parentSchema, parentTable)),
                            context.getConnection()).resolveTable(parentSchema, parentTable);
                PTable dataTable = dataTableRef.getTable();
                // Set index maintainer of the local index.
                serializeIndexMaintainerIntoScan(scan, dataTable);
                // Set view constants if exists.
                serializeViewConstantsIntoScan(scan, dataTable);
            }
        }
        
        if (LOG.isDebugEnabled()) {
        	LOG.debug(LogUtil.addCustomAnnotations("Scan ready for iteration: " + scan, connection));
        }
        
        ResultIterator iterator = newIterator();
        iterator = dependencies.isEmpty() ?
                iterator : new DelegateResultIterator(iterator) {
            @Override
            public void close() throws SQLException {
                try {
                    super.close();
                } finally {
                    SQLCloseables.closeAll(dependencies);
                }
            }
        };
        
        if (LOG.isDebugEnabled()) {
        	LOG.debug(LogUtil.addCustomAnnotations("Iterator ready: " + iterator, connection));
        }

        // wrap the iterator so we start/end tracing as we expect
        TraceScope scope =
                Tracing.startNewSpan(context.getConnection(), "Creating basic query for "
                        + getPlanSteps(iterator));
        return (scope.getSpan() != null) ? new TracingIterator(scope, iterator) : iterator;
    }

    private void serializeIndexMaintainerIntoScan(Scan scan, PTable dataTable) {
        PName name = context.getCurrentTable().getTable().getName();
        List<PTable> indexes = Lists.newArrayListWithExpectedSize(1);
        for (PTable index : dataTable.getIndexes()) {
            if (index.getName().equals(name) && index.getIndexType() == IndexType.LOCAL) {
                indexes.add(index);
                break;
            }
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        IndexMaintainer.serialize(dataTable, ptr, indexes, context.getConnection());
        scan.setAttribute(BaseScannerRegionObserver.LOCAL_INDEX_BUILD, ByteUtil.copyKeyBytesIfNecessary(ptr));
    }

    private void serializeViewConstantsIntoScan(Scan scan, PTable dataTable) {
        int dataPosOffset = (dataTable.getBucketNum() != null ? 1 : 0) + (dataTable.isMultiTenant() ? 1 : 0);
        int nViewConstants = 0;
        if (dataTable.getType() == PTableType.VIEW) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            List<PColumn> dataPkColumns = dataTable.getPKColumns();
            for (int i = dataPosOffset; i < dataPkColumns.size(); i++) {
                PColumn dataPKColumn = dataPkColumns.get(i);
                if (dataPKColumn.getViewConstant() != null) {
                    nViewConstants++;
                }
            }
            if (nViewConstants > 0) {
                byte[][] viewConstants = new byte[nViewConstants][];
                int j = 0;
                for (int i = dataPosOffset; i < dataPkColumns.size(); i++) {
                    PColumn dataPkColumn = dataPkColumns.get(i);
                    if (dataPkColumn.getViewConstant() != null) {
                        if (IndexUtil.getViewConstantValue(dataPkColumn, ptr)) {
                            viewConstants[j++] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                }
                serializeViewConstantsIntoScan(viewConstants, scan);
            }
        }
    }

    private void serializeViewConstantsIntoScan(byte[][] viewConstants, Scan scan) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, viewConstants.length);
            for (byte[] viewConstant : viewConstants) {
                Bytes.writeByteArray(output, viewConstant);
            }
            scan.setAttribute(BaseScannerRegionObserver.VIEW_CONSTANTS, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void serializeDataTableColumnsToJoin(Scan scan, Set<PColumn> dataColumns) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, dataColumns.size());
            for (PColumn column : dataColumns) {
                Bytes.writeByteArray(output, column.getFamilyName().getBytes());
                Bytes.writeByteArray(output, column.getName().getBytes());
            }
            scan.setAttribute(BaseScannerRegionObserver.DATA_TABLE_COLUMNS_TO_JOIN, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void serializeSchemaIntoScan(Scan scan, KeyValueSchema schema) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(schema.getEstimatedByteSize());
        try {
            DataOutputStream output = new DataOutputStream(stream);
            schema.write(output);
            scan.setAttribute(BaseScannerRegionObserver.LOCAL_INDEX_JOIN_SCHEMA, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    abstract protected ResultIterator newIterator() throws SQLException;
    
    @Override
    public long getEstimatedSize() {
        return DEFAULT_ESTIMATED_SIZE;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return paramMetaData;
    }

    @Override
    public FilterableStatement getStatement() {
        return statement;
    }

    @Override
    public StatementContext getContext() {
        return context;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        if (context.getScanRanges() == ScanRanges.NOTHING) {
            return new ExplainPlan(Collections.singletonList("DEGENERATE SCAN OVER " + tableRef.getTable().getName().getString()));
        }
        
        // Optimize here when getting explain plan, as queries don't get optimized until after compilation
        QueryPlan plan = context.getConnection().getQueryServices().getOptimizer().optimize(context.getStatement(), this);
        return plan instanceof BaseQueryPlan ? new ExplainPlan(getPlanSteps(plan.iterator())) : plan.getExplainPlan();
    }

    private List<String> getPlanSteps(ResultIterator iterator){
        List<String> planSteps = Lists.newArrayListWithExpectedSize(5);
        iterator.explain(planSteps);
        return planSteps;
    }

    @Override
    public boolean isRowKeyOrdered() {
        return groupBy.isEmpty() ? orderBy.getOrderByExpressions().isEmpty() : groupBy.isOrderPreserving();
    }
}
