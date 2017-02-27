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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.iterate.DefaultParallelScanGrouper;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.trace.TracingIterator;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.collect.ImmutableSet;
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
    protected final Set<TableRef> tableRefs;
    protected final StatementContext context;
    protected final FilterableStatement statement;
    protected final RowProjector projection;
    protected final ParameterMetaData paramMetaData;
    protected final Integer limit;
    protected final Integer offset;
    protected final OrderBy orderBy;
    protected final GroupBy groupBy;
    protected final ParallelIteratorFactory parallelIteratorFactory;    
    /*
     * The filter expression that contains CorrelateVariableFieldAccessExpression
     * and will have impact on the ScanRanges. It will recompiled at runtime 
     * immediately before creating the ResultIterator.
     */
    protected final Expression dynamicFilter;
    protected Long estimatedRows;
    protected Long estimatedSize;
    

    protected BaseQueryPlan(
            StatementContext context, FilterableStatement statement, TableRef table,
            RowProjector projection, ParameterMetaData paramMetaData, Integer limit, Integer offset, OrderBy orderBy,
            GroupBy groupBy, ParallelIteratorFactory parallelIteratorFactory,
            Expression dynamicFilter) {
        this.context = context;
        this.statement = statement;
        this.tableRef = table;
        this.tableRefs = ImmutableSet.of(table);
        this.projection = projection;
        this.paramMetaData = paramMetaData;
        this.limit = limit;
        this.offset = offset;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.parallelIteratorFactory = parallelIteratorFactory;
        this.dynamicFilter = dynamicFilter;
    }

    public Long getEstimatedRowCount() {
        return this.estimatedRows;
    }
    
    public Long getEstimatedByteCount() {
        return this.estimatedSize;
    }
    

	@Override
	public Operation getOperation() {
		return Operation.QUERY;
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
    public Set<TableRef> getSourceRefs() {
        return tableRefs;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }
    
    @Override
    public Integer getOffset() {
        return offset;
    }

    @Override
    public RowProjector getProjector() {
        return projection;
    }
    
    public Expression getDynamicFilter() {
        return dynamicFilter;
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
        return iterator(Collections.<SQLCloseable>emptyList(), DefaultParallelScanGrouper.getInstance(), null);
    }
    
    @Override
    public final ResultIterator iterator(ParallelScanGrouper scanGrouper) throws SQLException {
        return iterator(Collections.<SQLCloseable>emptyList(), scanGrouper, null);
    }

    @Override
    public final ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        return iterator(Collections.<SQLCloseable>emptyList(), scanGrouper, scan);
    }

    public final ResultIterator iterator(final List<? extends SQLCloseable> dependencies, ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
         if (scan == null) {
             scan = context.getScan();
         }
         
		/*
		 * For aggregate queries, we still need to let the AggregationPlan to
		 * proceed so that we can give proper aggregates even if there are no
		 * row to be scanned.
		 */
        if (context.getScanRanges() == ScanRanges.NOTHING && !getStatement().isAggregate()) {
            return ResultIterator.EMPTY_ITERATOR;
        }
        
        if (tableRef == TableRef.EMPTY_TABLE_REF) {
            return newIterator(scanGrouper, scan);
        }
        
        // Set miscellaneous scan attributes. This is the last chance to set them before we
        // clone the scan for each parallelized chunk.
        TableRef tableRef = context.getCurrentTable();
        PTable table = tableRef.getTable();
        
        if (dynamicFilter != null) {
            WhereCompiler.compile(context, statement, null, Collections.singletonList(dynamicFilter), false, null);            
        }
        
        if (OrderBy.REV_ROW_KEY_ORDER_BY.equals(orderBy)) {
            ScanUtil.setReversed(scan);
            // Hack for working around PHOENIX-3121 and HBASE-16296.
            // TODO: remove once PHOENIX-3121 and/or HBASE-16296 are fixed.
            int scannerCacheSize = context.getStatement().getFetchSize();
            if (limit != null && limit % scannerCacheSize == 0) {
                scan.setCaching(scannerCacheSize + 1);
            }
        }
        
        if (statement.getHint().hasHint(Hint.SMALL)) {
            scan.setSmall(true);
        }
        
        PhoenixConnection connection = context.getConnection();

        // set read consistency
        if (table.getType() != PTableType.SYSTEM) {
            scan.setConsistency(connection.getConsistency());
        }
        // TODO fix this in PHOENIX-2415 Support ROW_TIMESTAMP with transactional tables
        if (!table.isTransactional()) {
	                // Get the time range of row_timestamp column
	        TimeRange rowTimestampRange = context.getScanRanges().getRowTimestampRange();
	        // Get the already existing time range on the scan.
	        TimeRange scanTimeRange = scan.getTimeRange();
	        Long scn = connection.getSCN();
	        if (scn == null) {
	            // If we haven't resolved the time at the beginning of compilation, don't
	            // force the lookup on the server, but use HConstants.LATEST_TIMESTAMP instead.
	            scn = tableRef.getTimeStamp();
	            if (scn == QueryConstants.UNSET_TIMESTAMP) {
	                scn = HConstants.LATEST_TIMESTAMP;
	            }
	        }
	        try {
	            TimeRange timeRangeToUse = ScanUtil.intersectTimeRange(rowTimestampRange, scanTimeRange, scn);
	            if (timeRangeToUse == null) {
	                return ResultIterator.EMPTY_ITERATOR;
	            }
	            scan.setTimeRange(timeRangeToUse.getMin(), timeRangeToUse.getMax());
	        } catch (IOException e) {
	            throw new RuntimeException(e);
	        }
	    }
        byte[] tenantIdBytes;
        if( table.isMultiTenant() == true ) {
            tenantIdBytes = connection.getTenantId() == null ? null :
                    ScanUtil.getTenantIdBytes(
                            table.getRowKeySchema(),
                            table.getBucketNum() != null,
                            connection.getTenantId(), table.getViewIndexId() != null);
        } else {
            tenantIdBytes = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
        }

        ScanUtil.setTenantId(scan, tenantIdBytes);
        String customAnnotations = LogUtil.customAnnotationsToString(connection);
        ScanUtil.setCustomAnnotations(scan, customAnnotations == null ? null : customAnnotations.getBytes());
        // Set local index related scan attributes. 
        if (table.getIndexType() == IndexType.LOCAL) {
            ScanUtil.setLocalIndex(scan);
            Set<PColumn> dataColumns = context.getDataColumns();
            // If any data columns to join back from data table are present then we set following attributes
            // 1. data columns to be projected and their key value schema.
            // 2. index maintainer and view constants if exists to build data row key from index row key. 
            // TODO: can have an hint to skip joining back to data table, in that case if any column to
            // project is not present in the index then we need to skip this plan.
            if (!dataColumns.isEmpty()) {
                // Set data columns to be join back from data table.
                PTable parentTable = context.getCurrentTable().getTable();
                String parentSchemaName = parentTable.getParentSchemaName().getString();
                String parentTableName = parentTable.getParentTableName().getString();
                final ParseNodeFactory FACTORY = new ParseNodeFactory();
                // TODO: is it necessary to re-resolve the table?
                TableRef dataTableRef =
                        FromCompiler.getResolver(
                            FACTORY.namedTable(null, TableName.create(parentSchemaName, parentTableName)),
                            context.getConnection()).resolveTable(parentSchemaName, parentTableName);
                PTable dataTable = dataTableRef.getTable();
                // Set data columns to be join back from data table.
                serializeDataTableColumnsToJoin(scan, dataColumns, dataTable);
                KeyValueSchema schema = ProjectedColumnExpression.buildSchema(dataColumns);
                // Set key value schema of the data columns.
                serializeSchemaIntoScan(scan, schema);
                
                // Set index maintainer of the local index.
                serializeIndexMaintainerIntoScan(scan, dataTable);
                // Set view constants if exists.
                serializeViewConstantsIntoScan(scan, dataTable);
            }
        }
        
        if (LOG.isDebugEnabled()) {
        	LOG.debug(LogUtil.addCustomAnnotations("Scan ready for iteration: " + scan, connection));
        }
        
        ResultIterator iterator = newIterator(scanGrouper, scan);
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

    private void serializeIndexMaintainerIntoScan(Scan scan, PTable dataTable) throws SQLException {
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
        scan.setAttribute(BaseScannerRegionObserver.LOCAL_INDEX_BUILD_PROTO, ByteUtil.copyKeyBytesIfNecessary(ptr));
        if (dataTable.isTransactional()) {
            scan.setAttribute(BaseScannerRegionObserver.TX_STATE, context.getConnection().getMutationState().encodeTransaction());
        }
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

    private void serializeDataTableColumnsToJoin(Scan scan, Set<PColumn> dataColumns, PTable dataTable) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            boolean storeColsInSingleCell = dataTable.getImmutableStorageScheme() == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
            if (storeColsInSingleCell) {
                // if storeColsInSingleCell is true all columns of a given column family are stored in a single cell
                scan.setAttribute(BaseScannerRegionObserver.COLUMNS_STORED_IN_SINGLE_CELL, QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
            }
            WritableUtils.writeVInt(output, dataColumns.size());
            for (PColumn column : dataColumns) {
                byte[] cf = column.getFamilyName().getBytes();
                byte[] cq = column.getColumnQualifierBytes();
                Bytes.writeByteArray(output, cf);
                Bytes.writeByteArray(output, cq);
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

    abstract protected ResultIterator newIterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException;
    
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
            return new ExplainPlan(Collections.singletonList("DEGENERATE SCAN OVER " + getTableRef().getTable().getName().getString()));
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