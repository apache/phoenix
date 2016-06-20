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


import static org.apache.phoenix.util.ScanUtil.isPacingScannersPossible;
import static org.apache.phoenix.util.ScanUtil.isRoundRobinPossible;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.ScanRegionObserver;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.iterate.ChunkedResultIterator;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.MergeSortRowKeyResultIterator;
import org.apache.phoenix.iterate.MergeSortTopNResultIterator;
import org.apache.phoenix.iterate.OffsetResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.RoundRobinResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.SerialIterators;
import org.apache.phoenix.iterate.SpoolingResultIterator;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.PTableStats;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 * Query plan for a basic table scan
 *
 *
 * @since 0.1
 */
public class ScanPlan extends BaseQueryPlan {
    private static final Logger logger = LoggerFactory.getLogger(ScanPlan.class);
    private List<KeyRange> splits;
    private List<List<Scan>> scans;
    private boolean allowPageFilter;
    
    public ScanPlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy, ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter) throws SQLException {
        this(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory, allowPageFilter, null);
    }
    
    private ScanPlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy, ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter, Expression dynamicFilter) throws SQLException {
        super(context, statement, table, projector, context.getBindManager().getParameterMetaData(), limit,offset, orderBy, GroupBy.EMPTY_GROUP_BY,
                parallelIteratorFactory != null ? parallelIteratorFactory :
                        buildResultIteratorFactory(context, statement, table, orderBy, limit, offset, allowPageFilter), dynamicFilter);
        this.allowPageFilter = allowPageFilter;
        if (!orderBy.getOrderByExpressions().isEmpty()) { // TopN
            int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                    QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            ScanRegionObserver.serializeIntoScan(context.getScan(), thresholdBytes, limit == null ? -1 : limit, orderBy.getOrderByExpressions(), projector.getEstimatedRowByteSize());
        }
    }

    private static boolean isSerial(StatementContext context, FilterableStatement statement,
            TableRef tableRef, OrderBy orderBy, Integer limit, Integer offset, boolean allowPageFilter) throws SQLException {
        PTable table = tableRef.getTable();
        boolean hasSerialHint = statement.getHint().hasHint(HintNode.Hint.SERIAL);
        boolean canBeExecutedSerially = ScanUtil.canQueryBeExecutedSerially(table, orderBy, context); 
        if (!canBeExecutedSerially) { 
            if (hasSerialHint) {
                logger.warn("This query cannot be executed serially. Ignoring the hint");
            }
            return false;
        } else if (hasSerialHint) {
            return true;
        }
        
        Scan scan = context.getScan();
        /*
         * If a limit is provided and we have no filter, run the scan serially when we estimate that
         * the limit's worth of data will fit into a single region.
         */
        Integer perScanLimit = !allowPageFilter ? null : limit;
        if (perScanLimit == null || scan.getFilter() != null) {
            return false;
        }
        long scn = context.getConnection().getSCN() == null ? Long.MAX_VALUE : context.getConnection().getSCN();
        PTableStats tableStats = context.getConnection().getQueryServices().getTableStats(table.getName().getBytes(), scn);
        GuidePostsInfo gpsInfo = tableStats.getGuidePosts().get(SchemaUtil.getEmptyColumnFamily(table));
        long estRowSize = SchemaUtil.estimateRowSize(table);
        long estRegionSize;
        if (gpsInfo == null) {
            // Use guidepost depth as minimum size
            ConnectionQueryServices services = context.getConnection().getQueryServices();
            HTableDescriptor desc = services.getTableDescriptor(table.getPhysicalName().getBytes());
            int guidepostPerRegion = services.getProps().getInt(QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB,
                    QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_PER_REGION);
            long guidepostWidth = services.getProps().getLong(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB,
                    QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES);
            estRegionSize = StatisticsUtil.getGuidePostDepth(guidepostPerRegion, guidepostWidth, desc);
        } else {
            // Region size estimated based on total number of bytes divided by number of regions
            long totByteSize = 0;
            for (long byteCount : gpsInfo.getByteCounts()) {
                totByteSize += byteCount;
            }
            estRegionSize = totByteSize / (gpsInfo.getGuidePostsCount()+1);
        }
        // TODO: configurable number of bytes?
        boolean isSerial = (perScanLimit * estRowSize < estRegionSize);
        
        if (logger.isDebugEnabled()) logger.debug(LogUtil.addCustomAnnotations("With LIMIT=" + perScanLimit
                + ", estimated row size=" + estRowSize
                + ", estimated region size=" + estRegionSize + " (" + (gpsInfo == null ? "without " : "with ") + "stats)"
                + ": " + (isSerial ? "SERIAL" : "PARALLEL") + " execution", context.getConnection()));
        return isSerial;
    }
    
    private static ParallelIteratorFactory buildResultIteratorFactory(StatementContext context, FilterableStatement statement,
            TableRef table, OrderBy orderBy, Integer limit,Integer offset, boolean allowPageFilter) throws SQLException {

        if ((isSerial(context, statement, table, orderBy, limit, offset, allowPageFilter)
                || isRoundRobinPossible(orderBy, context) || isPacingScannersPossible(context))) {
            return ParallelIteratorFactory.NOOP_FACTORY;
        }
        ParallelIteratorFactory spoolingResultIteratorFactory =
                new SpoolingResultIterator.SpoolingResultIteratorFactory(
                        context.getConnection().getQueryServices());

        // If we're doing an order by then we need the full result before we can do anything,
        // so we don't bother chunking it. If we're just doing a simple scan then we chunk
        // the scan to have a quicker initial response.
        if (!orderBy.getOrderByExpressions().isEmpty()) {
            return spoolingResultIteratorFactory;
        } else {
            return new ChunkedResultIterator.ChunkedResultIteratorFactory(
                    spoolingResultIteratorFactory, context.getConnection().getMutationState(), table);
        }
    }

    @Override
    public List<KeyRange> getSplits() {
        if (splits == null)
            return Collections.emptyList();
        else
            return splits;
    }

    @Override
    public List<List<Scan>> getScans() {
        if (scans == null)
            return Collections.emptyList();
        else
            return scans;
    }

    private static boolean isOffsetPossibleOnServer(StatementContext context, OrderBy orderBy, Integer offset,
            boolean isSalted, IndexType indexType) {
        return offset != null && orderBy.getOrderByExpressions().isEmpty()
                && !((isSalted || indexType == IndexType.LOCAL)
                        && ScanUtil.shouldRowsBeInRowKeyOrder(orderBy, context));
    }

    @Override
    protected ResultIterator newIterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        // Set any scan attributes before creating the scanner, as it will be too late afterwards
        scan.setAttribute(BaseScannerRegionObserver.NON_AGGREGATE_QUERY, QueryConstants.TRUE);
        ResultIterator scanner;
        TableRef tableRef = this.getTableRef();
        PTable table = tableRef.getTable();
        boolean isSalted = table.getBucketNum() != null;
        /* If no limit or topN, use parallel iterator so that we get results faster. Otherwise, if
         * limit is provided, run query serially.
         */
        boolean isOrdered = !orderBy.getOrderByExpressions().isEmpty();
        boolean isSerial = isSerial(context, statement, tableRef, orderBy, limit, offset, allowPageFilter);
        Integer perScanLimit = !allowPageFilter || isOrdered ? null : limit;
        if (perScanLimit != null) {
            perScanLimit = QueryUtil.getOffsetLimit(perScanLimit, offset);
        }
        BaseResultIterators iterators;
        boolean isOffsetOnServer = isOffsetPossibleOnServer(context, orderBy, offset, isSalted, table.getIndexType());
        if (isOffsetOnServer) {
            iterators = new SerialIterators(this, perScanLimit, offset, parallelIteratorFactory, scanGrouper, scan);
        } else if (isSerial) {
            iterators = new SerialIterators(this, perScanLimit, null, parallelIteratorFactory, scanGrouper, scan);
        } else {
            iterators = new ParallelIterators(this, perScanLimit, parallelIteratorFactory, scanGrouper, scan);
        }
        splits = iterators.getSplits();
        scans = iterators.getScans();
        estimatedSize = iterators.getEstimatedByteCount();
        estimatedRows = iterators.getEstimatedRowCount();
        if (isOffsetOnServer) {
            scanner = new ConcatResultIterator(iterators);
            if (limit != null) {
                scanner = new LimitingResultIterator(scanner, limit);
            }
        } else if (isOrdered) {
            scanner = new MergeSortTopNResultIterator(iterators, limit, offset, orderBy.getOrderByExpressions());
        } else {
            if ((isSalted || table.getIndexType() == IndexType.LOCAL) && ScanUtil.shouldRowsBeInRowKeyOrder(orderBy, context)) {
                /*
                 * For salted tables or local index, a merge sort is needed if: 
                 * 1) The config phoenix.query.force.rowkeyorder is set to true 
                 * 2) Or if the query has an order by that wants to sort
                 * the results by the row key (forward or reverse ordering)
                 */
                scanner = new MergeSortRowKeyResultIterator(iterators, isSalted ? SaltingUtil.NUM_SALTING_BYTES : 0, orderBy == OrderBy.REV_ROW_KEY_ORDER_BY);
            } else if (useRoundRobinIterator()) {
                /*
                 * For any kind of tables, round robin is possible if there is
                 * no ordering of rows needed.
                 */
                scanner = new RoundRobinResultIterator(iterators, this);
            } else {
                scanner = new ConcatResultIterator(iterators);
            }
            if (offset != null) {
                scanner = new OffsetResultIterator(scanner, offset);
            }
            if (limit != null) {
                scanner = new LimitingResultIterator(scanner, limit);
            }
        }

        if (context.getSequenceManager().getSequenceCount() > 0) {
            scanner = new SequenceResultIterator(scanner, context.getSequenceManager());
        }
        return scanner;
    }
    
    @Override
    public boolean useRoundRobinIterator() throws SQLException {
        return ScanUtil.isRoundRobinPossible(orderBy, context);
    }

}