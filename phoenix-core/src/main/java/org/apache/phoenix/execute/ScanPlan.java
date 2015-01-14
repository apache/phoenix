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


import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.ScanRegionObserver;
import org.apache.phoenix.iterate.ChunkedResultIterator;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.MergeSortRowKeyResultIterator;
import org.apache.phoenix.iterate.MergeSortTopNResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.ResultIterators;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.SerialIterators;
import org.apache.phoenix.iterate.SpoolingResultIterator;
import org.apache.phoenix.parse.FilterableStatement;
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
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.util.LogUtil;
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

    public ScanPlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector, Integer limit, OrderBy orderBy, ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter) throws SQLException {
        super(context, statement, table, projector, context.getBindManager().getParameterMetaData(), limit, orderBy, GroupBy.EMPTY_GROUP_BY,
                parallelIteratorFactory != null ? parallelIteratorFactory :
                        buildResultIteratorFactory(context, table, orderBy, limit, allowPageFilter));
        this.allowPageFilter = allowPageFilter;
        if (!orderBy.getOrderByExpressions().isEmpty()) { // TopN
            int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                    QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            ScanRegionObserver.serializeIntoScan(context.getScan(), thresholdBytes, limit == null ? -1 : limit, orderBy.getOrderByExpressions(), projector.getEstimatedRowByteSize());
        }
    }

    private static boolean isSerial(StatementContext context,
            TableRef tableRef, OrderBy orderBy, Integer limit, boolean allowPageFilter) throws SQLException {
        Scan scan = context.getScan();
        /*
         * If a limit is provided and we have no filter, run the scan serially when we estimate that
         * the limit's worth of data will fit into a single region.
         */
        boolean isOrdered = !orderBy.getOrderByExpressions().isEmpty();
        Integer perScanLimit = !allowPageFilter || isOrdered ? null : limit;
        if (perScanLimit == null || scan.getFilter() != null) {
            return false;
        }
        PTable table = tableRef.getTable();
        GuidePostsInfo gpsInfo = table.getTableStats().getGuidePosts().get(SchemaUtil.getEmptyColumnFamily(table));
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
            long totByteSize = gpsInfo.getByteCount();
            estRegionSize = totByteSize / (gpsInfo.getGuidePosts().size()+1);
        }
        // TODO: configurable number of bytes?
        boolean isSerial = (perScanLimit * estRowSize < estRegionSize);
        
        if (logger.isDebugEnabled()) logger.debug(LogUtil.addCustomAnnotations("With LIMIT=" + perScanLimit
                + ", estimated row size=" + estRowSize
                + ", estimated region size=" + estRegionSize + " (" + (gpsInfo == null ? "without " : "with ") + "stats)"
                + ": " + (isSerial ? "SERIAL" : "PARALLEL") + " execution", context.getConnection()));
        return isSerial;
    }
    
    private static ParallelIteratorFactory buildResultIteratorFactory(StatementContext context,
            TableRef table, OrderBy orderBy, Integer limit, boolean allowPageFilter) throws SQLException {

        if (isSerial(context, table, orderBy, limit, allowPageFilter)) {
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
                    spoolingResultIteratorFactory, table);
        }
    }

    @Override
    public List<KeyRange> getSplits() {
        return splits;
    }

    @Override
    public List<List<Scan>> getScans() {
        return scans;
    }

    @Override
    protected ResultIterator newIterator() throws SQLException {
        // Set any scan attributes before creating the scanner, as it will be too late afterwards
    	Scan scan = context.getScan();
        scan.setAttribute(BaseScannerRegionObserver.NON_AGGREGATE_QUERY, QueryConstants.TRUE);
        ResultIterator scanner;
        TableRef tableRef = this.getTableRef();
        PTable table = tableRef.getTable();
        boolean isSalted = table.getBucketNum() != null;
        /* If no limit or topN, use parallel iterator so that we get results faster. Otherwise, if
         * limit is provided, run query serially.
         */
        boolean isOrdered = !orderBy.getOrderByExpressions().isEmpty();
        boolean isSerial = isSerial(context, tableRef, orderBy, limit, allowPageFilter);
        Integer perScanLimit = !allowPageFilter || isOrdered ? null : limit;
        ResultIterators iterators;
        if (isSerial) {
        	iterators = new SerialIterators(this, perScanLimit, parallelIteratorFactory);
        } else {
        	iterators = new ParallelIterators(this, perScanLimit, parallelIteratorFactory);
        }
        splits = iterators.getSplits();
        scans = iterators.getScans();
        if (isOrdered) {
            scanner = new MergeSortTopNResultIterator(iterators, limit, orderBy.getOrderByExpressions());
        } else {
            if ((isSalted || table.getIndexType() == IndexType.LOCAL) &&
                    (context.getConnection().getQueryServices().getProps().getBoolean(
                            QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB,
                            QueryServicesOptions.DEFAULT_ROW_KEY_ORDER_SALTED_TABLE) ||
                     orderBy == OrderBy.FWD_ROW_KEY_ORDER_BY ||
                     orderBy == OrderBy.REV_ROW_KEY_ORDER_BY)) { // ORDER BY was optimized out b/c query is in row key order
                scanner = new MergeSortRowKeyResultIterator(iterators, isSalted ? SaltingUtil.NUM_SALTING_BYTES : 0, orderBy == OrderBy.REV_ROW_KEY_ORDER_BY);
            } else {
                scanner = new ConcatResultIterator(iterators);
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
}