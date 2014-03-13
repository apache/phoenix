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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.*;
import org.apache.phoenix.filter.ColumnProjectionFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.*;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;


/**
 *
 * Class that parallelizes the scan over a table using the ExecutorService provided.  Each region of the table will be scanned in parallel with
 * the results accessible through {@link #getIterators()}
 *
 * 
 * @since 0.1
 */
public class ParallelIterators extends ExplainTable implements ResultIterators {
	private static final Logger logger = LoggerFactory.getLogger(ParallelIterators.class);
    private final List<KeyRange> splits;
    private final ParallelIteratorFactory iteratorFactory;
    
    public static interface ParallelIteratorFactory {
        PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner) throws SQLException;
    }

    private static final int DEFAULT_THREAD_TIMEOUT_MS = 60000; // 1min

    static final Function<HRegionLocation, KeyRange> TO_KEY_RANGE = new Function<HRegionLocation, KeyRange>() {
        @Override
        public KeyRange apply(HRegionLocation region) {
            return KeyRange.getKeyRange(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
        }
    };

    public ParallelIterators(StatementContext context, TableRef tableRef, FilterableStatement statement,
            RowProjector projector, GroupBy groupBy, Integer limit, ParallelIteratorFactory iteratorFactory)
            throws SQLException {
        super(context, tableRef, groupBy);
        this.splits = getSplits(context, tableRef, statement.getHint());
        this.iteratorFactory = iteratorFactory;
        Scan scan = context.getScan();
        PTable table = tableRef.getTable();
        if (projector.isProjectEmptyKeyValue()) {
            Map<byte [], NavigableSet<byte []>> familyMap = scan.getFamilyMap();
            // If nothing projected into scan and we only have one column family, just allow everything
            // to be projected and use a FirstKeyOnlyFilter to skip from row to row. This turns out to
            // be quite a bit faster.
            // Where condition columns also will get added into familyMap
            // When where conditions are present, we can not add FirstKeyOnlyFilter at beginning.
            if (familyMap.isEmpty() && context.getWhereCoditionColumns().isEmpty()
                    && table.getColumnFamilies().size() == 1) {
                // Project the one column family. We must project a column family since it's possible
                // that there are other non declared column families that we need to ignore.
                scan.addFamily(table.getColumnFamilies().get(0).getName().getBytes());
                ScanUtil.andFilterAtBeginning(scan, new FirstKeyOnlyFilter());
            } else {
                byte[] ecf = SchemaUtil.getEmptyColumnFamily(table);
                // Project empty key value unless the column family containing it has
                // been projected in its entirety.
                if (!familyMap.containsKey(ecf) || familyMap.get(ecf) != null) {
                    scan.addColumn(ecf, QueryConstants.EMPTY_COLUMN_BYTES);
                }
            }
        } else if (table.getViewType() == ViewType.MAPPED) {
                // Since we don't have the empty key value in MAPPED tables, we must select all CFs in HRS. But only the
                // selected column values are returned back to client
                for (PColumnFamily family : table.getColumnFamilies()) {
                    scan.addFamily(family.getName().getBytes());
                }
        } // TODO adding all CFs here is not correct. It should be done only after ColumnProjectionOptimization.
        if (limit != null) {
            ScanUtil.andFilterAtEnd(scan, new PageFilter(limit));
        }

        doColumnProjectionOptimization(context, scan, table, statement);
    }

    private void doColumnProjectionOptimization(StatementContext context, Scan scan, PTable table, FilterableStatement statement) {
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        if (familyMap != null && !familyMap.isEmpty()) {
            // columnsTracker contain cf -> qualifiers which should get returned.
            Map<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>> columnsTracker = 
                    new TreeMap<ImmutableBytesPtr, NavigableSet<ImmutableBytesPtr>>();
            Set<byte[]> conditionOnlyCfs = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
            int referencedCfCount = familyMap.size();
            for (Pair<byte[], byte[]> whereCol : context.getWhereCoditionColumns()) {
                if (!(familyMap.containsKey(whereCol.getFirst()))) {
                    referencedCfCount++;
                }
            }
            boolean useOptimization;
            if (statement.getHint().hasHint(Hint.SEEK_TO_COLUMN)) {
                // Do not use the optimization
                useOptimization = false;
            } else if (statement.getHint().hasHint(Hint.NO_SEEK_TO_COLUMN)) {
                // Strictly use the optimization
                useOptimization = true;
            } else {
                // when referencedCfCount is >1 and no Hints, we are not using the optimization
                useOptimization = referencedCfCount == 1;
            }
            if (useOptimization) {
                for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
                    ImmutableBytesPtr cf = new ImmutableBytesPtr(entry.getKey());
                    NavigableSet<byte[]> qs = entry.getValue();
                    NavigableSet<ImmutableBytesPtr> cols = null;
                    if (qs != null) {
                        cols = new TreeSet<ImmutableBytesPtr>();
                        for (byte[] q : qs) {
                            cols.add(new ImmutableBytesPtr(q));
                        }
                    }
                    columnsTracker.put(cf, cols);
                }
            }
            // Making sure that where condition CFs are getting scanned at HRS.
            for (Pair<byte[], byte[]> whereCol : context.getWhereCoditionColumns()) {
                if (useOptimization) {
                    if (!(familyMap.containsKey(whereCol.getFirst()))) {
                        scan.addFamily(whereCol.getFirst());
                        conditionOnlyCfs.add(whereCol.getFirst());
                    }
                } else {
                    if (familyMap.containsKey(whereCol.getFirst())) {
                        // where column's CF is present. If there are some specific columns added against this CF, we
                        // need to ensure this where column also getting added in it.
                        // If the select was like select cf1.*, then that itself will select the whole CF. So no need to
                        // specifically add the where column. Adding that will remove the cf1.* stuff and only this
                        // where condition column will get returned!
                        NavigableSet<byte[]> cols = familyMap.get(whereCol.getFirst());
                        // cols is null means the whole CF will get scanned.
                        if (cols != null) {
                            scan.addColumn(whereCol.getFirst(), whereCol.getSecond());
                        }
                    } else {
                        // where column's CF itself is not present in family map. We need to add the column
                        scan.addColumn(whereCol.getFirst(), whereCol.getSecond());
                    }
                }
            }
            if (useOptimization && !columnsTracker.isEmpty()) {
                for (ImmutableBytesPtr f : columnsTracker.keySet()) {
                    // This addFamily will remove explicit cols in scan familyMap and make it as entire row.
                    // We don't want the ExplicitColumnTracker to be used. Instead we have the ColumnProjectionFilter
                    scan.addFamily(f.get());
                }
                // We don't need this filter for aggregates, as we're not returning back what's
                // in the scan in this case. We still want the other optimization that causes
                // the ExplicitColumnTracker not to be used, though.
                if (!(statement.isAggregate())) {
                    ScanUtil.andFilterAtEnd(scan, new ColumnProjectionFilter(SchemaUtil.getEmptyColumnFamily(table),
                            columnsTracker, conditionOnlyCfs));
                }
            }
        }
    }

    /**
     * Splits the given scan's key range so that each split can be queried in parallel
     * @param hintNode TODO
     *
     * @return the key ranges that should be scanned in parallel
     */
    // exposed for tests
    public static List<KeyRange> getSplits(StatementContext context, TableRef table, HintNode hintNode) throws SQLException {
        return ParallelIteratorRegionSplitterFactory.getSplitter(context, table, hintNode).getSplits();
    }

    public List<KeyRange> getSplits() {
        return splits;
    }

    /**
     * Executes the scan in parallel across all regions, blocking until all scans are complete.
     * @return the result iterators for the scan of each region
     */
    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        boolean success = false;
        final ConnectionQueryServices services = context.getConnection().getQueryServices();
        ReadOnlyProps props = services.getProps();
        int numSplits = splits.size();
        List<PeekingResultIterator> iterators = new ArrayList<PeekingResultIterator>(numSplits);
        List<Pair<byte[],Future<PeekingResultIterator>>> futures = new ArrayList<Pair<byte[],Future<PeekingResultIterator>>>(numSplits);
        final UUID scanId = UUID.randomUUID();
        try {
            ExecutorService executor = services.getExecutor();
            for (KeyRange split : splits) {
                final Scan splitScan = new Scan(this.context.getScan());
                // Intersect with existing start/stop key if the table is salted
                // If not salted, we've already intersected it. If salted, we need
                // to wait until now to intersect, as we're running parallel scans
                // on all the possible regions here.
                if (tableRef.getTable().getBucketNum() != null) {
                    KeyRange minMaxRange = context.getMinMaxRange();
                    if (minMaxRange != null) {
                        // Add salt byte based on current split, as minMaxRange won't have it
                        minMaxRange = SaltingUtil.addSaltByte(split.getLowerRange(), minMaxRange);
                        split = split.intersect(minMaxRange);
                    }
                }
                if (ScanUtil.intersectScanRange(splitScan, split.getLowerRange(), split.getUpperRange(), this.context.getScanRanges().useSkipScanFilter())) {
                    // Delay the swapping of start/stop row until row so we don't muck with the intersect logic
                    ScanUtil.swapStartStopRowIfReversed(splitScan);
                    Future<PeekingResultIterator> future =
                        executor.submit(new JobCallable<PeekingResultIterator>() {

                        @Override
                        public PeekingResultIterator call() throws Exception {
                            // TODO: different HTableInterfaces for each thread or the same is better?
                        	long startTime = System.currentTimeMillis();
                            ResultIterator scanner = new TableResultIterator(context, tableRef, splitScan);
                            if (logger.isDebugEnabled()) {
                            	logger.debug("Id: " + scanId + ", Time: " + (System.currentTimeMillis() - startTime) + "ms, Scan: " + splitScan);
                            }
                            return iteratorFactory.newIterator(context, scanner);
                        }

                        /**
                         * Defines the grouping for round robin behavior.  All threads spawned to process
                         * this scan will be grouped together and time sliced with other simultaneously
                         * executing parallel scans.
                         */
                        @Override
                        public Object getJobId() {
                            return ParallelIterators.this;
                        }
                    });
                    futures.add(new Pair<byte[],Future<PeekingResultIterator>>(split.getLowerRange(),future));
                }
            }

            int timeoutMs = props.getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, DEFAULT_THREAD_TIMEOUT_MS);
            final int factor = ScanUtil.isReversed(this.context.getScan()) ? -1 : 1;
            // Sort futures by row key so that we have a predicatble order we're getting rows back for scans.
            // We're going to wait here until they're finished anyway and this makes testing much easier.
            Collections.sort(futures, new Comparator<Pair<byte[],Future<PeekingResultIterator>>>() {
                @Override
                public int compare(Pair<byte[], Future<PeekingResultIterator>> o1, Pair<byte[], Future<PeekingResultIterator>> o2) {
                    return factor * Bytes.compareTo(o1.getFirst(), o2.getFirst());
                }
            });
            for (Pair<byte[],Future<PeekingResultIterator>> future : futures) {
                iterators.add(future.getSecond().get(timeoutMs, TimeUnit.MILLISECONDS));
            }

            success = true;
            return iterators;
        } catch (Exception e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            if (!success) {
                SQLCloseables.closeAllQuietly(iterators);
                // Don't call cancel, as it causes the HConnection to get into a funk
//                for (Pair<byte[],Future<PeekingResultIterator>> future : futures) {
//                    future.getSecond().cancel(true);
//                }
            }
        }
    }

    @Override
    public int size() {
        return this.splits.size();
    }

    @Override
    public void explain(List<String> planSteps) {
        StringBuilder buf = new StringBuilder();
        buf.append("CLIENT PARALLEL " + size() + "-WAY ");
        explain(buf.toString(),planSteps);
    }
}