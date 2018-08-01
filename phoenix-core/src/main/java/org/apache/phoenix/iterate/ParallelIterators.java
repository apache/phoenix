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

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_NUM_PARALLEL_SCANS;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.monitoring.TaskExecutionMetricsHolder;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
/**
 *
 * Class that parallelizes the scan over a table using the ExecutorService provided.  Each region of the table will be scanned in parallel with
 * the results accessible through {@link #getIterators()}
 *
 * 
 * @since 0.1
 */
public class ParallelIterators extends BaseResultIterators {
	private static final Logger logger = LoggerFactory.getLogger(ParallelIterators.class);
	private static final String NAME = "PARALLEL";
    private final ParallelIteratorFactory iteratorFactory;
    private final boolean initFirstScanOnly;
    
    public ParallelIterators(QueryPlan plan, Integer perScanLimit, ParallelIteratorFactory iteratorFactory, ParallelScanGrouper scanGrouper, Scan scan, boolean initFirstScanOnly, Map<ImmutableBytesPtr,ServerCache> caches, QueryPlan dataPlan)
            throws SQLException {
        super(plan, perScanLimit, null, scanGrouper, scan,caches, dataPlan);
        this.iteratorFactory = iteratorFactory;
        this.initFirstScanOnly = initFirstScanOnly;
    }   
    
    public ParallelIterators(QueryPlan plan, Integer perScanLimit, ParallelIteratorFactory iteratorFactory, Scan scan, boolean initOneScanPerRegion, Map<ImmutableBytesPtr,ServerCache> caches, QueryPlan dataPlan)
            throws SQLException {
        this(plan, perScanLimit, iteratorFactory, DefaultParallelScanGrouper.getInstance(), scan, initOneScanPerRegion, caches, dataPlan);
    }  

    /**
     * No need to use stats when executing serially
     */
    @Override
    protected boolean isSerial() {
        return false;
    }
    
    @Override
    protected void submitWork(final List<List<Scan>> nestedScans, List<List<Pair<Scan,Future<PeekingResultIterator>>>> nestedFutures,
            final Queue<PeekingResultIterator> allIterators, int estFlattenedSize, final boolean isReverse, ParallelScanGrouper scanGrouper) throws SQLException {
        // Pre-populate nestedFutures lists so that we can shuffle the scans
        // and add the future to the right nested list. By shuffling the scans
        // we get better utilization of the cluster since our thread executor
        // will spray the scans across machines as opposed to targeting a
        // single one since the scans are in row key order.
        ExecutorService executor = context.getConnection().getQueryServices().getExecutor();
        List<ScanLocator> scanLocations = Lists.newArrayListWithExpectedSize(estFlattenedSize);
        for (int i = 0; i < nestedScans.size(); i++) {
            List<Scan> scans = nestedScans.get(i);
            int numScans = scans.size();
            List<Pair<Scan,Future<PeekingResultIterator>>> futures = Lists.newArrayListWithExpectedSize(numScans);
            nestedFutures.add(futures);
            for (int j = 0; j < numScans; j++) {
            	Scan scan = nestedScans.get(i).get(j);
                scanLocations.add(new ScanLocator(scan, i, j, j == 0, (j == numScans - 1)));
                futures.add(null); // placeholder
            }
        }
        // Shuffle so that we start execution across many machines
        // before we fill up the thread pool
        Collections.shuffle(scanLocations);
        ReadMetricQueue readMetrics = context.getReadMetricsQueue();
        final String physicalTableName = tableRef.getTable().getPhysicalName().getString();
        int numScans = scanLocations.size();
        context.getOverallQueryMetrics().updateNumParallelScans(numScans);
        GLOBAL_NUM_PARALLEL_SCANS.update(numScans);
        final long renewLeaseThreshold = context.getConnection().getQueryServices().getRenewLeaseThresholdMilliSeconds();
        for (final ScanLocator scanLocation : scanLocations) {
            final Scan scan = scanLocation.getScan();
            final ScanMetricsHolder scanMetricsHolder = ScanMetricsHolder.getInstance(readMetrics, physicalTableName,
                scan, context.getConnection().getLogLevel());
            final TaskExecutionMetricsHolder taskMetrics = new TaskExecutionMetricsHolder(readMetrics, physicalTableName);
            final TableResultIterator tableResultItr =
                    context.getConnection().getTableResultIteratorFactory().newIterator(
                        mutationState, tableRef, scan, scanMetricsHolder, renewLeaseThreshold, plan,
                        scanGrouper, caches);
            context.getConnection().addIteratorForLeaseRenewal(tableResultItr);
            Future<PeekingResultIterator> future = executor.submit(Tracing.wrap(new JobCallable<PeekingResultIterator>() {
                
                @Override
                public PeekingResultIterator call() throws Exception {
                    long startTime = System.currentTimeMillis();
                    if (logger.isDebugEnabled()) {
                        logger.debug(LogUtil.addCustomAnnotations("Id: " + scanId + ", Time: " + (System.currentTimeMillis() - startTime) + "ms, Scan: " + scan, ScanUtil.getCustomAnnotations(scan)));
                    }
                    PeekingResultIterator iterator = iteratorFactory.newIterator(context, tableResultItr, scan, physicalTableName, ParallelIterators.this.plan);
                    if (initFirstScanOnly) {
                        if ((!isReverse && scanLocation.isFirstScan()) || (isReverse && scanLocation.isLastScan())) {
                            // Fill the scanner's cache. This helps reduce latency since we are parallelizing the I/O needed.
                            iterator.peek();
                        }
                    } else {
                        iterator.peek();
                    }
                    allIterators.add(iterator);
                    return iterator;
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

                @Override
                public TaskExecutionMetricsHolder getTaskExecutionMetric() {
                    return taskMetrics;
                }
            }, "Parallel scanner for table: " + tableRef.getTable().getPhysicalName().getString()));
            // Add our future in the right place so that we can concatenate the
            // results of the inner futures versus merge sorting across all of them.
            nestedFutures.get(scanLocation.getOuterListIndex()).set(scanLocation.getInnerListIndex(), new Pair<Scan,Future<PeekingResultIterator>>(scan,future));
        }
    }

    @Override
    protected String getName() {
        return NAME;
    }
}