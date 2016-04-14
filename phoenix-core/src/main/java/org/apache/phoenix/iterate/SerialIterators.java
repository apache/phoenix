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

import static org.apache.phoenix.monitoring.MetricType.SCAN_BYTES;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.monitoring.TaskExecutionMetricsHolder;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 *
 * Class that parallelizes the scan over a table using the ExecutorService provided.  Each region of the table will be scanned in parallel with
 * the results accessible through {@link #getIterators()}
 *
 * 
 * @since 0.1
 */
public class SerialIterators extends BaseResultIterators {
	private static final String NAME = "SERIAL";
    private final ParallelIteratorFactory iteratorFactory;
    
    public SerialIterators(QueryPlan plan, Integer perScanLimit, Integer offset,
            ParallelIteratorFactory iteratorFactory, ParallelScanGrouper scanGrouper)
            throws SQLException {
        super(plan, perScanLimit, offset, scanGrouper);
        // must be a offset or a limit specified or a SERIAL hint
        Preconditions.checkArgument(
                offset != null || perScanLimit != null || plan.getStatement().getHint().hasHint(HintNode.Hint.SERIAL));
        this.iteratorFactory = iteratorFactory;
    }

    @Override
    protected void submitWork(List<List<Scan>> nestedScans, List<List<Pair<Scan,Future<PeekingResultIterator>>>> nestedFutures,
            final Queue<PeekingResultIterator> allIterators, int estFlattenedSize) {
        // Pre-populate nestedFutures lists so that we can shuffle the scans
        // and add the future to the right nested list. By shuffling the scans
        // we get better utilization of the cluster since our thread executor
        // will spray the scans across machines as opposed to targeting a
        // single one since the scans are in row key order.
        ExecutorService executor = context.getConnection().getQueryServices().getExecutor();
        
        for (final List<Scan> scans : nestedScans) {
            Scan firstScan = scans.get(0);
            Scan lastScan = scans.get(scans.size()-1);
            final Scan overallScan = ScanUtil.newScan(firstScan);
            overallScan.setStopRow(lastScan.getStopRow());
            final String tableName = tableRef.getTable().getPhysicalName().getString();
            final TaskExecutionMetricsHolder taskMetrics = new TaskExecutionMetricsHolder(context.getReadMetricsQueue(), tableName);
            final PhoenixConnection conn = context.getConnection();
            final long renewLeaseThreshold = conn.getQueryServices().getRenewLeaseThresholdMilliSeconds();
            lastScan.setAttribute(QueryConstants.LAST_SCAN, Bytes.toBytes(Boolean.TRUE));
            Future<PeekingResultIterator> future = executor.submit(Tracing.wrap(new JobCallable<PeekingResultIterator>() {
                @Override
                public PeekingResultIterator call() throws Exception {
                    PeekingResultIterator previousIterator = null;
                	List<PeekingResultIterator> concatIterators = Lists.newArrayListWithExpectedSize(scans.size());
                	for (final Scan scan : scans) {
                	    TableResultIterator scanner = new TableResultIterator(mutationState, tableRef, scan, context.getReadMetricsQueue().allotMetric(SCAN_BYTES, tableName), renewLeaseThreshold, previousIterator);
                	    conn.addIterator(scanner);
                	    PeekingResultIterator iterator = iteratorFactory.newIterator(context, scanner, scan, tableName);
                	    concatIterators.add(iterator);
                	    previousIterator = iterator;
                	}
                	PeekingResultIterator concatIterator = ConcatResultIterator.newIterator(concatIterators);
                    allIterators.add(concatIterator);
                    return concatIterator;
                }

                /**
                 * Defines the grouping for round robin behavior.  All threads spawned to process
                 * this scan will be grouped together and time sliced with other simultaneously
                 * executing parallel scans.
                 */
                @Override
                public Object getJobId() {
                    return SerialIterators.this;
                }

                @Override
                public TaskExecutionMetricsHolder getTaskExecutionMetric() {
                    return taskMetrics;
                }
            }, "Serial scanner for table: " + tableRef.getTable().getPhysicalName().getString()));
            // Add our singleton Future which will execute serially
            nestedFutures.add(Collections.singletonList(new Pair<Scan,Future<PeekingResultIterator>>(overallScan,future)));
        }
    }

    @Override
    protected String getName() {
        return NAME;
    }
}