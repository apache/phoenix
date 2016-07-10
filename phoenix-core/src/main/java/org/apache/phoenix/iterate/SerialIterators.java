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
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.monitoring.TaskExecutionMetricsHolder;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.QueryUtil;

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
    private final Integer offset;
    
    public SerialIterators(QueryPlan plan, Integer perScanLimit, Integer offset,
            ParallelIteratorFactory iteratorFactory, ParallelScanGrouper scanGrouper, Scan scan)
            throws SQLException {
        super(plan, perScanLimit, offset, scanGrouper, scan);
        this.offset = offset;
        // must be a offset or a limit specified or a SERIAL hint
        Preconditions.checkArgument(
                offset != null || perScanLimit != null || plan.getStatement().getHint().hasHint(HintNode.Hint.SERIAL));
        this.iteratorFactory = iteratorFactory;
    }

    @Override
    protected void submitWork(final List<List<Scan>> nestedScans, List<List<Pair<Scan,Future<PeekingResultIterator>>>> nestedFutures,
            final Queue<PeekingResultIterator> allIterators, int estFlattenedSize, boolean isReverse, final ParallelScanGrouper scanGrouper) {
        ExecutorService executor = context.getConnection().getQueryServices().getExecutor();
        final String tableName = tableRef.getTable().getPhysicalName().getString();
        final TaskExecutionMetricsHolder taskMetrics = new TaskExecutionMetricsHolder(context.getReadMetricsQueue(), tableName);
        final PhoenixConnection conn = context.getConnection();
        final long renewLeaseThreshold = conn.getQueryServices().getRenewLeaseThresholdMilliSeconds();
        int expectedListSize = nestedScans.size() * 10;
        List<Scan> flattenedScans = Lists.newArrayListWithExpectedSize(expectedListSize);
        for (List<Scan> list : nestedScans) {
            flattenedScans.addAll(list);
        }
        if (!flattenedScans.isEmpty()) { 
            if (isReverse) {
                flattenedScans = Lists.reverse(flattenedScans);
            }
            final List<Scan> finalScans = flattenedScans;
            Future<PeekingResultIterator> future = executor.submit(Tracing.wrap(new JobCallable<PeekingResultIterator>() {
                @Override
                public PeekingResultIterator call() throws Exception {
                    PeekingResultIterator itr = new SerialIterator(finalScans, tableName, renewLeaseThreshold, offset);
                    return itr;
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
            nestedFutures.add(Collections.singletonList(new Pair<Scan, Future<PeekingResultIterator>>(flattenedScans.get(0), future)));
        }
    }

    /**
     * No need to use stats when executing serially
     */
    @Override
    protected boolean useStats() {
        return false;
    }
    
    @Override
    protected String getName() {
        return NAME;
    }
    
    /**
     * 
     * Iterator that creates iterators for scans only when needed.
     * This helps reduce the cost of pre-constructing all the iterators
     * which we may not even use.
     */
    private class SerialIterator implements PeekingResultIterator {
        private final List<Scan> scans;
        private final String tableName;
        private final long renewLeaseThreshold;
        private int index;
        private PeekingResultIterator currentIterator;
        private Integer remainingOffset;
        
        private SerialIterator(List<Scan> flattenedScans, String tableName, long renewLeaseThreshold, Integer offset) throws SQLException {
            this.scans = Lists.newArrayListWithExpectedSize(flattenedScans.size());
            this.tableName = tableName;
            this.renewLeaseThreshold = renewLeaseThreshold;
            this.scans.addAll(flattenedScans);
            this.remainingOffset = offset;
            if (this.remainingOffset != null) {
                // mark the last scan for offset purposes
                this.scans.get(this.scans.size() - 1).setAttribute(QueryConstants.LAST_SCAN, Bytes.toBytes(Boolean.TRUE));
            }
        }
        
        private PeekingResultIterator currentIterator() throws SQLException {
            if (currentIterator == null) {
                return currentIterator = nextIterator();
            }
            if (currentIterator.peek() == null) {
                currentIterator.close();
                currentIterator = nextIterator();
            }
            return currentIterator;
        }
        
        private PeekingResultIterator nextIterator() throws SQLException {
            if (index >= scans.size()) {
                return EMPTY_ITERATOR;
            }
            while (index < scans.size()) {
                Scan currentScan = scans.get(index++);
                if (remainingOffset != null) {
                    currentScan.setAttribute(BaseScannerRegionObserver.SCAN_OFFSET, PInteger.INSTANCE.toBytes(remainingOffset));
                }
                TableResultIterator itr = new TableResultIterator(mutationState, currentScan, context.getReadMetricsQueue().allotMetric(SCAN_BYTES, tableName), renewLeaseThreshold, plan, scanGrouper);
                PeekingResultIterator peekingItr = iteratorFactory.newIterator(context, itr, currentScan, tableName, plan);
                Tuple tuple;
                if ((tuple = peekingItr.peek()) == null) {
                    peekingItr.close();
                    continue;
                } else if ((remainingOffset = QueryUtil.getRemainingOffset(tuple)) != null) {
                    peekingItr.next();
                    peekingItr.close();
                    continue;
                }
                context.getConnection().addIteratorForLeaseRenewal(itr);
                return peekingItr;
            }
            return EMPTY_ITERATOR;
        }
        
        @Override
        public Tuple next() throws SQLException {
            return currentIterator().next();
        }

        @Override
        public void explain(List<String> planSteps) {}

        @Override
        public void close() throws SQLException {
            if (currentIterator != null) {
                currentIterator.close();
            }
        }

        @Override
        public Tuple peek() throws SQLException {
            return currentIterator().peek();
        }
        
    }
}