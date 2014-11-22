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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.TableResultIterator.ScannerCreation;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger logger = LoggerFactory.getLogger(SerialIterators.class);
	private static final String NAME = "SERIAL";
    private final ParallelIteratorFactory iteratorFactory;
    
    public SerialIterators(QueryPlan plan, Integer perScanLimit, ParallelIteratorFactory iteratorFactory)
            throws SQLException {
        super(plan, perScanLimit);
        Preconditions.checkArgument(perScanLimit != null); // must be a limit specified
        this.iteratorFactory = iteratorFactory;
    }

    @Override
    protected void submitWork(List<List<Scan>> nestedScans, List<List<Pair<Scan,Future<PeekingResultIterator>>>> nestedFutures,
            final List<PeekingResultIterator> allIterators, int estFlattenedSize) {
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
            Future<PeekingResultIterator> future = executor.submit(Tracing.wrap(new JobCallable<PeekingResultIterator>() {

                @Override
                public PeekingResultIterator call() throws Exception {
                	List<PeekingResultIterator> concatIterators = Lists.newArrayListWithExpectedSize(scans.size());
                	for (final Scan scan : scans) {
	                    long startTime = System.currentTimeMillis();
	                    ResultIterator scanner = new TableResultIterator(context, tableRef, scan, ScannerCreation.DELAYED);
	                    if (logger.isDebugEnabled()) {
	                        logger.debug(LogUtil.addCustomAnnotations("Id: " + scanId + ", Time: " + (System.currentTimeMillis() - startTime) + "ms, Scan: " + scan, ScanUtil.getCustomAnnotations(scan)));
	                    }
	                    concatIterators.add(iteratorFactory.newIterator(context, scanner, scan));
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
            }, "Serial scanner for table: " + tableRef.getTable().getName().getString()));
            // Add our singleton Future which will execute serially
            nestedFutures.add(Collections.singletonList(new Pair<Scan,Future<PeekingResultIterator>>(overallScan,future)));
        }
    }

    @Override
    protected String getName() {
        return NAME;
    }
}