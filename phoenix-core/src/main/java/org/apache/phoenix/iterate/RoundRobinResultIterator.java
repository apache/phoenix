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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_FAILED_QUERY_COUNTER;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

/**
 * ResultIterator that keeps track of the number of records fetched by each {@link PeekingResultIterator} making sure it
 * asks for records from each iterator in a round-robin fashion. When the iterators have fetched the scan cache size of
 * records, it submits the iterators to the thread pool to help parallelize the I/O needed to fetch the next batch of
 * records. This iterator assumes that the PeekingResultIterators that it manages are not nested i.e. they directly
 * manage the underlying scanners. This kind of ResultIterator should only be used when one doesn't care about the order
 * in which records are returned.
 */
public class RoundRobinResultIterator implements ResultIterator {

    private static final Logger logger = LoggerFactory.getLogger(RoundRobinResultIterator.class);

    private final int threshold;

    private int numScannersCacheExhausted = 0;
    private ResultIterators resultIterators;

    private List<RoundRobinIterator> openIterators = new ArrayList<>();

    private int index;
    private boolean closed;
    private final QueryPlan plan;

    // For testing purposes
    private int numParallelFetches;

    public RoundRobinResultIterator(ResultIterators iterators, QueryPlan plan) {
        this.resultIterators = iterators;
        this.plan = plan;
        this.threshold = getThreshold();
    }

    public RoundRobinResultIterator(List<PeekingResultIterator> iterators, QueryPlan plan) {
        this.resultIterators = null;
        this.plan = plan;
        this.threshold = getThreshold();
        initOpenIterators(wrapToRoundRobinIterators(iterators));
    }

    public static ResultIterator newIterator(final List<PeekingResultIterator> iterators, QueryPlan plan) {
        if (iterators.isEmpty()) { return EMPTY_ITERATOR; }
        if (iterators.size() == 1) { return iterators.get(0); }
        return new RoundRobinResultIterator(iterators, plan);
    }

    @Override
    public Tuple next() throws SQLException {
        List<RoundRobinIterator> iterators;
        int size;
        while ((size = (iterators = getIterators()).size()) > 0) {
            index = index % size;
            RoundRobinIterator itr = iterators.get(index);
            if (itr.getNumRecordsRead() < threshold) {
                Tuple tuple;
                if ((tuple = itr.peek()) != null) {
                    tuple = itr.next();
                    if (itr.getNumRecordsRead() == threshold) {
                        numScannersCacheExhausted++;
                    }
                    index = (index + 1) % size;
                    return tuple;
                } else {
                    // The underlying scanner is exhausted. Close the iterator and un-track it.
                    itr.close();
                    iterators.remove(index);
                    if (iterators.size() == 0) {
                        close();
                    }
                }
            } else {
                index = (index + 1) % size;
            }
        }
        return null;
    }

    @Override
    public void close() throws SQLException {
        if (closed) { return; }
        closed = true;
        SQLException toThrow = null;
        try {
            if (resultIterators != null) {
                resultIterators.close();
            }
        } catch (Exception e) {
            toThrow = ServerUtil.parseServerException(e);
        } finally {
            try {
                if (openIterators.size() > 0) {
                    for (RoundRobinIterator itr : openIterators) {
                        try {
                            itr.close();
                        } catch (Exception e) {
                            if (toThrow == null) {
                                toThrow = ServerUtil.parseServerException(e);
                            } else {
                                toThrow.setNextException(ServerUtil.parseServerException(e));
                            }
                        }
                    }
                }
            } finally {
                if (toThrow != null) { throw toThrow; }
            }
        }
    }

    @Override
    public void explain(List<String> planSteps) {
        if (resultIterators != null) {
            resultIterators.explain(planSteps);
        }
    }

    @VisibleForTesting
    int getNumberOfParallelFetches() {
        return numParallelFetches;
    }

    @VisibleForTesting
    QueryPlan getQueryPlan() {
        return plan;
    }

    private List<RoundRobinIterator> getIterators() throws SQLException {
        if (closed) { return Collections.emptyList(); }
        if (openIterators.size() > 0 && openIterators.size() == numScannersCacheExhausted) {
            /*
             * All the scanners have exhausted their cache. Submit the scanners back to the pool so that they can fetch
             * the next batch of records in parallel.
             */
            initOpenIterators(fetchNextBatch());
        } else if (openIterators.size() == 0 && resultIterators != null) {
            List<PeekingResultIterator> iterators = resultIterators.getIterators();
            initOpenIterators(wrapToRoundRobinIterators(iterators));
        }
        return openIterators;
    }

    private List<RoundRobinIterator> wrapToRoundRobinIterators(List<PeekingResultIterator> iterators) {
        List<RoundRobinIterator> roundRobinItrs = new ArrayList<>(iterators.size());
        for (PeekingResultIterator itr : iterators) {
            roundRobinItrs.add(new RoundRobinIterator(itr, null));
        }
        return roundRobinItrs;
    }

    private void initOpenIterators(List<RoundRobinIterator> iterators) {
        openIterators.clear();
        openIterators.addAll(iterators);
        index = 0;
        numScannersCacheExhausted = 0;
    }

    private int getThreshold() {
        int cacheSize = getScannerCacheSize();
        checkArgument(cacheSize > 1, "RoundRobinResultIterator doesn't work when cache size is less than or equal to 1");
        return cacheSize - 1;
    }

    private int getScannerCacheSize() {
        try {
            return plan.getContext().getStatement().getFetchSize();
        } catch (Throwable e) {
            Throwables.propagate(e);
        }
        return -1; // unreachable
    }

    private List<RoundRobinIterator> fetchNextBatch() throws SQLException {
        int numExpectedIterators = openIterators.size();
        List<Future<Tuple>> futures = new ArrayList<>(numExpectedIterators);
        List<RoundRobinIterator> results = new ArrayList<>();

        // Randomize the order in which we will be hitting region servers to try not overload particular region servers.
        Collections.shuffle(openIterators);
        boolean success = false;
        SQLException toThrow = null;
        try {
            StatementContext context = plan.getContext();
            final ConnectionQueryServices services = context.getConnection().getQueryServices();
            ExecutorService executor = services.getExecutor();
            numParallelFetches++;
            if (logger.isDebugEnabled()) {
                logger.debug("Performing parallel fetch for " + openIterators.size() + " iterators. ");
            }
            for (final RoundRobinIterator itr : openIterators) {
                Future<Tuple> future = executor.submit(new Callable<Tuple>() {
                    @Override
                    public Tuple call() throws Exception {
                        // Read the next record to refill the scanner's cache.
                        return itr.next();
                    }
                });
                futures.add(future);
            }
            int i = 0;
            for (Future<Tuple> future : futures) {
                Tuple tuple = future.get();
                if (tuple != null) {
                    results.add(new RoundRobinIterator(openIterators.get(i).delegate, tuple));
                } else {
                    // Underlying scanner is exhausted. So close it.
                    openIterators.get(i).close();
                }
                i++;
            }
            success = true;
            return results;
        } catch (SQLException e) {
            toThrow = e;
        } catch (Exception e) {
            toThrow = ServerUtil.parseServerException(e);
        } finally {
            try {
                if (!success) {
                    try {
                        close();
                    } catch (Exception e) {
                        if (toThrow == null) {
                            toThrow = ServerUtil.parseServerException(e);
                        } else {
                            toThrow.setNextException(ServerUtil.parseServerException(e));
                        }
                    }
                }
            } finally {
                if (toThrow != null) {
                    GLOBAL_FAILED_QUERY_COUNTER.increment();
                    throw toThrow;
                }
            }
        }
        return null; // Not reachable
    }

    /**
     * Inner class that delegates to {@link PeekingResultIterator} keeping track the number of records it has read. Also
     * keeps track of the tuple the {@link PeekingResultIterator} read in the previous next() call before it ran out of
     * underlying scanner cache.
     */
    private class RoundRobinIterator implements PeekingResultIterator {

        private PeekingResultIterator delegate;
        private Tuple tuple;
        private int numRecordsRead;

        private RoundRobinIterator(PeekingResultIterator itr, Tuple tuple) {
            this.delegate = itr;
            this.tuple = tuple;
            this.numRecordsRead = 0;
        }

        @Override
        public void close() throws SQLException {
            delegate.close();
        }

        @Override
        public Tuple next() throws SQLException {
            if (tuple != null) {
                Tuple t = tuple;
                tuple = null;
                return t;
            }
            numRecordsRead++;
            return delegate.next();
        }

        @Override
        public void explain(List<String> planSteps) {
            delegate.explain(planSteps);
        }

        @Override
        public Tuple peek() throws SQLException {
            if (tuple != null) { return tuple; }
            return delegate.peek();
        }

        public int getNumRecordsRead() {
            return numRecordsRead;
        }

    }

}
