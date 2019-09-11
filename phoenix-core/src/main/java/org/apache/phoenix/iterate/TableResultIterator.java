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

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SCAN_ACTUAL_START_ROW;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SCAN_START_ROW_SUFFIX;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.CLOSED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.LOCK_NOT_ACQUIRED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.NOT_RENEWED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.NOT_SUPPORTED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.RENEWED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.THRESHOLD_NOT_REACHED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.UNINITIALIZED;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.HashJoinCacheNotFoundException;
import org.apache.phoenix.execute.BaseQueryPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.join.HashCacheClient;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;


/**
 *
 * Wrapper for ResultScanner creation that closes HTableInterface
 * when ResultScanner is closed.
 *
 * 
 * @since 0.1
 */
public class TableResultIterator implements ResultIterator {
    private final Scan scan;
    private final Table htable;
    private final ScanMetricsHolder scanMetricsHolder;
    private static final ResultIterator UNINITIALIZED_SCANNER = ResultIterator.EMPTY_ITERATOR;
    private final long renewLeaseThreshold;
    private final QueryPlan plan;
    private final ParallelScanGrouper scanGrouper;
    private static final Logger LOGGER = LoggerFactory.getLogger(TableResultIterator.class);
    private Tuple lastTuple = null;
    private ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    @GuardedBy("renewLeaseLock")
    private ResultIterator scanIterator;

    @GuardedBy("renewLeaseLock")
    private boolean closed = false;

    @GuardedBy("renewLeaseLock")
    private long renewLeaseTime = 0;

    private final Lock renewLeaseLock = new ReentrantLock();

    private int retry;
    private Map<ImmutableBytesPtr,ServerCache> caches;
    private HashCacheClient hashCacheClient;

    @VisibleForTesting // Exposed for testing. DON'T USE ANYWHERE ELSE!
    TableResultIterator() {
        this.scanMetricsHolder = null;
        this.renewLeaseThreshold = 0;
        this.htable = null;
        this.scan = null;
        this.plan = null;
        this.scanGrouper = null;
        this.caches = null;
        this.retry = 0;
    }

    public static enum RenewLeaseStatus {
        RENEWED, NOT_RENEWED, CLOSED, UNINITIALIZED, THRESHOLD_NOT_REACHED, LOCK_NOT_ACQUIRED, NOT_SUPPORTED
    };

    public TableResultIterator(MutationState mutationState, Scan scan, ScanMetricsHolder scanMetricsHolder,
            long renewLeaseThreshold, QueryPlan plan, ParallelScanGrouper scanGrouper) throws SQLException {
        this(mutationState, scan, scanMetricsHolder, renewLeaseThreshold, plan, scanGrouper, null);
    }

    public TableResultIterator(MutationState mutationState, Scan scan, ScanMetricsHolder scanMetricsHolder,
            long renewLeaseThreshold, QueryPlan plan, ParallelScanGrouper scanGrouper,Map<ImmutableBytesPtr,ServerCache> caches) throws SQLException {
        this.scan = scan;
        this.scanMetricsHolder = scanMetricsHolder;
        this.plan = plan;
        PTable table = plan.getTableRef().getTable();
        htable = mutationState.getHTable(table);
        this.scanIterator = UNINITIALIZED_SCANNER;
        this.renewLeaseThreshold = renewLeaseThreshold;
        this.scanGrouper = scanGrouper;
        this.hashCacheClient = new HashCacheClient(plan.getContext().getConnection());
        this.caches = caches;
        this.retry=plan.getContext().getConnection().getQueryServices().getProps()
                .getInt(QueryConstants.HASH_JOIN_CACHE_RETRIES, QueryConstants.DEFAULT_HASH_JOIN_CACHE_RETRIES);
        IndexUtil.setScanAttributesForIndexReadRepair(scan, table, plan.getContext().getConnection());
    }

    @Override
    public void close() throws SQLException {
        try {
            renewLeaseLock.lock();
            closed = true; // ok to say closed even if the below code throws an exception
            try {
                scanIterator.close();
            } finally {
                try {
                    scanIterator = UNINITIALIZED_SCANNER;
                    htable.close();
                } catch (IOException e) {
                    throw ServerUtil.parseServerException(e);
                }
            }
        } finally {
            renewLeaseLock.unlock();
        }

    }

    @Override
    public Tuple next() throws SQLException {
        try {
            renewLeaseLock.lock();
            initScanner();
            try {
                lastTuple = scanIterator.next();
                if (lastTuple != null) {
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                    lastTuple.getKey(ptr);
                }
            } catch (SQLException e) {
                try {
                    throw ServerUtil.parseServerException(e);
                } catch(HashJoinCacheNotFoundException e1) {
                    if(ScanUtil.isNonAggregateScan(scan) && plan.getContext().getAggregationManager().isEmpty()) {
                        // For non aggregate queries if we get stale region boundary exception we can
                        // continue scanning from the next value of lasted fetched result.
                        Scan newScan = ScanUtil.newScan(scan);
                        newScan.withStartRow(newScan.getAttribute(SCAN_ACTUAL_START_ROW));
                        if(lastTuple != null) {
                            lastTuple.getKey(ptr);
                            byte[] startRowSuffix = ByteUtil.copyKeyBytesIfNecessary(ptr);
                            if(ScanUtil.isLocalIndex(newScan)) {
                                // If we just set scan start row suffix then server side we prepare
                                // actual scan boundaries by prefixing the region start key.
                                newScan.setAttribute(SCAN_START_ROW_SUFFIX, ByteUtil.nextKey(startRowSuffix));
                            } else {
                                newScan.withStartRow(ByteUtil.nextKey(startRowSuffix));
                            }
                        }
                        plan.getContext().getConnection().getQueryServices().clearTableRegionCache(htable.getName());
                        LOGGER.debug(
                                "Retrying when Hash Join cache is not found on the server ,by sending the cache again");
                        if (retry <= 0) {
                            throw e1;
                        }
                        Long cacheId = e1.getCacheId();
                        retry--;
                        try {
                            ServerCache cache = caches == null ? null :
                                    caches.get(new ImmutableBytesPtr(Bytes.toBytes(cacheId)));
                            if (!hashCacheClient.addHashCacheToServer(newScan.getStartRow(),
                                    cache, plan.getTableRef().getTable())) {
                                throw e1;
                            }
                            this.scanIterator = ((BaseQueryPlan) plan).iterator(caches, scanGrouper, newScan);

                        } catch (Exception ex) {
                            throw ServerUtil.parseServerException(ex);
                        }
                        lastTuple = scanIterator.next();
                    } else {
                        throw e;
                    }
                }
            }
            return lastTuple;
        } finally {
            renewLeaseLock.unlock();
        }
    }

    public void initScanner() throws SQLException {
        try {
            renewLeaseLock.lock();
            if (closed) {
                return;
            }
            ResultIterator delegate = this.scanIterator;
            if (delegate == UNINITIALIZED_SCANNER) {
                try {
                    this.scanIterator =
                            new ScanningResultIterator(htable.getScanner(scan), scan, scanMetricsHolder);
                } catch (IOException e) {
                    Closeables.closeQuietly(htable);
                    throw ServerUtil.parseServerException(e);
                }
            }
        } finally {
            renewLeaseLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "TableResultIterator [htable=" + htable + ", scan=" + scan  + "]";
    }

    public RenewLeaseStatus renewLease() {
        boolean lockAcquired = false;
        try {
            lockAcquired = renewLeaseLock.tryLock();
            if (lockAcquired) {
                if (closed) {
                    return CLOSED;
                }
                if (scanIterator == UNINITIALIZED_SCANNER) {
                    return UNINITIALIZED;
                }
                long delay = now() - renewLeaseTime;
                if (delay < renewLeaseThreshold) {
                    return THRESHOLD_NOT_REACHED;
                }
                if (scanIterator instanceof ScanningResultIterator
                        && ((ScanningResultIterator)scanIterator).getScanner() instanceof AbstractClientScanner) {
                    // Need this explicit cast because HBase's ResultScanner doesn't have this method exposed.
                    boolean leaseRenewed = ((AbstractClientScanner)((ScanningResultIterator)scanIterator).getScanner()).renewLease();
                    if (leaseRenewed) {
                        renewLeaseTime = now();
                        return RENEWED;
                    } else {
                        return NOT_RENEWED;
                    }
                } else {
                    return NOT_SUPPORTED;
                }
            }
            return LOCK_NOT_ACQUIRED;
        } 
        finally {
            if (lockAcquired) {
                renewLeaseLock.unlock();
            }
        }
    }

    private static long now() {
        return EnvironmentEdgeManager.currentTimeMillis();
    }

    @Override
    public void explain(List<String> planSteps) {
        scanIterator.explain(planSteps);
    }

}
