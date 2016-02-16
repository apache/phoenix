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

import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.CLOSED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.NOT_RENEWED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.RENEWED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.THRESHOLD_NOT_REACHED;
import static org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus.UNINITIALIZED;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.monitoring.CombinableMetric;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.ServerUtil;

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
    private final HTableInterface htable;
    private final CombinableMetric scanMetrics;
    private static final ResultIterator UNINITIALIZED_SCANNER = ResultIterator.EMPTY_ITERATOR;
    private final long renewLeaseThreshold;

    @GuardedBy("this")
    private ResultIterator scanIterator;

    @GuardedBy("this")
    private boolean closed = false;

    @GuardedBy("this")
    private long renewLeaseTime = 0;

    @VisibleForTesting // Exposed for testing. DON'T USE ANYWHERE ELSE!
    TableResultIterator() {
        this.scanMetrics = null;
        this.renewLeaseThreshold = 0;
        this.htable = null;
        this.scan = null;
    }

    public static enum RenewLeaseStatus {
        RENEWED, CLOSED, UNINITIALIZED, THRESHOLD_NOT_REACHED, NOT_RENEWED
    };


    public TableResultIterator(MutationState mutationState, TableRef tableRef, Scan scan, CombinableMetric scanMetrics, long renewLeaseThreshold) throws SQLException {
        this.scan = scan;
        this.scanMetrics = scanMetrics;
        PTable table = tableRef.getTable();
        htable = mutationState.getHTable(table);
        this.scanIterator = UNINITIALIZED_SCANNER;
        this.renewLeaseThreshold = renewLeaseThreshold;
    }

    @Override
    public synchronized void close() throws SQLException {
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
    }
    
    @Override
    public synchronized Tuple next() throws SQLException {
        initScanner();
        Tuple t = scanIterator.next();
        return t;
    }

    public synchronized void initScanner() throws SQLException {
        if (closed) {
            return;
        }
        ResultIterator delegate = this.scanIterator;
        if (delegate == UNINITIALIZED_SCANNER) {
            try {
                this.scanIterator =
                        new ScanningResultIterator(htable.getScanner(scan), scanMetrics);
            } catch (IOException e) {
                Closeables.closeQuietly(htable);
                throw ServerUtil.parseServerException(e);
            }
        }
    }

    @Override
    public String toString() {
        return "TableResultIterator [htable=" + htable + ", scan=" + scan  + "]";
    }

    public synchronized RenewLeaseStatus renewLease() {
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
            }
        }
        return NOT_RENEWED;
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    @Override
    public void explain(List<String> planSteps) {
        scanIterator.explain(planSteps);
    }

}
