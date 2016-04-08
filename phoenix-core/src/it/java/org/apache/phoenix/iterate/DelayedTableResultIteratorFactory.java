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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.monitoring.CombinableMetric;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;

public class DelayedTableResultIteratorFactory implements TableResultIteratorFactory {
    
    private final long delay;
    
    public DelayedTableResultIteratorFactory(long delay) {
        this.delay = delay;
    }
    
    @Override
    public TableResultIterator newIterator(MutationState mutationState, TableRef tableRef, Scan scan,
            CombinableMetric scanMetrics, long renewLeaseThreshold, QueryPlan plan, ParallelScanGrouper scanGrouper) throws SQLException {
        return new DelayedTableResultIterator(mutationState, tableRef, scan, scanMetrics, renewLeaseThreshold, plan, scanGrouper);
    }
    
    private class DelayedTableResultIterator extends TableResultIterator {
        public DelayedTableResultIterator (MutationState mutationState, TableRef tableRef, Scan scan, CombinableMetric scanMetrics, long renewLeaseThreshold, QueryPlan plan, ParallelScanGrouper scanGrouper) throws SQLException {
            super(mutationState, scan, scanMetrics, renewLeaseThreshold, plan, scanGrouper);
        }
        
        @Override
        public synchronized void initScanner() throws SQLException {
            super.initScanner();
        }
        
        @Override
        public Tuple next() throws SQLException {
            delay();
            return super.next();
        }
        
        private void delay() throws SQLException {
            try {
                new CountDownLatch(1).await(delay, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e).build().buildException();
            }
        }
        
        
    }

}
