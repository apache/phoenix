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

package org.apache.phoenix.jdbc;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.monitoring.MetricType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static org.apache.phoenix.exception.SQLExceptionCode.CLASS_NOT_UNWRAPPABLE;

/**
 * ParallelPhoenixResultSet that provides the standard wait until at least one cluster completes approach
 */
public class ParallelPhoenixResultSet extends DelegateResultSet implements PhoenixMonitoredResultSet {

    private final ParallelPhoenixContext context;
    private final CompletableFuture<ResultSet> rs1, rs2;

    public ParallelPhoenixResultSet(ParallelPhoenixContext context, CompletableFuture<ResultSet> rs1, CompletableFuture<ResultSet> rs2) {
        super(null);
        this.context = context;
        this.rs = null;
        this.rs1 = rs1;
        this.rs2 = rs2;
    }

    @Override
    public boolean next() throws SQLException {
        context.checkOpen();
        //As this starts iterating through a result set after we have a winner we bind to a single thread
        if(rs == null) {

            Function<ResultSet, Boolean> function = (T) -> {
                try {
                     return T.next();
                } catch (SQLException exception) {
                    throw new CompletionException(exception);
                }
            };

            List<CompletableFuture<Boolean>> futures = ParallelPhoenixUtil.INSTANCE.applyFunctionToFutures(function, rs1,
                    rs2, context, false);

            Preconditions.checkState(futures.size() == 2);
            CompletableFuture<Boolean> next1 = futures.get(0);
            CompletableFuture<Boolean> next2 = futures.get(1);

            //Ensure one statement is successful before returning
            ParallelPhoenixUtil.INSTANCE.runFutures(futures, context, true);

            try {
                if(next1.isDone() && !next1.isCompletedExceptionally()) {
                    rs = rs1.get();
                    return next1.get();
                } else { //(next2.isDone() && !next2.isCompletedExceptionally())
                    rs = rs2.get();
                    return next2.get();
                }
            } catch (Exception e) {
                    //should never happen
                throw new SQLException("Unknown Error happened while processing initial next.",e);
            }

        } else {
            return rs.next();
        }
    }

    @VisibleForTesting
    CompletableFuture<ResultSet> getResultSetFuture1() {
        return rs1;
    }

    @VisibleForTesting
    CompletableFuture<ResultSet> getResultSetFuture2() {
        return rs2;
    }

    @VisibleForTesting
    void setResultSet(ResultSet rs) {
        this.rs = rs;
    }

    @VisibleForTesting
    ResultSet getResultSet() {
        return rs;
    }

    @Override
    public Map<String, Map<MetricType, Long>> getReadMetrics() {
        Map<String, Map<MetricType, Long>> metrics;
        if(rs != null) {
            metrics = ((PhoenixMonitoredResultSet) rs).getReadMetrics();
        } else {
            metrics = new HashMap<>();
        }
        context.decorateMetrics(metrics);
        return metrics;
    }

    @Override
    public Map<MetricType, Long> getOverAllRequestReadMetrics() {
        Map<MetricType, Long> metrics;
        if(rs != null) {
            metrics = ((PhoenixResultSet) rs).getOverAllRequestReadMetrics();
        } else {
            metrics = context.getContextMetrics();
        }
        return metrics;
    }

    @Override
    public void resetMetrics() {
        if(rs != null) {
            ((PhoenixResultSet)rs).resetMetrics();
        }
        //reset our metrics
        context.resetMetrics();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        throw new SQLExceptionInfo.Builder(CLASS_NOT_UNWRAPPABLE).build().buildException();
    }
}
