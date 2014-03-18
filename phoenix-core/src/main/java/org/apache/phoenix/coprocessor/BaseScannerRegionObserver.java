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
package org.apache.phoenix.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.util.ServerUtil;


abstract public class BaseScannerRegionObserver extends BaseRegionObserver {
    
    public static final String AGGREGATORS = "_Aggs";
    public static final String UNORDERED_GROUP_BY_EXPRESSIONS = "_UnorderedGroupByExpressions";
    public static final String KEY_ORDERED_GROUP_BY_EXPRESSIONS = "_OrderedGroupByExpressions";
    public static final String ESTIMATED_DISTINCT_VALUES = "_EstDistinctValues";
    public static final String NON_AGGREGATE_QUERY = "_NonAggregateQuery";
    public static final String TOPN = "_TopN";
    public static final String UNGROUPED_AGG = "_UngroupedAgg";
    public static final String DELETE_AGG = "_DeleteAgg";
    public static final String UPSERT_SELECT_TABLE = "_UpsertSelectTable";
    public static final String UPSERT_SELECT_EXPRS = "_UpsertSelectExprs";
    public static final String DELETE_CQ = "_DeleteCQ";
    public static final String DELETE_CF = "_DeleteCF";
    public static final String EMPTY_CF = "_EmptyCF";
    public static final String SPECIFIC_ARRAY_INDEX = "_SpecificArrayIndex";
    public static final String GROUP_BY_LIMIT = "_GroupByLimit";

    /**
     * Used by logger to identify coprocessor
     */
    @Override
    public String toString() {
        return this.getClass().getName();
    }
    
    abstract protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable;
    
    /**
     * Wrapper for {@link #postScannerOpen(ObserverContext, Scan, RegionScanner)} that ensures no non IOException is thrown,
     * to prevent the coprocessor from becoming blacklisted.
     * 
     */
    @Override
    public final RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws IOException {
        try {
            return doPostScannerOpen(c, scan, s);
        } catch (Throwable t) {
            ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
            return null; // impossible
        }
    }
}
