/*
 * Copyright 2014 The Apache Software Foundation
 *
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

import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.TupleUtil;



/**
 * Region observer that aggregates grouped rows (i.e. SQL query with GROUP BY clause)
 * 
 * 
 * @since 0.1
 */
public class GroupedAggregateRegionObserver extends BaseScannerRegionObserver {
    private static final Logger logger = LoggerFactory.getLogger(GroupedAggregateRegionObserver.class);
    
    public static final String AGGREGATORS = "Aggs";
    public static final String UNORDERED_GROUP_BY_EXPRESSIONS = "UnorderedGroupByExpressions";
    public static final String KEY_ORDERED_GROUP_BY_EXPRESSIONS = "OrderedGroupByExpressions";

    public static final String ESTIMATED_DISTINCT_VALUES = "EstDistinctValues";
    public static final int DEFAULT_ESTIMATED_DISTINCT_VALUES = 10000;
    public static final int MIN_DISTINCT_VALUES = 100;
    

    /**
     * Replaces the RegionScanner s with a RegionScanner that groups by the key formed by the list of expressions from the scan
     * and returns the aggregated rows of each group.  For example, given the following original rows in the RegionScanner:
     * KEY    COL1
     * row1   a
     * row2   b
     * row3   a
     * row4   a
     * 
     * the following rows will be returned for COUNT(*):
     * KEY    COUNT
     * a      3
     * b      1
     *
     * The client is required to do a sort and a final aggregation, since multiple rows with the same key may be returned from different regions.
     */
    @Override
    protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
        boolean keyOrdered = false;
        byte[] expressionBytes = scan.getAttribute(UNORDERED_GROUP_BY_EXPRESSIONS);

        if (expressionBytes == null) {
            expressionBytes = scan.getAttribute(KEY_ORDERED_GROUP_BY_EXPRESSIONS);
            if (expressionBytes == null) {
                return s;
            }
            keyOrdered = true;
        }
        List<Expression> expressions = deserializeGroupByExpressions(expressionBytes);
        
        ServerAggregators aggregators = ServerAggregators.deserialize(
                scan.getAttribute(GroupedAggregateRegionObserver.AGGREGATORS), c.getEnvironment().getConfiguration());

        final ScanProjector p = ScanProjector.deserializeProjectorFromScan(scan);
        final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);        
        RegionScanner innerScanner = s;
        if (p != null || j != null) {
            innerScanner = new HashJoinRegionScanner(s, p, j, ScanUtil.getTenantId(scan), c.getEnvironment());
        }
        
        if (keyOrdered) { // Optimize by taking advantage that the rows are already in the required group by key order
            return scanOrdered(c, scan, innerScanner, expressions, aggregators);
        } else { // Otherwse, collect them all up in an in memory map
            return scanUnordered(c, scan, innerScanner, expressions, aggregators);
        }
    }

    private static int sizeOfUnorderedGroupByMap(int nRows, int valueSize) {
        return SizedUtil.sizeOfMap(nRows, SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE, valueSize);
    }

    public static void serializeIntoScan(Scan scan, String attribName, List<Expression> groupByExpressions) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(Math.max(1, groupByExpressions.size() * 10));
        try {
            if (groupByExpressions.isEmpty()) { // FIXME ?
                stream.write(QueryConstants.TRUE);
            } else {
                DataOutputStream output = new DataOutputStream(stream);
                for (Expression expression : groupByExpressions) {
                    WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
                    expression.write(output);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        scan.setAttribute(attribName, stream.toByteArray());

    }

    private List<Expression> deserializeGroupByExpressions(byte[] expressionBytes) throws IOException {
        List<Expression> expressions = new ArrayList<Expression>(3);
        ByteArrayInputStream stream = new ByteArrayInputStream(expressionBytes);
        try {
            DataInputStream input = new DataInputStream(stream);
            while (true) {
                try {
                    int expressionOrdinal = WritableUtils.readVInt(input);
                    Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
                    expression.readFields(input);
                    expressions.add(expression);
                } catch (EOFException e) {
                    break;
                }
            }
        } finally {
            stream.close();
        }
        return expressions;
    }
    
    /**
     * Used for an aggregate query in which the key order does not necessarily match the group by key order. In this case,
     * we must collect all distinct groups within a region into a map, aggregating as we go.
     */
    private RegionScanner scanUnordered(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, final RegionScanner s, List<Expression> expressions, ServerAggregators aggregators) throws IOException {
        
        if (logger.isDebugEnabled()) {
            logger.debug("Grouped aggregation over unordered rows with scan " + scan + ", group by " + expressions + ", aggregators " + aggregators);
        }
        int estDistVals = DEFAULT_ESTIMATED_DISTINCT_VALUES;
        byte[] estDistValsBytes = scan.getAttribute(ESTIMATED_DISTINCT_VALUES);
        if (estDistValsBytes != null) {
            estDistVals = Math.min(MIN_DISTINCT_VALUES, (int)(Bytes.toInt(estDistValsBytes) * 1.5f));  // Allocate 1.5x estimation
        }
        
        TenantCache tenantCache = GlobalCache.getTenantCache(c.getEnvironment(), ScanUtil.getTenantId(scan));
        int estSize = sizeOfUnorderedGroupByMap(estDistVals, aggregators.getSize());
        final MemoryChunk chunk = tenantCache.getMemoryManager().allocate(estSize);
        boolean success = false;
        try {
            // TODO: spool map to disk if map becomes too big
            boolean hasMore;
            int estValueSize = aggregators.getSize();
            MultiKeyValueTuple result = new MultiKeyValueTuple();
            Map<ImmutableBytesWritable, Aggregator[]> aggregateMap = new HashMap<ImmutableBytesWritable, Aggregator[]>(estDistVals);
            HRegion region = c.getEnvironment().getRegion();
            MultiVersionConsistencyControl.setThreadReadPoint(s.getMvccReadPoint());
            region.startRegionOperation();
            try {
                do {
                    List<KeyValue> results = new ArrayList<KeyValue>();
                    // Results are potentially returned even when the return value of s.next is false
                    // since this is an indication of whether or not there are more values after the
                    // ones returned
                    hasMore = s.nextRaw(results, null);
                    if (!results.isEmpty()) {
                        result.setKeyValues(results);
                        ImmutableBytesWritable key = TupleUtil.getConcatenatedValue(result, expressions);
                        Aggregator[] rowAggregators = aggregateMap.get(key);
                        if (rowAggregators == null) {
                            // If Aggregators not found for this distinct value, clone our original one (we need one per distinct value)
                            if (logger.isDebugEnabled()) {
                                logger.debug("Adding new aggregate bucket for row key " + Bytes.toStringBinary(key.get(),key.getOffset(),key.getLength()));
                            }
                            rowAggregators = aggregators.newAggregators(c.getEnvironment().getConfiguration());
                            aggregateMap.put(key, rowAggregators);
                        }
                        // Aggregate values here
                        aggregators.aggregate(rowAggregators, result);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Row passed filters: " + results + ", aggregated values: " + Arrays.asList(rowAggregators));
                        }
                            
                        if (aggregateMap.size() > estDistVals) { // increase allocation
                            estDistVals *= 1.5f;
                            estSize = sizeOfUnorderedGroupByMap(estDistVals, estValueSize);
                            chunk.resize(estSize);
                        }
                    }
                } while (hasMore);
            } finally {
                region.closeRegionOperation();
            }
    
            // Compute final allocation
            estSize = sizeOfUnorderedGroupByMap(aggregateMap.size(), estValueSize);
            chunk.resize(estSize);
            
            // TODO: spool list to disk if too big and free memory?
            final List<KeyValue> aggResults = new ArrayList<KeyValue>(aggregateMap.size());
            for (Map.Entry<ImmutableBytesWritable, Aggregator[]> entry : aggregateMap.entrySet()) {
                ImmutableBytesWritable key = entry.getKey();
                Aggregator[] rowAggregators = entry.getValue();
                // Generate byte array of Aggregators and set as value of row
                byte[] value = aggregators.toBytes(rowAggregators);
                
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new distinct group: " + Bytes.toStringBinary(key.get(),key.getOffset(), key.getLength()) + 
                            " with aggregators " + Arrays.asList(rowAggregators).toString() + 
                            " value = " + Bytes.toStringBinary(value));
                }
                KeyValue keyValue = KeyValueUtil.newKeyValue(key.get(),key.getOffset(), key.getLength(),SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length);
                aggResults.add(keyValue);
            }
            // Do not sort here, but sort back on the client instead
            // The reason is that if the scan ever extends beyond a region (which can happen
            // if we're basing our parallelization split points on old metadata), we'll get
            // incorrect query results.
            RegionScanner scanner = new BaseRegionScanner() {
                private int index = 0;
    
                @Override
                public HRegionInfo getRegionInfo() {
                    return s.getRegionInfo();
                }
    
                @Override
                public void close() throws IOException {
                    try {
                        s.close();
                    } finally {
                        chunk.close();
                    }
                }
    
                @Override
                public boolean next(List<KeyValue> results) throws IOException {
                    if (index >= aggResults.size()) return false;
                    results.add(aggResults.get(index));
                    index++;
                    return index < aggResults.size();
                }
            };
            success = true;
            return scanner;
        } finally {
            if (!success)
                chunk.close();
        }
    }

    /**
     * Used for an aggregate query in which the key order match the group by key order. In this case, we can do the
     * aggregation as we scan, by detecting when the group by key changes.
     */
    private RegionScanner scanOrdered(final ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, final RegionScanner s, final List<Expression> expressions, final ServerAggregators aggregators) {
        
        if (logger.isDebugEnabled()) {
            logger.debug("Grouped aggregation over ordered rows with scan " + scan + ", group by " + expressions + ", aggregators " + aggregators);
        }
        return new BaseRegionScanner() {
            private ImmutableBytesWritable currentKey = null;

            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public void close() throws IOException {
                s.close();
            }

            @Override
            public boolean next(List<KeyValue> results) throws IOException {
                boolean hasMore;
                boolean aggBoundary = false;
                MultiKeyValueTuple result = new MultiKeyValueTuple();
                ImmutableBytesWritable key = null;
                Aggregator[] rowAggregators = aggregators.getAggregators();
                HRegion region = c.getEnvironment().getRegion();
                MultiVersionConsistencyControl.setThreadReadPoint(s.getMvccReadPoint());
                region.startRegionOperation();
                try {
                    do {
                        List<KeyValue> kvs = new ArrayList<KeyValue>();
                        // Results are potentially returned even when the return value of s.next is false
                        // since this is an indication of whether or not there are more values after the
                        // ones returned
                        hasMore = s.nextRaw(kvs, null);
                        if (!kvs.isEmpty()) {
                            result.setKeyValues(kvs);
                            key = TupleUtil.getConcatenatedValue(result, expressions);
                            aggBoundary = currentKey != null && currentKey.compareTo(key) != 0;
                            if (!aggBoundary) {
                                aggregators.aggregate(rowAggregators, result);
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Row passed filters: " + kvs + ", aggregated values: " + Arrays.asList(rowAggregators));
                                }
                                currentKey = key;
                            }
                        }
                    } while (hasMore && !aggBoundary);
                } finally {
                    region.closeRegionOperation();
                }
                
                if (currentKey != null) {
                    byte[] value = aggregators.toBytes(rowAggregators);
                    KeyValue keyValue = KeyValueUtil.newKeyValue(currentKey.get(),currentKey.getOffset(), currentKey.getLength(),SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length);
                    results.add(keyValue);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Adding new aggregate row: " + keyValue + ",for current key " + Bytes.toStringBinary(currentKey.get(),currentKey.getOffset(), currentKey.getLength()) + ", aggregated values: " + Arrays.asList(rowAggregators));
                    }
                    // If we're at an aggregation boundary, reset the aggregators and
                    // aggregate with the current result (which is not a part of the returned result).
                    if (aggBoundary) {
                        aggregators.reset(rowAggregators);
                        aggregators.aggregate(rowAggregators, result);
                        currentKey = key;
                    }
                }
                // Continue if there are more
                if (hasMore || aggBoundary) {
                    return true;
                }
                currentKey = null;
                return false;
            }
        };
    }
}
