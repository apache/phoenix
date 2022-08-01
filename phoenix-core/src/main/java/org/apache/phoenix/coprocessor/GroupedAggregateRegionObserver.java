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

import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryServices.GROUPBY_ESTIMATED_DISTINCT_VALUES_ATTRIB;
import static org.apache.phoenix.query.QueryServices.GROUPBY_SPILLABLE_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_GROUPBY_ESTIMATED_DISTINCT_VALUES;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_GROUPBY_SPILLABLE;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.cache.aggcache.SpillableGroupByCache;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.EncodedColumnQualiferCellsList;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.PositionBasedMultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.TupleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Region observer that aggregates grouped rows (i.e. SQL query with GROUP BY clause)
 *
 * @since 0.1
 */
public class GroupedAggregateRegionObserver extends BaseScannerRegionObserver implements RegionCoprocessor {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(GroupedAggregateRegionObserver.class);
    public static final int MIN_DISTINCT_VALUES = 100;
    
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }


    /**
     * Replaces the RegionScanner s with a RegionScanner that groups by the key formed by the list
     * of expressions from the scan and returns the aggregated rows of each group. For example,
     * given the following original rows in the RegionScanner: KEY COL1 row1 a row2 b row3 a row4 a
     * the following rows will be returned for COUNT(*): KEY COUNT a 3 b 1 The client is required to
     * do a sort and a final aggregation, since multiple rows with the same key may be returned from
     * different regions.
     */
    @Override
    protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Scan scan, RegionScanner s) throws IOException {
        boolean keyOrdered = false;
        byte[] expressionBytes = scan.getAttribute(BaseScannerRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS);

        if (expressionBytes == null) {
            expressionBytes = scan.getAttribute(BaseScannerRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS);
            keyOrdered = true;
        }
        int offset = 0;
        boolean useNewValueColumnQualifier = EncodedColumnsUtil.useNewValueColumnQualifier(scan);
        if (ScanUtil.isLocalIndex(scan)) {
            /*
             * For local indexes, we need to set an offset on row key expressions to skip
             * the region start key.
             */
            Region region = c.getEnvironment().getRegion();
            offset = region.getRegionInfo().getStartKey().length != 0 ? region.getRegionInfo().getStartKey().length :
                region.getRegionInfo().getEndKey().length;
            ScanUtil.setRowKeyOffset(scan, offset);
        }

        List<Expression> expressions = deserializeGroupByExpressions(expressionBytes, 0);
        final TenantCache tenantCache = GlobalCache.getTenantCache(c.getEnvironment(), ScanUtil.getTenantId(scan));
        try (MemoryChunk em = tenantCache.getMemoryManager().allocate(0)) {
            ServerAggregators aggregators =
                    ServerAggregators.deserialize(scan
                            .getAttribute(BaseScannerRegionObserver.AGGREGATORS), c
                            .getEnvironment().getConfiguration(), em);
    
            RegionScanner innerScanner = s;
            List<IndexMaintainer> indexMaintainers =
                    IndexUtil.deSerializeIndexMaintainersFromScan(scan);
            TupleProjector tupleProjector = null;
            byte[][] viewConstants = null;
            ColumnReference[] dataColumns = IndexUtil.deserializeDataTableColumnsToJoin(scan);
    
            final TupleProjector p = TupleProjector.deserializeProjectorFromScan(scan);
            final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
            boolean useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan));
            if (ScanUtil.isLocalOrUncoveredGlobalIndex(scan)
                    || (j == null && p != null)) {
                if (dataColumns != null) {
                    tupleProjector = IndexUtil.getTupleProjector(scan, dataColumns);
                    viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
                }
                ImmutableBytesPtr tempPtr = new ImmutableBytesPtr();
                innerScanner =
                        getWrappedScanner(c, innerScanner, offset, scan, dataColumns, tupleProjector, 
                                c.getEnvironment().getRegion(), indexMaintainers == null ? null : indexMaintainers.get(0), viewConstants, p, tempPtr, useQualifierAsIndex);
            } 
    
            if (j != null) {
                innerScanner =
                        new HashJoinRegionScanner(innerScanner, scan, p, j, ScanUtil.getTenantId(scan),
                                c.getEnvironment(), useQualifierAsIndex, useNewValueColumnQualifier);
            }
    
            long limit = Long.MAX_VALUE;
            byte[] limitBytes = scan.getAttribute(GROUP_BY_LIMIT);
            if (limitBytes != null) {
                limit = PInteger.INSTANCE.getCodec().decodeInt(limitBytes, 0, SortOrder.getDefault());
            }
            long pageSizeMs = getPageSizeMsForRegionScanner(scan);
            if (keyOrdered) { // Optimize by taking advantage that the rows are
                              // already in the required group by key order
                return new OrderedGroupByRegionScanner(c, scan, innerScanner, expressions, aggregators, limit, pageSizeMs);
            } else { // Otherwse, collect them all up in an in memory map
                return new UnorderedGroupByRegionScanner(c, scan, innerScanner, expressions, aggregators, limit, pageSizeMs);
            }
        }
    }

    public static long sizeOfUnorderedGroupByMap(int nRows, int valueSize) {
        return SizedUtil.sizeOfMap(nRows, SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE, valueSize);
    }

    public static void serializeIntoScan(Scan scan, String attribName,
            List<Expression> groupByExpressions) {
        ByteArrayOutputStream stream =
                new ByteArrayOutputStream(Math.max(1, groupByExpressions.size() * 10));
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

    private List<Expression> deserializeGroupByExpressions(byte[] expressionBytes, int offset)
            throws IOException {
        List<Expression> expressions = new ArrayList<Expression>(3);
        ByteArrayInputStream stream = new ByteArrayInputStream(expressionBytes);
        try {
            DataInputStream input = new DataInputStream(stream);
            while (true) {
                try {
                    int expressionOrdinal = WritableUtils.readVInt(input);
                    Expression expression =
                            ExpressionType.values()[expressionOrdinal].newInstance();
                    expression.readFields(input);
                    if (offset != 0) {
                        IndexUtil.setRowKeyExpressionOffset(expression, offset);
                    }
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
     *
     * Cache for distinct values and their aggregations which is completely
     * in-memory (as opposed to spilling to disk). Used when GROUPBY_SPILLABLE_ATTRIB
     * is set to false. The memory usage is tracked at a coursed grain and will
     * throw and abort if too much is used.
     *
     *
     * @since 3.0.0
     */
    private static final class InMemoryGroupByCache implements GroupByCache {
        private final MemoryChunk chunk;
        private final Map<ImmutableBytesPtr, Aggregator[]> aggregateMap;
        private final ServerAggregators aggregators;
        private final RegionCoprocessorEnvironment env;
        private final byte[] customAnnotations;

        private int estDistVals;

        InMemoryGroupByCache(RegionCoprocessorEnvironment env, ImmutableBytesPtr tenantId, byte[] customAnnotations, ServerAggregators aggregators, int estDistVals) {
            int estValueSize = aggregators.getEstimatedByteSize();
            long estSize = sizeOfUnorderedGroupByMap(estDistVals, estValueSize);
            TenantCache tenantCache = GlobalCache.getTenantCache(env, tenantId);
            this.env = env;
            this.estDistVals = estDistVals;
            this.aggregators = aggregators;
            this.aggregateMap = Maps.newHashMapWithExpectedSize(estDistVals);
            this.chunk = tenantCache.getMemoryManager().allocate(estSize);
            this.customAnnotations = customAnnotations;
        }

        @Override
        public void close() throws IOException {
            this.chunk.close();
        }

        @Override
        public Aggregator[] cache(ImmutableBytesPtr cacheKey) {
            ImmutableBytesPtr key = new ImmutableBytesPtr(cacheKey);
            Aggregator[] rowAggregators = aggregateMap.get(key);
            if (rowAggregators == null) {
                // If Aggregators not found for this distinct
                // value, clone our original one (we need one
                // per distinct value)
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(LogUtil.addCustomAnnotations("Adding new aggregate bucket for row key "
                            + Bytes.toStringBinary(key.get(), key.getOffset(),
                                key.getLength()), customAnnotations));
                }
                rowAggregators =
                        aggregators.newAggregators(env.getConfiguration());
                aggregateMap.put(key, rowAggregators);

                if (aggregateMap.size() > estDistVals) { // increase allocation
                    estDistVals *= 1.5f;
                    long estSize = sizeOfUnorderedGroupByMap(estDistVals, aggregators.getEstimatedByteSize());
                    chunk.resize(estSize);
                }
            }
            return rowAggregators;
        }

        @Override
        public RegionScanner getScanner(final RegionScanner s) {
            // Compute final allocation
            long estSize = sizeOfUnorderedGroupByMap(aggregateMap.size(), aggregators.getEstimatedByteSize());
            chunk.resize(estSize);

            final List<Cell> aggResults = new ArrayList<Cell>(aggregateMap.size());

            final Iterator<Map.Entry<ImmutableBytesPtr, Aggregator[]>> cacheIter =
                    aggregateMap.entrySet().iterator();
            while (cacheIter.hasNext()) {
                Map.Entry<ImmutableBytesPtr, Aggregator[]> entry = cacheIter.next();
                ImmutableBytesPtr key = entry.getKey();
                Aggregator[] rowAggregators = entry.getValue();
                // Generate byte array of Aggregators and set as value of row
                byte[] value = aggregators.toBytes(rowAggregators);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(LogUtil.addCustomAnnotations("Adding new distinct group: "
                            + Bytes.toStringBinary(key.get(), key.getOffset(), key.getLength())
                            + " with aggregators " + Arrays.asList(rowAggregators).toString()
                            + " value = " + Bytes.toStringBinary(value), customAnnotations));
                }
                Cell keyValue =
                        PhoenixKeyValueUtil.newKeyValue(key.get(), key.getOffset(), key.getLength(),
                            SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0,
                            value.length);
                aggResults.add(keyValue);
            }
            // scanner using the non spillable, memory-only implementation
            return new BaseRegionScanner(s) {
                private int index = 0;

                @Override
                public void close() throws IOException {
                    try {
                        s.close();
                    } finally {
                        InMemoryGroupByCache.this.close();
                    }
                }

                @Override
                public boolean next(List<Cell> results) throws IOException {
                    if (index >= aggResults.size()) {
                        return false;
                    }
                    results.add(aggResults.get(index));
                    index++;
                    return index < aggResults.size();
                }
            };
        }

        @Override
        public long size() {
            return aggregateMap.size();
        }

    }

    private static final class GroupByCacheFactory {
        public static final GroupByCacheFactory INSTANCE = new GroupByCacheFactory();

        private GroupByCacheFactory() {
        }

        GroupByCache newCache(RegionCoprocessorEnvironment env, ImmutableBytesPtr tenantId, byte[] customAnnotations, ServerAggregators aggregators, int estDistVals) {
            Configuration conf = env.getConfiguration();
            boolean spillableEnabled =
                    conf.getBoolean(GROUPBY_SPILLABLE_ATTRIB, DEFAULT_GROUPBY_SPILLABLE);
            if (spillableEnabled) {
                return new SpillableGroupByCache(env, tenantId, aggregators, estDistVals);
            }

            return new InMemoryGroupByCache(env, tenantId, customAnnotations, aggregators, estDistVals);
        }
    }

    /**
     * Used for an aggregate query in which the key order does not necessarily match the group by
     * key order. In this case, we must collect all distinct groups within a region into a map,
     * aggregating as we go.
     */
    private static class UnorderedGroupByRegionScanner extends BaseRegionScanner {
        private final Region region;
        private final Pair<Integer, Integer> minMaxQualifiers;
        private final boolean useQualifierAsIndex;
        private final PTable.QualifierEncodingScheme encodingScheme;
        private final ServerAggregators aggregators;
        private final long limit;
        private final List<Expression> expressions;
        private final long pageSizeMs;
        private RegionScanner regionScanner = null;
        private final GroupByCache groupByCache;

        private UnorderedGroupByRegionScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
                                              final Scan scan, final RegionScanner scanner, final List<Expression> expressions,
                                              final ServerAggregators aggregators, final long limit, final long pageSizeMs) {
            super(scanner);
            this.region = c.getEnvironment().getRegion();
            this.aggregators = aggregators;
            this.limit = limit;
            this.pageSizeMs = pageSizeMs;
            this.expressions = expressions;
            RegionCoprocessorEnvironment env = c.getEnvironment();
            Configuration conf = env.getConfiguration();
            int estDistVals = conf.getInt(GROUPBY_ESTIMATED_DISTINCT_VALUES_ATTRIB, DEFAULT_GROUPBY_ESTIMATED_DISTINCT_VALUES);
            byte[] estDistValsBytes = scan.getAttribute(BaseScannerRegionObserver.ESTIMATED_DISTINCT_VALUES);
            if (estDistValsBytes != null) {
                // Allocate 1.5x estimation
                estDistVals = Math.max(MIN_DISTINCT_VALUES,
                        (int) (Bytes.toInt(estDistValsBytes) * 1.5f));
            }

            minMaxQualifiers = EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan);
            useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan));
            final boolean spillableEnabled = conf.getBoolean(GROUPBY_SPILLABLE_ATTRIB, DEFAULT_GROUPBY_SPILLABLE);
            encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);

            groupByCache = GroupByCacheFactory.INSTANCE.newCache(
                    env, ScanUtil.getTenantId(scan), ScanUtil.getCustomAnnotations(scan), aggregators, estDistVals);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(LogUtil.addCustomAnnotations(
                        "Grouped aggregation over unordered rows with scan " + scan
                                + ", group by " + expressions + ", aggregators " + aggregators,
                        ScanUtil.getCustomAnnotations(scan)));
                LOGGER.debug(LogUtil.addCustomAnnotations(
                        "Spillable groupby enabled: " + spillableEnabled,
                        ScanUtil.getCustomAnnotations(scan)));
            }
        }

        @Override
        public boolean next(List<Cell> resultsToReturn) throws IOException {
            boolean hasMore;
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            long now;
            Tuple result = useQualifierAsIndex ? new PositionBasedMultiKeyValueTuple() : new MultiKeyValueTuple();
            boolean acquiredLock = false;
            try {
                region.startRegionOperation();
                acquiredLock = true;
                synchronized (delegate) {
                    if (regionScanner != null) {
                        return regionScanner.next(resultsToReturn);
                    }
                    do {
                        List<Cell> results = useQualifierAsIndex ?
                                new EncodedColumnQualiferCellsList(minMaxQualifiers.getFirst(),
                                        minMaxQualifiers.getSecond(), encodingScheme) :
                                new ArrayList<Cell>();
                        // Results are potentially returned even when the return
                        // value of s.next is false
                        // since this is an indication of whether or not there are
                        // more values after the
                        // ones returned
                        hasMore = delegate.nextRaw(results);
                        if (!results.isEmpty()) {
                            if (isDummy(results)) {
                                getDummyResult(resultsToReturn);
                                return true;
                            }
                            result.setKeyValues(results);
                            ImmutableBytesPtr key =
                                    TupleUtil.getConcatenatedValue(result, expressions);
                            Aggregator[] rowAggregators = groupByCache.cache(key);
                            // Aggregate values here
                            aggregators.aggregate(rowAggregators, result);
                        }
                        now = EnvironmentEdgeManager.currentTimeMillis();
                    } while (hasMore && groupByCache.size() < limit && (now - startTime) < pageSizeMs);
                    if (hasMore && groupByCache.size() < limit && (now - startTime) >= pageSizeMs) {
                        // Return a dummy result as we have processed a page worth of rows
                        // but we are not ready to aggregate
                        getDummyResult(resultsToReturn);
                        return true;
                    }
                    regionScanner = groupByCache.getScanner(delegate);
                    // Do not sort here, but sort back on the client instead
                    // The reason is that if the scan ever extends beyond a region
                    // (which can happen if we're basing our parallelization split
                    // points on old metadata), we'll get incorrect query results.
                    return regionScanner.next(resultsToReturn);
                }
            } finally {
                if (acquiredLock) region.closeRegionOperation();
            }
        }

        @Override
        public void close() throws IOException {
            if (regionScanner != null) {
                regionScanner.close();
            } else {
                Closeables.closeQuietly(groupByCache);
            }
        }
    }

    /**
     * Used for an aggregate query in which the key order match the group by key order. In this
     * case, we can do the aggregation as we scan, by detecting when the group by key changes.
     */
    private static class OrderedGroupByRegionScanner extends BaseRegionScanner {
        private final Scan scan;
        private final Region region;
        private final Pair<Integer, Integer> minMaxQualifiers;
        private final boolean useQualifierAsIndex;
        private final PTable.QualifierEncodingScheme encodingScheme;
        private final ServerAggregators aggregators;
        private final long limit;
        private final List<Expression> expressions;
        private final long pageSizeMs;
        private long rowCount = 0;
        private ImmutableBytesPtr currentKey = null;

        private OrderedGroupByRegionScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
                                            final Scan scan, final RegionScanner scanner, final List<Expression> expressions,
                                            final ServerAggregators aggregators, final long limit, final long pageSizeMs) {
            super(scanner);
            this.scan = scan;
            this.aggregators = aggregators;
            this.limit = limit;
            this.pageSizeMs = pageSizeMs;
            this.expressions = expressions;
            region = c.getEnvironment().getRegion();
            minMaxQualifiers = EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan);
            useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(minMaxQualifiers);
            encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(LogUtil.addCustomAnnotations(
                        "Grouped aggregation over ordered rows with scan " + scan + ", group by "
                                + expressions + ", aggregators " + aggregators,
                        ScanUtil.getCustomAnnotations(scan)));
            }
        }
        @Override
        public boolean next(List<Cell> results) throws IOException {
            boolean hasMore;
            boolean atLimit;
            boolean aggBoundary = false;
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            long now;
            Tuple result = useQualifierAsIndex ? new PositionBasedMultiKeyValueTuple() : new MultiKeyValueTuple();
            ImmutableBytesPtr key = null;
            Aggregator[] rowAggregators = aggregators.getAggregators();
            // If we're calculating no aggregate functions, we can exit at the
            // start of a new row. Otherwise, we have to wait until an agg
            int countOffset = rowAggregators.length == 0 ? 1 : 0;
            boolean acquiredLock = false;
            try {
                region.startRegionOperation();
                acquiredLock = true;
                synchronized (delegate) {
                    do {
                        List<Cell> kvs = useQualifierAsIndex ?
                                new EncodedColumnQualiferCellsList(minMaxQualifiers.getFirst(),
                                        minMaxQualifiers.getSecond(), encodingScheme) :
                                new ArrayList<Cell>();
                        // Results are potentially returned even when the return
                        // value of s.next is false
                        // since this is an indication of whether or not there
                        // are more values after the
                        // ones returned
                        hasMore = delegate.nextRaw(kvs);
                        if (!kvs.isEmpty()) {
                            if (isDummy(kvs)) {
                                getDummyResult(results);
                                return true;
                            }
                            result.setKeyValues(kvs);
                            key = TupleUtil.getConcatenatedValue(result, expressions);
                            aggBoundary = currentKey != null && currentKey.compareTo(key) != 0;
                            if (!aggBoundary) {
                                aggregators.aggregate(rowAggregators, result);
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(LogUtil.addCustomAnnotations(
                                            "Row passed filters: " + kvs
                                                    + ", aggregated values: "
                                                    + Arrays.asList(rowAggregators),
                                            ScanUtil.getCustomAnnotations(scan)));
                                }
                                currentKey = key;
                            }
                        }
                        atLimit = rowCount + countOffset >= limit;
                        // Do rowCount + 1 b/c we don't have to wait for a complete
                        // row in the case of a DISTINCT with a LIMIT
                        now = EnvironmentEdgeManager.currentTimeMillis();
                    } while (hasMore && !aggBoundary && !atLimit && (now - startTime) < pageSizeMs);
                }
            } finally {
                if (acquiredLock) region.closeRegionOperation();
            }
            if (hasMore && !aggBoundary && !atLimit && (now - startTime) >= pageSizeMs) {
                // Return a dummy result as we have processed a page worth of rows
                // but we are not ready to aggregate
                getDummyResult(results);
                return true;
            }
            if (currentKey != null) {
                byte[] value = aggregators.toBytes(rowAggregators);
                Cell keyValue =
                        PhoenixKeyValueUtil.newKeyValue(currentKey.get(), currentKey.getOffset(),
                                currentKey.getLength(), SINGLE_COLUMN_FAMILY, SINGLE_COLUMN,
                                AGG_TIMESTAMP, value, 0, value.length);
                results.add(keyValue);
                // If we're at an aggregation boundary, reset the
                // aggregators and
                // aggregate with the current result (which is not a part of
                // the returned result).
                if (aggBoundary) {
                    aggregators.reset(rowAggregators);
                    aggregators.aggregate(rowAggregators, result);
                    currentKey = key;
                    rowCount++;
                    atLimit |= rowCount >= limit;
                }
            }
            // Continue if there are more
            if (!atLimit && (hasMore || aggBoundary)) {
                return true;
            }
            currentKey = null;
            return false;
        }
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS) != null ||
               scan.getAttribute(BaseScannerRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS) != null;
    }
}
