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
import static org.apache.phoenix.query.QueryConstants.GROUPED_AGGREGATOR_VALUE_BYTES;
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
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.cache.aggcache.SpillableGroupByCache;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
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
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.EncodedColumnQualiferCellsList;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.PositionBasedMultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
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
        byte[] expressionBytes = scan.getAttribute(BaseScannerRegionObserverConstants.UNORDERED_GROUP_BY_EXPRESSIONS);

        if (expressionBytes == null) {
            expressionBytes = scan.getAttribute(BaseScannerRegionObserverConstants.KEY_ORDERED_GROUP_BY_EXPRESSIONS);
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
                            .getAttribute(BaseScannerRegionObserverConstants.AGGREGATORS), c
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
            byte[] limitBytes = scan.getAttribute(BaseScannerRegionObserverConstants.GROUP_BY_LIMIT);
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
        private final ConcurrentMap<ImmutableBytesWritable, ImmutableBytesWritable>
                aggregateValueToLastScannedRowKeys;
        private final boolean isIncompatibleClient;

        private int estDistVals;

        InMemoryGroupByCache(RegionCoprocessorEnvironment env, ImmutableBytesPtr tenantId,
                             byte[] customAnnotations, ServerAggregators aggregators,
                             int estDistVals,
                             boolean isIncompatibleClient) {
            this.isIncompatibleClient = isIncompatibleClient;
            int estValueSize = aggregators.getEstimatedByteSize();
            long estSize = sizeOfUnorderedGroupByMap(estDistVals, estValueSize);
            TenantCache tenantCache = GlobalCache.getTenantCache(env, tenantId);
            this.env = env;
            this.estDistVals = estDistVals;
            this.aggregators = aggregators;
            this.aggregateMap = Maps.newHashMapWithExpectedSize(estDistVals);
            this.chunk = tenantCache.getMemoryManager().allocate(estSize);
            this.customAnnotations = customAnnotations;
            aggregateValueToLastScannedRowKeys = Maps.newConcurrentMap();
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

            for (Map.Entry<ImmutableBytesPtr, Aggregator[]> entry : aggregateMap.entrySet()) {
                ImmutableBytesWritable aggregateGroupValPtr = entry.getKey();
                Aggregator[] rowAggregators = entry.getValue();
                // Generate byte array of Aggregators and set as value of row
                byte[] aggregateArrayBytes = aggregators.toBytes(rowAggregators);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(LogUtil.addCustomAnnotations("Adding new distinct group: "
                            + Bytes.toStringBinary(aggregateGroupValPtr.get(),
                            aggregateGroupValPtr.getOffset(), aggregateGroupValPtr.getLength())
                            + " with aggregators " + Arrays.asList(rowAggregators) + " value = "
                            + Bytes.toStringBinary(aggregateArrayBytes), customAnnotations));
                }
                if (!isIncompatibleClient) {
                    ImmutableBytesWritable lastScannedRowKey =
                            aggregateValueToLastScannedRowKeys.get(aggregateGroupValPtr);
                    byte[] aggregateGroupValueBytes = new byte[aggregateGroupValPtr.getLength()];
                    System.arraycopy(aggregateGroupValPtr.get(), aggregateGroupValPtr.getOffset(),
                            aggregateGroupValueBytes, 0,
                            aggregateGroupValueBytes.length);
                    byte[] finalValue =
                            ByteUtil.concat(
                                    PInteger.INSTANCE.toBytes(aggregateGroupValueBytes.length),
                                    aggregateGroupValueBytes, aggregateArrayBytes);
                    aggResults.add(
                            PhoenixKeyValueUtil.newKeyValue(
                                    lastScannedRowKey.get(),
                                    lastScannedRowKey.getOffset(),
                                    lastScannedRowKey.getLength(),
                                    GROUPED_AGGREGATOR_VALUE_BYTES,
                                    GROUPED_AGGREGATOR_VALUE_BYTES,
                                    AGG_TIMESTAMP,
                                    finalValue,
                                    0,
                                    finalValue.length));
                } else {
                    aggResults.add(
                            PhoenixKeyValueUtil.newKeyValue(
                                    aggregateGroupValPtr.get(),
                                    aggregateGroupValPtr.getOffset(),
                                    aggregateGroupValPtr.getLength(),
                                    SINGLE_COLUMN_FAMILY,
                                    SINGLE_COLUMN,
                                    AGG_TIMESTAMP,
                                    aggregateArrayBytes,
                                    0,
                                    aggregateArrayBytes.length));
                }
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
        public void cacheAggregateRowKey(ImmutableBytesPtr value, ImmutableBytesPtr rowKey) {
            aggregateValueToLastScannedRowKeys.put(value, rowKey);
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

        GroupByCache newCache(RegionCoprocessorEnvironment env,
                              ImmutableBytesPtr tenantId,
                              byte[] customAnnotations,
                              ServerAggregators aggregators,
                              int estDistVals,
                              boolean isIncompatibleClient) {
            Configuration conf = env.getConfiguration();
            boolean spillableEnabled =
                    conf.getBoolean(GROUPBY_SPILLABLE_ATTRIB, DEFAULT_GROUPBY_SPILLABLE);
            if (spillableEnabled) {
                return new SpillableGroupByCache(env, tenantId, aggregators, estDistVals,
                        isIncompatibleClient);
            }
            return new InMemoryGroupByCache(env, tenantId, customAnnotations, aggregators,
                    estDistVals, isIncompatibleClient);
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
        private final Scan scan;
        private final byte[] scanStartRowKey;
        private final boolean includeStartRowKey;
        private final byte[] actualScanStartRowKey;
        private final boolean actualScanIncludeStartRowKey;
        private boolean firstScan = true;
        private boolean skipValidRowsSent = false;
        private byte[] lastReturnedRowKey = null;

        private UnorderedGroupByRegionScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
                                              final Scan scan, final RegionScanner scanner, final List<Expression> expressions,
                                              final ServerAggregators aggregators, final long limit, final long pageSizeMs) {
            super(scanner);
            this.region = c.getEnvironment().getRegion();
            this.scan = scan;
            scanStartRowKey =
                    ServerUtil.getScanStartRowKeyFromScanOrRegionBoundaries(scan, region);
            includeStartRowKey = scan.includeStartRow();
            // Retrieve start rowkey of the previous scan. This would be different than
            // current scan start rowkey if the region has recently moved or split or merged.
            this.actualScanStartRowKey =
                    scan.getAttribute(BaseScannerRegionObserverConstants.SCAN_ACTUAL_START_ROW);
            this.actualScanIncludeStartRowKey = true;
            this.aggregators = aggregators;
            this.limit = limit;
            this.pageSizeMs = pageSizeMs;
            this.expressions = expressions;
            RegionCoprocessorEnvironment env = c.getEnvironment();
            Configuration conf = env.getConfiguration();
            int estDistVals = conf.getInt(GROUPBY_ESTIMATED_DISTINCT_VALUES_ATTRIB, DEFAULT_GROUPBY_ESTIMATED_DISTINCT_VALUES);
            byte[] estDistValsBytes = scan.getAttribute(BaseScannerRegionObserverConstants.ESTIMATED_DISTINCT_VALUES);
            if (estDistValsBytes != null) {
                // Allocate 1.5x estimation
                estDistVals = Math.max(MIN_DISTINCT_VALUES,
                        (int) (Bytes.toInt(estDistValsBytes) * 1.5f));
            }

            minMaxQualifiers = EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan);
            useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan));
            final boolean spillableEnabled = conf.getBoolean(GROUPBY_SPILLABLE_ATTRIB, DEFAULT_GROUPBY_SPILLABLE);
            encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
            final boolean isIncompatibleClient =
                    ScanUtil.isIncompatibleClientForServerReturnValidRowKey(scan);
            groupByCache = GroupByCacheFactory.INSTANCE.newCache(
                    env, ScanUtil.getTenantId(scan), ScanUtil.getCustomAnnotations(scan),
                    aggregators, estDistVals, isIncompatibleClient);
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
            if (firstScan && actualScanStartRowKey != null) {
                if (scanStartRowKey.length > 0 && !ScanUtil.isLocalIndex(scan)) {
                    if (hasRegionMoved()) {
                        LOGGER.info("Region has moved.. Actual scan start rowkey {} is not same "
                                        + "as current scan start rowkey {}",
                                Bytes.toStringBinary(actualScanStartRowKey),
                                Bytes.toStringBinary(scanStartRowKey));
                        // If region has moved in the middle of the scan operation, after resetting
                        // the scanner, hbase client uses (latest received rowkey + \x00) as new
                        // start rowkey for resuming the scan operation on the new scanner.
                        if (Bytes.compareTo(
                                ByteUtil.concat(actualScanStartRowKey, ByteUtil.ZERO_BYTE),
                                scanStartRowKey) == 0) {
                            scan.setAttribute(QueryServices.PHOENIX_PAGING_NEW_SCAN_START_ROWKEY,
                                    actualScanStartRowKey);
                            scan.setAttribute(
                                    QueryServices.PHOENIX_PAGING_NEW_SCAN_START_ROWKEY_INCLUDE,
                                    Bytes.toBytes(actualScanIncludeStartRowKey));
                        } else {
                            // This happens when the server side scanner has already sent some
                            // rows back to the client and region has moved, so now we need to
                            // use skipValidRowsSent flag and also reset the scanner
                            // at paging region scanner level to re-read the previously sent
                            // values in order to re-compute the aggregation and then return
                            // only the next rowkey that was not yet sent back to the client.
                            skipValidRowsSent = true;
                            scan.setAttribute(QueryServices.PHOENIX_PAGING_NEW_SCAN_START_ROWKEY,
                                    actualScanStartRowKey);
                            scan.setAttribute(
                                    QueryServices.PHOENIX_PAGING_NEW_SCAN_START_ROWKEY_INCLUDE,
                                    Bytes.toBytes(actualScanIncludeStartRowKey));
                        }
                    }
                }
            }
            if (firstScan) {
                firstScan = false;
            }
            boolean moreRows = nextInternal(resultsToReturn);
            if (ScanUtil.isDummy(resultsToReturn)) {
                return true;
            }
            if (skipValidRowsSent) {
                while (true) {
                    if (!moreRows) {
                        skipValidRowsSent = false;
                        if (resultsToReturn.size() > 0) {
                            lastReturnedRowKey = CellUtil.cloneRow(resultsToReturn.get(0));
                        }
                        return moreRows;
                    }
                    Cell firstCell = resultsToReturn.get(0);
                    byte[] resultRowKey = new byte[firstCell.getRowLength()];
                    System.arraycopy(firstCell.getRowArray(), firstCell.getRowOffset(),
                            resultRowKey, 0, resultRowKey.length);
                    // In case of regular scans, if the region moves and scanner is reset,
                    // hbase client checks the last returned row by the server, gets the
                    // rowkey and appends "\x00" byte, before resuming the scan. With this,
                    // scan includeStartRowKey is set to true.
                    // However, same is not the case with reverse scans. For the reverse scan,
                    // hbase client checks the last returned row by the server, gets the
                    // rowkey and treats it as startRowKey for resuming the scan. With this,
                    // scan includeStartRowKey is set to false.
                    // Hence, we need to cover both cases here.
                    if (Bytes.compareTo(resultRowKey, scanStartRowKey) == 0) {
                        // This can be true for reverse scan case.
                        skipValidRowsSent = false;
                        if (includeStartRowKey) {
                            if (resultsToReturn.size() > 0) {
                                lastReturnedRowKey = CellUtil.cloneRow(resultsToReturn.get(0));
                            }
                            return moreRows;
                        }
                        // If includeStartRowKey is false and the current rowkey is matching
                        // with scanStartRowKey, return the next row result.
                        resultsToReturn.clear();
                        moreRows = nextInternal(resultsToReturn);
                        if (ScanUtil.isDummy(resultsToReturn)) {
                            return true;
                        }
                        if (resultsToReturn.size() > 0) {
                            lastReturnedRowKey = CellUtil.cloneRow(resultsToReturn.get(0));
                        }
                        return moreRows;
                    } else if (
                            Bytes.compareTo(
                                    ByteUtil.concat(resultRowKey, ByteUtil.ZERO_BYTE),
                                    scanStartRowKey) == 0) {
                        // This can be true for regular scan case.
                        skipValidRowsSent = false;
                        if (includeStartRowKey) {
                            // If includeStartRowKey is true and the (current rowkey + "\0xx") is
                            // matching with scanStartRowKey, return the next row result.
                            resultsToReturn.clear();
                            moreRows = nextInternal(resultsToReturn);
                            if (ScanUtil.isDummy(resultsToReturn)) {
                                return true;
                            }
                            if (resultsToReturn.size() > 0) {
                                lastReturnedRowKey = CellUtil.cloneRow(resultsToReturn.get(0));
                            }
                            return moreRows;
                        }
                    }
                    // In the loop, keep iterating through rows.
                    resultsToReturn.clear();
                    moreRows = nextInternal(resultsToReturn);
                    if (ScanUtil.isDummy(resultsToReturn)) {
                        return true;
                    }
                }
            }
            if (resultsToReturn.size() > 0) {
                lastReturnedRowKey = CellUtil.cloneRow(resultsToReturn.get(0));
            }
            return moreRows;
        }

        /**
         * Perform the next operation to grab the next row's worth of values.
         *
         * @param resultsToReturn output list of cells that are read as part of this operation.
         * @return true if more rows exist after this one, false if scanner is done.
         * @throws IOException if something goes wrong.
         */
        private boolean nextInternal(List<Cell> resultsToReturn) throws IOException {
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
                                return getDummyResult(resultsToReturn);
                            }
                            result.setKeyValues(results);
                            ImmutableBytesPtr key =
                                    TupleUtil.getConcatenatedValue(result, expressions);
                            ImmutableBytesPtr originalRowKey = new ImmutableBytesPtr();
                            result.getKey(originalRowKey);
                            Aggregator[] rowAggregators = groupByCache.cache(key);
                            groupByCache.cacheAggregateRowKey(key, originalRowKey);
                            // Aggregate values here
                            aggregators.aggregate(rowAggregators, result);
                        }
                        now = EnvironmentEdgeManager.currentTimeMillis();
                        if (hasMore && groupByCache.size() < limit
                                && (now - startTime) >= pageSizeMs) {
                            return getDummyResult(resultsToReturn);
                        }
                    } while (hasMore && groupByCache.size() < limit);
                    regionScanner = groupByCache.getScanner(delegate);
                    // Do not sort here, but sort back on the client instead
                    // The reason is that if the scan ever extends beyond a region
                    // (which can happen if we're basing our parallelization split
                    // points on old metadata), we'll get incorrect query results.
                    return regionScanner.next(resultsToReturn);
                }
            } catch (Exception e) {
                LOGGER.error("Unordered group-by scanner next encountered error for region {}",
                        region.getRegionInfo().getRegionNameAsString(), e);
                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new IOException(e);
                }
            } finally {
                if (acquiredLock) region.closeRegionOperation();
            }
        }

        /**
         * Retrieve dummy rowkey and return to the client.
         *
         * @param resultsToReturn dummy cell.
         * @return always true, because some rows are likely to exist as we are returning
         * dummy result to the client.
         */
        private boolean getDummyResult(List<Cell> resultsToReturn) {
            if (lastReturnedRowKey != null) {
                ScanUtil.getDummyResult(lastReturnedRowKey, resultsToReturn);
                return true;
            }
            if (scanStartRowKey.length > 0 && !ScanUtil.isLocalIndex(scan)) {
                if (hasRegionMoved()) {
                    byte[] lastByte =
                            new byte[]{scanStartRowKey[scanStartRowKey.length - 1]};
                    if (scanStartRowKey.length > 1 && Bytes.compareTo(lastByte,
                            ByteUtil.ZERO_BYTE) == 0) {
                        byte[] prevKey = new byte[scanStartRowKey.length - 1];
                        System.arraycopy(scanStartRowKey, 0, prevKey, 0,
                                prevKey.length);
                        ScanUtil.getDummyResult(prevKey, resultsToReturn);
                    } else {
                        ScanUtil.getDummyResult(scanStartRowKey,
                                resultsToReturn);
                    }
                } else {
                    ScanUtil.getDummyResult(scanStartRowKey, resultsToReturn);
                }
            } else {
                ScanUtil.getDummyResult(scanStartRowKey, resultsToReturn);
            }
            return true;
        }

        /**
         * Return true if the region has moved in the middle of an ongoing scan operation,
         * resulting in scanner reset. Based on the return value of this function, we need to
         * either scan the region as if we are scanning for the first time or we need to scan
         * the region considering that we have already returned some rows back to client and
         * we need to resume from the last row that we returned to the client.
         *
         * @return true if the region has moved in the middle of an ongoing scan operation.
         */
        private boolean hasRegionMoved() {
            return Bytes.compareTo(actualScanStartRowKey, scanStartRowKey) != 0
                    || actualScanIncludeStartRowKey != includeStartRowKey;
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
        private final ImmutableBytesPtr currentKeyRowKey = new ImmutableBytesPtr();
        private final boolean isIncompatibleClient;
        private final byte[] initStartRowKey;
        private final boolean includeInitStartRowKey;
        private byte[] previousResultRowKey;

        private OrderedGroupByRegionScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
                                            final Scan scan, final RegionScanner scanner, final List<Expression> expressions,
                                            final ServerAggregators aggregators, final long limit, final long pageSizeMs) {
            super(scanner);
            this.scan = scan;
            isIncompatibleClient = ScanUtil.isIncompatibleClientForServerReturnValidRowKey(scan);
            this.aggregators = aggregators;
            this.limit = limit;
            this.pageSizeMs = pageSizeMs;
            this.expressions = expressions;
            region = c.getEnvironment().getRegion();
            minMaxQualifiers = EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan);
            useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(minMaxQualifiers);
            encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
            initStartRowKey = ServerUtil.getScanStartRowKeyFromScanOrRegionBoundaries(scan,
                    region);
            includeInitStartRowKey = scan.includeStartRow();
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
                                updateDummyWithPrevRowKey(results, initStartRowKey,
                                        includeInitStartRowKey, scan);
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
                                if (result.size() > 0) {
                                    result.getKey(currentKeyRowKey);
                                }
                            }
                        }
                        atLimit = rowCount + countOffset >= limit;
                        // Do rowCount + 1 b/c we don't have to wait for a complete
                        // row in the case of a DISTINCT with a LIMIT
                        now = EnvironmentEdgeManager.currentTimeMillis();
                    } while (hasMore && !aggBoundary && !atLimit && (now - startTime) < pageSizeMs);
                }
            } catch (Exception e) {
                LOGGER.error("Ordered group-by scanner next encountered error for region {}",
                        region.getRegionInfo().getRegionNameAsString(), e);
                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new IOException(e);
                }
            } finally {
                if (acquiredLock) region.closeRegionOperation();
            }
            try {
                if (hasMore && !aggBoundary && !atLimit && (now - startTime) >= pageSizeMs) {
                    updateDummyWithPrevRowKey(results, initStartRowKey,
                            includeInitStartRowKey, scan);
                    return true;
                }
                if (currentKey != null) {
                    if (!isIncompatibleClient) {
                        byte[] aggregateArrayBytes = aggregators.toBytes(rowAggregators);
                        byte[] aggregateGroupValueBytes = new byte[currentKey.getLength()];
                        System.arraycopy(currentKey.get(), currentKey.getOffset(),
                                aggregateGroupValueBytes, 0,
                                aggregateGroupValueBytes.length);
                        byte[] finalValue =
                                ByteUtil.concat(
                                        PInteger.INSTANCE.toBytes(aggregateGroupValueBytes.length),
                                        aggregateGroupValueBytes, aggregateArrayBytes);
                        Cell keyValue =
                                PhoenixKeyValueUtil.newKeyValue(
                                        currentKeyRowKey.get(),
                                        currentKeyRowKey.getOffset(),
                                        currentKeyRowKey.getLength(),
                                        GROUPED_AGGREGATOR_VALUE_BYTES,
                                        GROUPED_AGGREGATOR_VALUE_BYTES,
                                        AGG_TIMESTAMP,
                                        finalValue,
                                        0,
                                        finalValue.length);
                        results.add(keyValue);
                    } else {
                        byte[] value = aggregators.toBytes(rowAggregators);
                        Cell keyValue =
                                PhoenixKeyValueUtil.newKeyValue(
                                        currentKey.get(),
                                        currentKey.getOffset(),
                                        currentKey.getLength(),
                                        SINGLE_COLUMN_FAMILY,
                                        SINGLE_COLUMN,
                                        AGG_TIMESTAMP,
                                        value,
                                        0,
                                        value.length);
                        results.add(keyValue);
                    }
                    // If we're at an aggregation boundary, reset the
                    // aggregators and
                    // aggregate with the current result (which is not a part of
                    // the returned result).
                    if (aggBoundary) {
                        aggregators.reset(rowAggregators);
                        aggregators.aggregate(rowAggregators, result);
                        currentKey = key;
                        if (result.size() > 0) {
                            result.getKey(currentKeyRowKey);
                        }
                        rowCount++;
                        atLimit |= rowCount >= limit;
                    }
                }
                // Continue if there are more
                if (!atLimit && (hasMore || aggBoundary)) {
                    if (!results.isEmpty()) {
                        previousResultRowKey = CellUtil.cloneRow(results.get(results.size() - 1));
                    }
                    return true;
                }
                currentKey = null;
                return false;
            } catch (Exception e) {
                LOGGER.error("Ordered group-by scanner next encountered some issue for"
                        + " region {}", region.getRegionInfo().getRegionNameAsString(), e);
                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new IOException(e);
                }
            }
        }

        /**
         * Add dummy cell to the result list based on either the previous rowkey returned to the
         * client or the start rowkey and start rowkey include params.
         *
         * @param result result to add the dummy cell to.
         * @param initStartRowKey scan start rowkey.
         * @param includeInitStartRowKey scan start rowkey included.
         * @param scan scan object.
         */
        private void updateDummyWithPrevRowKey(List<Cell> result, byte[] initStartRowKey,
                                               boolean includeInitStartRowKey, Scan scan) {
            result.clear();
            if (previousResultRowKey != null) {
                getDummyResult(previousResultRowKey, result);
            } else {
                if (includeInitStartRowKey && initStartRowKey.length > 0) {
                    byte[] prevKey;
                    // In order to generate largest possible rowkey that is less than
                    // initStartRowKey, we need to check size of the region name that can be
                    // used by hbase client for meta lookup, in case meta cache is expired at
                    // client.
                    // Once we know regionLookupInMetaLen, use it to generate largest possible
                    // rowkey that is lower than initStartRowKey by using
                    // ByteUtil#previousKeyWithLength function, which appends "\\xFF" bytes to
                    // prev rowkey upto the length provided. e.g. for the given key
                    // "\\x01\\xC1\\x06", the previous key with length 5 would be
                    // "\\x01\\xC1\\x05\\xFF\\xFF" by padding 2 bytes "\\xFF".
                    // The length of the largest scan start rowkey should not exceed
                    // HConstants#MAX_ROW_LENGTH.
                    int regionLookupInMetaLen =
                            RegionInfo.createRegionName(region.getTableDescriptor().getTableName(),
                                    new byte[1], HConstants.NINES, false).length;
                    if (Bytes.compareTo(initStartRowKey, initStartRowKey.length - 1,
                            1, ByteUtil.ZERO_BYTE, 0, 1) == 0) {
                        // If initStartRowKey has last byte as "\\x00", we can discard the last
                        // byte and send the key as dummy rowkey.
                        prevKey = new byte[initStartRowKey.length - 1];
                        System.arraycopy(initStartRowKey, 0, prevKey, 0, prevKey.length);
                    } else if (initStartRowKey.length < (HConstants.MAX_ROW_LENGTH - 1
                            - regionLookupInMetaLen)) {
                        prevKey = ByteUtil.previousKeyWithLength(ByteUtil.concat(initStartRowKey,
                                        new byte[HConstants.MAX_ROW_LENGTH - initStartRowKey.length
                                                - 1 - regionLookupInMetaLen]),
                                HConstants.MAX_ROW_LENGTH - 1 - regionLookupInMetaLen);
                    } else {
                        prevKey = initStartRowKey;
                    }
                    getDummyResult(prevKey, result);
                } else {
                    getDummyResult(initStartRowKey, result);
                }
            }
        }

    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserverConstants.UNORDERED_GROUP_BY_EXPRESSIONS) != null ||
               scan.getAttribute(BaseScannerRegionObserverConstants.KEY_ORDERED_GROUP_BY_EXPRESSIONS) != null;
    }
}
