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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.ArrayIndexFunction;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.RegionScannerResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import com.google.common.collect.Lists;


/**
 *
 * Wraps the scan performing a non aggregate query to prevent needless retries
 * if a Phoenix bug is encountered from our custom filter expression evaluation.
 * Unfortunately, until HBASE-7481 gets fixed, there's no way to do this from our
 * custom filters.
 *
 *
 * @since 0.1
 */
public class ScanRegionObserver extends BaseScannerRegionObserver {
    private ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    private KeyValueSchema kvSchema = null;
    private ValueBitSet kvSchemaBitSet;
    public static void serializeIntoScan(Scan scan, int thresholdBytes, int limit, List<OrderByExpression> orderByExpressions, int estimatedRowSize) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(); // TODO: size?
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, thresholdBytes);
            WritableUtils.writeVInt(output, limit);
            WritableUtils.writeVInt(output, estimatedRowSize);
            WritableUtils.writeVInt(output, orderByExpressions.size());
            for (OrderByExpression orderingCol : orderByExpressions) {
                orderingCol.write(output);
            }
            scan.setAttribute(BaseScannerRegionObserver.TOPN, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static OrderedResultIterator deserializeFromScan(Scan scan, RegionScanner s) {
        byte[] topN = scan.getAttribute(BaseScannerRegionObserver.TOPN);
        if (topN == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(topN); // TODO: size?
        try {
            DataInputStream input = new DataInputStream(stream);
            int thresholdBytes = WritableUtils.readVInt(input);
            int limit = WritableUtils.readVInt(input);
            int estimatedRowSize = WritableUtils.readVInt(input);
            int size = WritableUtils.readVInt(input);
            List<OrderByExpression> orderByExpressions = Lists.newArrayListWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                OrderByExpression orderByExpression = new OrderByExpression();
                orderByExpression.readFields(input);
                orderByExpressions.add(orderByExpression);
            }
            ResultIterator inner = new RegionScannerResultIterator(s);
            return new OrderedResultIterator(inner, orderByExpressions, thresholdBytes, limit >= 0 ? limit : null, estimatedRowSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Expression[] deserializeArrayPostionalExpressionInfoFromScan(Scan scan, RegionScanner s,
            Set<KeyValueColumnExpression> arrayKVRefs) {
        byte[] specificArrayIdx = scan.getAttribute(BaseScannerRegionObserver.SPECIFIC_ARRAY_INDEX);
        if (specificArrayIdx == null) {
            return null;
        }
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        ByteArrayInputStream stream = new ByteArrayInputStream(specificArrayIdx);
        try {
            DataInputStream input = new DataInputStream(stream);
            int arrayKVRefSize = WritableUtils.readVInt(input);
            for (int i = 0; i < arrayKVRefSize; i++) {
                KeyValueColumnExpression kvExp = new KeyValueColumnExpression();
                kvExp.readFields(input);
                arrayKVRefs.add(kvExp);
            }
            int arrayKVFuncSize = WritableUtils.readVInt(input);
            Expression[] arrayFuncRefs = new Expression[arrayKVFuncSize];
            for (int i = 0; i < arrayKVFuncSize; i++) {
                ArrayIndexFunction arrayIdxFunc = new ArrayIndexFunction();
                arrayIdxFunc.readFields(input);
                arrayFuncRefs[i] = arrayIdxFunc;
                builder.addField(arrayIdxFunc);
            }
            kvSchema = builder.build();
            kvSchemaBitSet = ValueBitSet.newInstance(kvSchema);
            return arrayFuncRefs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable {
        int offset = 0;
        if (ScanUtil.isLocalIndex(scan)) {
            /*
             * For local indexes, we need to set an offset on row key expressions to skip
             * the region start key.
             */
            HRegion region = c.getEnvironment().getRegion();
            offset = region.getStartKey().length != 0 ? region.getStartKey().length:region.getEndKey().length;
            ScanUtil.setRowKeyOffset(scan, offset);
        }

        RegionScanner innerScanner = s;

        Set<KeyValueColumnExpression> arrayKVRefs = Sets.newHashSet();
        Expression[] arrayFuncRefs = deserializeArrayPostionalExpressionInfoFromScan(
                scan, innerScanner, arrayKVRefs);
        TupleProjector tupleProjector = null;
        HRegion dataRegion = null;
        IndexMaintainer indexMaintainer = null;
        byte[][] viewConstants = null;
        ColumnReference[] dataColumns = IndexUtil.deserializeDataTableColumnsToJoin(scan);
        if (dataColumns != null) {
            tupleProjector = IndexUtil.getTupleProjector(scan, dataColumns);
            dataRegion = IndexUtil.getDataRegion(c.getEnvironment());
            byte[] localIndexBytes = scan.getAttribute(LOCAL_INDEX_BUILD);
            List<IndexMaintainer> indexMaintainers = localIndexBytes == null ? null : IndexMaintainer.deserialize(localIndexBytes);
            indexMaintainer = indexMaintainers.get(0);
            viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
        }
        
        final TupleProjector p = TupleProjector.deserializeProjectorFromScan(scan);
        final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
        innerScanner =
                getWrappedScanner(c, innerScanner, arrayKVRefs, arrayFuncRefs, offset, scan,
                    dataColumns, tupleProjector, dataRegion, indexMaintainer, viewConstants,
                    kvSchema, kvSchemaBitSet, j == null ? p : null, ptr);

        final ImmutableBytesWritable tenantId = ScanUtil.getTenantId(scan);
        if (j != null) {
            innerScanner = new HashJoinRegionScanner(innerScanner, p, j, tenantId, c.getEnvironment());
        }

        final OrderedResultIterator iterator = deserializeFromScan(scan,innerScanner);
        if (iterator == null) {
            return innerScanner;
        }
        // TODO:the above wrapped scanner should be used here also
        return getTopNScanner(c, innerScanner, iterator, tenantId);
    }

    /**
     *  Return region scanner that does TopN.
     *  We only need to call startRegionOperation and closeRegionOperation when
     *  getting the first Tuple (which forces running through the entire region)
     *  since after this everything is held in memory
     */
    private RegionScanner getTopNScanner(final ObserverContext<RegionCoprocessorEnvironment> c, final RegionScanner s, final OrderedResultIterator iterator, ImmutableBytesWritable tenantId) throws Throwable {
        final Tuple firstTuple;
        TenantCache tenantCache = GlobalCache.getTenantCache(c.getEnvironment(), tenantId);
        long estSize = iterator.getEstimatedByteSize();
        final MemoryChunk chunk = tenantCache.getMemoryManager().allocate(estSize);
        final HRegion region = c.getEnvironment().getRegion();
        region.startRegionOperation();
        try {
            // Once we return from the first call to next, we've run through and cached
            // the topN rows, so we no longer need to start/stop a region operation.
            firstTuple = iterator.next();
            // Now that the topN are cached, we can resize based on the real size
            long actualSize = iterator.getByteSize();
            chunk.resize(actualSize);
        } catch (Throwable t) {
            ServerUtil.throwIOException(region.getRegionNameAsString(), t);
            return null;
        } finally {
            region.closeRegionOperation();
        }
        return new BaseRegionScanner() {
            private Tuple tuple = firstTuple;

            @Override
            public boolean isFilterDone() {
                return tuple == null;
            }

            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                try {
                    if (isFilterDone()) {
                        return false;
                    }

                    for (int i = 0; i < tuple.size(); i++) {
                        results.add(tuple.getValue(i));
                    }

                    tuple = iterator.next();
                    return !isFilterDone();
                } catch (Throwable t) {
                    ServerUtil.throwIOException(region.getRegionNameAsString(), t);
                    return false;
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    s.close();
                } finally {
                    try {
                        if(iterator != null) {
                            iterator.close();    
                        }
                    } catch (SQLException e) {
                        ServerUtil.throwIOException(region.getRegionNameAsString(), e);
                    } finally {
                        chunk.close();                
                    }
                }
            }

            @Override
            public long getMaxResultSize() {
                return s.getMaxResultSize();
            }
        };
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserver.NON_AGGREGATE_QUERY) != null;
    }

}
