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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.ArrayIndexFunction;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.RegionScannerResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.join.ScanProjector;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
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
            List<KeyValueColumnExpression> arrayKVRefs) {
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
        byte[] isScanQuery = scan.getAttribute(BaseScannerRegionObserver.NON_AGGREGATE_QUERY);

        if (isScanQuery == null || Bytes.compareTo(PDataType.FALSE_BYTES, isScanQuery) == 0) {
            return s;
        }
        
        final ScanProjector p = ScanProjector.deserializeProjectorFromScan(scan);
        final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
        final ImmutableBytesWritable tenantId = ScanUtil.getTenantId(scan);
        
        RegionScanner innerScanner = s;
        if (p != null || j != null) {
            innerScanner = new HashJoinRegionScanner(s, p, j, tenantId, c.getEnvironment());
        }
        
        final OrderedResultIterator iterator = deserializeFromScan(scan,innerScanner);
        List<KeyValueColumnExpression> arrayKVRefs = new ArrayList<KeyValueColumnExpression>();
        Expression[] arrayFuncRefs = deserializeArrayPostionalExpressionInfoFromScan(
                scan, innerScanner, arrayKVRefs);
        innerScanner = getWrappedScanner(c, innerScanner, arrayKVRefs, arrayFuncRefs);
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
                    chunk.close();                }
            }
            
            @Override
            public long getMaxResultSize() {
                return s.getMaxResultSize();
            }
        };
    }
        
    /**
     * Return wrapped scanner that catches unexpected exceptions (i.e. Phoenix bugs) and
     * re-throws as DoNotRetryIOException to prevent needless retrying hanging the query
     * for 30 seconds. Unfortunately, until HBASE-7481 gets fixed, there's no way to do
     * the same from a custom filter.
     * @param arrayFuncRefs 
     * @param arrayKVRefs 
     */
    private RegionScanner getWrappedScanner(final ObserverContext<RegionCoprocessorEnvironment> c, final RegionScanner s, 
           final List<KeyValueColumnExpression> arrayKVRefs, final Expression[] arrayFuncRefs) {
        return new RegionScanner() {

            @Override
            public boolean next(List<Cell> results) throws IOException {
                try {
                    return s.next(results);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean next(List<Cell> result, int limit) throws IOException {
                try {
                    return s.next(result, limit);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public void close() throws IOException {
                s.close();
            }

            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public boolean isFilterDone() throws IOException {
                return s.isFilterDone();
            }

            @Override
            public boolean reseek(byte[] row) throws IOException {
                return s.reseek(row);
            }
            
            @Override
            public long getMvccReadPoint() {
                return s.getMvccReadPoint();
            }

            @Override
            public boolean nextRaw(List<Cell> result) throws IOException {
                try {
                    boolean next = s.nextRaw(result);
                    if(result.size() == 0) {
                        return next;
                    } else if((arrayFuncRefs != null && arrayFuncRefs.length == 0) || arrayKVRefs.size() == 0) {
                        return next;
                    }
                    replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
                    return next;
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            @Override
            public boolean nextRaw(List<Cell> result, int limit) throws IOException {
                try {
                    boolean next = s.nextRaw(result, limit);
                    if (result.size() == 0) {
                        return next;
                    } else if ((arrayFuncRefs != null && arrayFuncRefs.length == 0) || arrayKVRefs.size() == 0) { 
                        return next; 
                    }
                    // There is a scanattribute set to retrieve the specific array element
                    replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
                    return next;
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegion().getRegionNameAsString(), t);
                    return false; // impossible
                }
            }

            private void replaceArrayIndexElement(final List<KeyValueColumnExpression> arrayKVRefs,
                    final Expression[] arrayFuncRefs, List<Cell> result) {
                MultiKeyValueTuple tuple = new MultiKeyValueTuple(result);
                // The size of both the arrays would be same?
                // Using KeyValueSchema to set and retrieve the value
                // collect the first kv to get the row
                Cell rowKv = result.get(0);
                for (int i = 0; i < arrayKVRefs.size(); i++) {
                    KeyValueColumnExpression kvExp = arrayKVRefs.get(i);
                    if (kvExp.evaluate(tuple, ptr)) {
                        for (int idx = tuple.size() - 1; idx >= 0; idx--) {
                        	Cell kv = tuple.getValue(idx);
                            if (Bytes.equals(kvExp.getColumnFamily(), 0, kvExp.getColumnFamily().length, 
                            		kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength())
                                && Bytes.equals(kvExp.getColumnName(), 0, kvExp.getColumnName().length, 
                                		kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength())) {
                                // remove the kv that has the full array values.
                                result.remove(idx);
                                break;
                            }
                        }
                    }
                }
                byte[] value = kvSchema.toBytes(tuple, arrayFuncRefs,
                        kvSchemaBitSet, ptr);
                // Add a dummy kv with the exact value of the array index
                result.add(new KeyValue(rowKv.getRowArray(), rowKv.getRowOffset(), rowKv.getRowLength(),
                        QueryConstants.ARRAY_VALUE_COLUMN_FAMILY, 0, QueryConstants.ARRAY_VALUE_COLUMN_FAMILY.length,
                        QueryConstants.ARRAY_VALUE_COLUMN_QUALIFIER, 0,
                        QueryConstants.ARRAY_VALUE_COLUMN_QUALIFIER.length, HConstants.LATEST_TIMESTAMP,
                        Type.codeToType(rowKv.getTypeByte()), value, 0, value.length));
            }
            
            @Override
            public long getMaxResultSize() {
                return s.getMaxResultSize();
            }
        };
    }
    
    
}
