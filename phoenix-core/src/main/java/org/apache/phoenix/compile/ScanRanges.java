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
package org.apache.phoenix.compile;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;


public class ScanRanges {
    private static final List<List<KeyRange>> EVERYTHING_RANGES = Collections.<List<KeyRange>>emptyList();
    private static final List<List<KeyRange>> NOTHING_RANGES = Collections.<List<KeyRange>>singletonList(Collections.<KeyRange>singletonList(KeyRange.EMPTY_RANGE));
    public static final ScanRanges EVERYTHING = new ScanRanges(EVERYTHING_RANGES,null,false, false);
    public static final ScanRanges NOTHING = new ScanRanges(NOTHING_RANGES,null,false, false);

    public static ScanRanges create(List<List<KeyRange>> ranges, RowKeySchema schema) {
        return create(ranges, schema, false, null);
    }
    
    public static ScanRanges create(List<List<KeyRange>> ranges, RowKeySchema schema, boolean forceRangeScan, Integer nBuckets) {
        int offset = nBuckets == null ? 0 : 1;
        if (ranges.size() == offset) {
            return EVERYTHING;
        } else if (ranges.size() == 1 + offset && ranges.get(offset).size() == 1 && ranges.get(offset).get(0) == KeyRange.EMPTY_RANGE) {
            return NOTHING;
        }
        boolean isPointLookup = !forceRangeScan && ScanRanges.isPointLookup(schema, ranges);
        if (isPointLookup) {
            // TODO: consider keeping original to use for serialization as it would
            // be smaller?
            List<byte[]> keys = ScanRanges.getPointKeys(ranges, schema, nBuckets);
            List<KeyRange> keyRanges = Lists.newArrayListWithExpectedSize(keys.size());
            for (byte[] key : keys) {
                keyRanges.add(KeyRange.getKeyRange(key));
            }
            ranges = Collections.singletonList(keyRanges);
            schema = SchemaUtil.VAR_BINARY_SCHEMA;
        } else if (nBuckets != null) {
            List<List<KeyRange>> saltedRanges = Lists.newArrayListWithExpectedSize(ranges.size());
            saltedRanges.add(SaltingUtil.generateAllSaltingRanges(nBuckets));
            saltedRanges.addAll(ranges.subList(1, ranges.size()));
            ranges = saltedRanges;
        }
        return new ScanRanges(ranges, schema, forceRangeScan, isPointLookup);
    }

    private SkipScanFilter filter;
    private final List<List<KeyRange>> ranges;
    private final RowKeySchema schema;
    private final boolean forceRangeScan;
    private final boolean isPointLookup;

    private ScanRanges (List<List<KeyRange>> ranges, RowKeySchema schema, boolean forceRangeScan, boolean isPointLookup) {
        this.isPointLookup = isPointLookup;
        List<List<KeyRange>> sortedRanges = Lists.newArrayListWithExpectedSize(ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            List<KeyRange> sorted = Lists.newArrayList(ranges.get(i));
            Collections.sort(sorted, KeyRange.COMPARATOR);
            sortedRanges.add(ImmutableList.copyOf(sorted));
        }
        this.ranges = ImmutableList.copyOf(sortedRanges);
        this.schema = schema;
        if (schema != null && !ranges.isEmpty()) {
            this.filter = new SkipScanFilter(this.ranges, schema);
        }
        this.forceRangeScan = forceRangeScan;
    }

    public SkipScanFilter getSkipScanFilter() {
        return filter;
    }
    
    public List<List<KeyRange>> getRanges() {
        return ranges;
    }

    public RowKeySchema getSchema() {
        return schema;
    }

    public boolean isEverything() {
        return this == EVERYTHING;
    }

    public boolean isDegenerate() {
        return this == NOTHING;
    }
    
    /**
     * Use SkipScanFilter under two circumstances:
     * 1) If we have multiple ranges for a given key slot (use of IN)
     * 2) If we have a range (i.e. not a single/point key) that is
     *    not the last key slot
     */
    public boolean useSkipScanFilter() {
        if (forceRangeScan) {
            return false;
        }
        if (isPointLookup) {
            return getPointLookupCount() > 1;
        }
        boolean hasRangeKey = false, useSkipScan = false;
        for (List<KeyRange> orRanges : ranges) {
            useSkipScan |= orRanges.size() > 1 | hasRangeKey;
            if (useSkipScan) {
                return true;
            }
            for (KeyRange range : orRanges) {
                hasRangeKey |= !range.isSingleKey();
            }
        }
        return false;
    }

    private static boolean isPointLookup(RowKeySchema schema, List<List<KeyRange>> ranges) {
        if (ranges.size() < schema.getMaxFields()) {
            return false;
        }
        for (List<KeyRange> orRanges : ranges) {
            for (KeyRange keyRange : orRanges) {
                if (!keyRange.isSingleKey()) {
                    return false;
                }
            }
        }
        return true;
    }
    
    
    private static boolean incrementKey(List<List<KeyRange>> slots, int[] position) {
        int idx = slots.size() - 1;
        while (idx >= 0 && (position[idx] = (position[idx] + 1) % slots.get(idx).size()) == 0) {
            idx--;
        }
        return idx >= 0;
    }

    private static List<byte[]> getPointKeys(List<List<KeyRange>> ranges, RowKeySchema schema, Integer bucketNum) {
        if (ranges == null || ranges.isEmpty()) {
            return Collections.emptyList();
        }
        boolean isSalted = bucketNum != null;
        int count = 1;
        int offset = isSalted ? 1 : 0;
        // Skip salt byte range in the first position if salted
        for (int i = offset; i < ranges.size(); i++) {
            count *= ranges.get(i).size();
        }
        List<byte[]> keys = Lists.newArrayListWithExpectedSize(count);
        int[] position = new int[ranges.size()];
        int maxKeyLength = SchemaUtil.getMaxKeyLength(schema, ranges);
        int length;
        byte[] key = new byte[maxKeyLength];
        do {
            length = ScanUtil.setKey(schema, ranges, position, Bound.LOWER, key, offset, offset, ranges.size(), offset);
            if (isSalted) {
                key[0] = SaltingUtil.getSaltingByte(key, offset, length, bucketNum);
            }
            keys.add(Arrays.copyOf(key, length + offset));
        } while (incrementKey(ranges, position));
        return keys;
    }

    /**
     * @return true if this represents a set of complete keys
     */
    public boolean isPointLookup() {
        return isPointLookup;
    }
    
    public int getPointLookupCount() {
        return isPointLookup ? ranges.get(0).size() : 0;
    }
    
    public Iterator<KeyRange> getPointLookupKeyIterator() {
        return isPointLookup ? ranges.get(0).iterator() : Iterators.<KeyRange>emptyIterator();
    }

    public void setScanStartStopRow(Scan scan) {
        if (isEverything()) {
            return;
        }
        if (isDegenerate()) {
            scan.setStartRow(KeyRange.EMPTY_RANGE.getLowerRange());
            scan.setStopRow(KeyRange.EMPTY_RANGE.getUpperRange());
            return;
        }
        
        byte[] expectedKey;
        expectedKey = ScanUtil.getMinKey(schema, ranges);
        if (expectedKey != null) {
            scan.setStartRow(expectedKey);
        }
        expectedKey = ScanUtil.getMaxKey(schema, ranges);
        if (expectedKey != null) {
            scan.setStopRow(expectedKey);
        }
    }

    public static final ImmutableBytesWritable UNBOUND = new ImmutableBytesWritable(KeyRange.UNBOUND);

    /**
     * Return true if the range formed by the lowerInclusiveKey and upperExclusiveKey
     * intersects with any of the scan ranges and false otherwise. We cannot pass in
     * a KeyRange here, because the underlying compare functions expect lower inclusive
     * and upper exclusive keys. We cannot get their next key because the key must
     * conform to the row key schema and if a null byte is added to a lower inclusive
     * key, it's no longer a valid, real key.
     * @param lowerInclusiveKey lower inclusive key
     * @param upperExclusiveKey upper exclusive key
     * @return true if the scan range intersects with the specified lower/upper key
     * range
     */
    public boolean intersect(byte[] lowerInclusiveKey, byte[] upperExclusiveKey) {
        if (isEverything()) {
            return true;
        }
        if (isDegenerate()) {
            return false;
        }
        return filter.hasIntersect(lowerInclusiveKey, upperExclusiveKey);
   }

    @Override
    public String toString() {
        return "ScanRanges[" + ranges.toString() + "]";
    }

}
