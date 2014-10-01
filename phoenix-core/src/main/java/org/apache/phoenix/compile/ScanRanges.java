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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;


public class ScanRanges {
    private static final List<List<KeyRange>> EVERYTHING_RANGES = Collections.<List<KeyRange>>emptyList();
    private static final List<List<KeyRange>> NOTHING_RANGES = Collections.<List<KeyRange>>singletonList(Collections.<KeyRange>singletonList(KeyRange.EMPTY_RANGE));
    public static final ScanRanges EVERYTHING = new ScanRanges(null,ScanUtil.SINGLE_COLUMN_SLOT_SPAN,EVERYTHING_RANGES, KeyRange.EVERYTHING_RANGE, false, false, null);
    public static final ScanRanges NOTHING = new ScanRanges(null,ScanUtil.SINGLE_COLUMN_SLOT_SPAN,NOTHING_RANGES, KeyRange.EMPTY_RANGE, false, false, null);

    public static ScanRanges create(RowKeySchema schema, List<List<KeyRange>> ranges, int[] slotSpan) {
        return create(schema, ranges, slotSpan, false, null);
    }
    
    public static ScanRanges create(RowKeySchema schema, List<List<KeyRange>> ranges, int[] slotSpan, boolean forceRangeScan, Integer nBuckets) {
        int offset = nBuckets == null ? 0 : 1;
        if (ranges.size() == offset) {
            return EVERYTHING;
        } else if (ranges.size() == 1 + offset && ranges.get(offset).size() == 1 && ranges.get(offset).get(0) == KeyRange.EMPTY_RANGE) {
            return NOTHING;
        }
        boolean isPointLookup = !forceRangeScan && ScanRanges.isPointLookup(schema, ranges, slotSpan);
        if (isPointLookup) {
            // TODO: consider keeping original to use for serialization as it would
            // be smaller?
            List<byte[]> keys = ScanRanges.getPointKeys(ranges, slotSpan, schema, nBuckets);
            List<KeyRange> keyRanges = Lists.newArrayListWithExpectedSize(keys.size());
            for (byte[] key : keys) {
                keyRanges.add(KeyRange.getKeyRange(key));
            }
            ranges = Collections.singletonList(keyRanges);
            if (keys.size() > 1) {
                schema = SchemaUtil.VAR_BINARY_SCHEMA;
                slotSpan = ScanUtil.SINGLE_COLUMN_SLOT_SPAN;
            } else {
                // Keep original schema and don't use skip scan as it's not necessary
                // when there's a single key.
                slotSpan = new int[] {schema.getMaxFields()-1};
            }
        } /*else if (nBuckets != null) {
            List<List<KeyRange>> saltedRanges = Lists.newArrayListWithExpectedSize(ranges.size());
            saltedRanges.add(SaltingUtil.generateAllSaltingRanges(nBuckets));
            saltedRanges.addAll(ranges.subList(1, ranges.size()));
            ranges = saltedRanges;
        }*/
        List<List<KeyRange>> sortedRanges = Lists.newArrayListWithExpectedSize(ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            List<KeyRange> sorted = Lists.newArrayList(ranges.get(i));
            Collections.sort(sorted, KeyRange.COMPARATOR);
            sortedRanges.add(ImmutableList.copyOf(sorted));
        }
        // Don't set minMaxRange for point lookup because it causes issues during intersect
        // by us ignoring the salt byte
        KeyRange minMaxRange = isPointLookup ? KeyRange.EVERYTHING_RANGE : calculateMinMaxRange(schema, slotSpan, sortedRanges);
        return new ScanRanges(schema, slotSpan, sortedRanges, minMaxRange, forceRangeScan, isPointLookup, nBuckets);
    }

    private static KeyRange calculateMinMaxRange(RowKeySchema schema, int[] slotSpan, List<List<KeyRange>> ranges) {
        byte[] minKey = ScanUtil.getMinKey(schema, ranges, slotSpan);
        byte[] maxKey = ScanUtil.getMaxKey(schema, ranges, slotSpan);
        return KeyRange.getKeyRange(minKey, maxKey);
    }
    
    private SkipScanFilter filter;
    private final List<List<KeyRange>> ranges;
    private final int[] slotSpan;
    private final RowKeySchema schema;
    private final boolean isPointLookup;
    private final boolean isSalted;
    private final boolean useSkipScanFilter;
    private final KeyRange minMaxRange;

    private ScanRanges (RowKeySchema schema, int[] slotSpan, List<List<KeyRange>> ranges, KeyRange minMaxRange, boolean forceRangeScan, boolean isPointLookup, Integer bucketNum) {
        this.isPointLookup = isPointLookup;
        this.isSalted = bucketNum != null;
        this.minMaxRange = minMaxRange;
        this.useSkipScanFilter = useSkipScanFilter(forceRangeScan, isPointLookup, ranges);
        
        // Only blow out the bucket values if we're using the skip scan. We need all the
        // bucket values in this case because we use intersect against a key that may have
        // any of the possible bucket values. Otherwise, we can pretty easily ignore the
        // bucket values.
        if (useSkipScanFilter && isSalted && !isPointLookup) {
        	ranges.set(0, SaltingUtil.generateAllSaltingRanges(bucketNum));
        }
        this.ranges = ImmutableList.copyOf(ranges);
        this.slotSpan = slotSpan;
        this.schema = schema;
        if (schema != null && !ranges.isEmpty()) { // TODO: only create if useSkipScanFilter is true?
            this.filter = new SkipScanFilter(this.ranges, slotSpan, schema);
        }
    }
    
    private static byte[] prefixKey(byte[] key, int keyOffset, byte[] prefixKey, int prefixKeyOffset) {
        if (key.length > 0) {
            byte[] newKey = new byte[key.length + prefixKeyOffset];
            int totalKeyOffset = keyOffset + prefixKeyOffset;
            System.arraycopy(prefixKey, 0, newKey, 0, totalKeyOffset);
            System.arraycopy(key, keyOffset, newKey, totalKeyOffset, key.length - keyOffset);
            return newKey;
        }
        return key;
    }
    
    private static byte[] replaceSaltByte(byte[] key, byte[] saltKey) {
        if (key.length == 0) {
            return key;
        }
        byte[] temp = new byte[key.length];
        System.arraycopy(saltKey, 0, temp, 0, SaltingUtil.NUM_SALTING_BYTES);
        System.arraycopy(key, SaltingUtil.NUM_SALTING_BYTES, temp, SaltingUtil.NUM_SALTING_BYTES, key.length - SaltingUtil.NUM_SALTING_BYTES);
        return temp;
    }
    
    private static byte[] stripLocalIndexPrefix(byte[] key, int keyOffset) {
        if (key.length == 0) {
            return key;
        }
        byte[] temp = new byte[key.length - keyOffset];
        System.arraycopy(key, keyOffset, temp, 0, key.length - keyOffset);
        return temp;
    }

    public Scan intersect(Scan scan, final byte[] originalStartKey, final byte[] originalStopKey, final int keyOffset) {
        byte[] startKey = originalStartKey;
        byte[] stopKey = originalStopKey;
        boolean mayHaveRows = false;
        final int scanKeyOffset = this.isSalted ? SaltingUtil.NUM_SALTING_BYTES : 0;
        // Offset for startKey/stopKey. Either 1 for salted tables or the prefix length
        // of the current region for local indexes.
        final int totalKeyOffset = scanKeyOffset + keyOffset;
        // In this case, we've crossed the "prefix" boundary and should consider everything after the startKey
        // This prevents us from having to prefix the key prior to knowing whether or not there may be an
        // intersection.
        byte[] prefixBytes = ByteUtil.EMPTY_BYTE_ARRAY;
        if (totalKeyOffset > 0) {
            prefixBytes = startKey.length > 0 ? startKey : (this.isSalted ? QueryConstants.SEPARATOR_BYTE_ARRAY : stopKey);
        }
        if (stopKey.length < totalKeyOffset || Bytes.compareTo(prefixBytes, 0, totalKeyOffset, stopKey, 0, totalKeyOffset) != 0) {
            stopKey = ByteUtil.EMPTY_BYTE_ARRAY;
        }
        assert (scanKeyOffset == 0 || keyOffset == 0);
        int scanStartKeyOffset = scanKeyOffset;
        byte[] scanStartKey = scan.getStartRow();
        // Compare ignoring key prefix and salt byte
        if (scanStartKey.length > 0) {
            if (startKey.length > 0 && Bytes.compareTo(scanStartKey, scanKeyOffset, scanStartKey.length - scanKeyOffset, startKey, totalKeyOffset, startKey.length - totalKeyOffset) < 0) {
                scanStartKey = startKey;
                scanStartKeyOffset = totalKeyOffset;
            }
        } else {
        	scanStartKey = startKey;
            scanStartKeyOffset = totalKeyOffset;
            mayHaveRows = true;
        }
        int scanStopKeyOffset = scanKeyOffset;
        byte[] scanStopKey = scan.getStopRow();
        if (scanStopKey.length > 0) {
            if (stopKey.length > 0 && Bytes.compareTo(scanStopKey, scanKeyOffset, scanStopKey.length - scanKeyOffset, stopKey, totalKeyOffset, stopKey.length - totalKeyOffset) > 0) {
                scanStopKey = stopKey;
                scanStopKeyOffset = totalKeyOffset;
            }
        } else {
        	scanStopKey = stopKey;
            scanStopKeyOffset = totalKeyOffset;
            mayHaveRows = true;
        }
        mayHaveRows = mayHaveRows || Bytes.compareTo(scanStartKey, scanStartKeyOffset, scanStartKey.length - scanStartKeyOffset, scanStopKey, scanStopKeyOffset, scanStopKey.length - scanStopKeyOffset) < 0;
        
        if (!mayHaveRows) {
            return null;
        }
        if (originalStopKey.length != 0 && scanStopKey.length == 0) {
            scanStopKey = originalStopKey;
        }
        Filter newFilter = scan.getFilter();
        // If the scan is using skip scan filter, intersect and replace the filter.
        if (this.useSkipScanFilter()) {
            byte[] skipScanStartKey = scanStartKey;
            byte[] skipScanStopKey = scanStopKey;
            // If we have a keyOffset and we've used the startKey/stopKey that
            // were passed in (which have the prefix) for the above range check,
            // we need to remove the prefix before running our intersect method.
            // TODO: we could use skipScanFilter.setOffset(keyOffset) if both
            // the startKey and stopKey were used above *and* our intersect
            // method honored the skipScanFilter.offset variable.
            if (scanKeyOffset > 0) {
                if (skipScanStartKey != originalStartKey) { // original already has correct salt byte
                    skipScanStartKey = replaceSaltByte(skipScanStartKey, prefixBytes);
                }
                if (skipScanStopKey != originalStopKey) {
                    skipScanStopKey = replaceSaltByte(skipScanStopKey, prefixBytes);
                }
            } else if (keyOffset > 0) {
                if (skipScanStartKey == originalStartKey) {
                    skipScanStartKey = stripLocalIndexPrefix(skipScanStartKey, keyOffset);
                }
                if (skipScanStopKey == originalStopKey) {
                    skipScanStopKey = stripLocalIndexPrefix(skipScanStopKey, keyOffset);
                }
            }
            Filter filter = scan.getFilter();
            if (filter instanceof SkipScanFilter) {
                SkipScanFilter oldSkipScanFilter = (SkipScanFilter)filter;
                newFilter = oldSkipScanFilter.intersect(skipScanStartKey, skipScanStopKey);
                if (newFilter == null) {
                    return null;
                }
            } else if (filter instanceof FilterList) {
                FilterList oldList = (FilterList)filter;
                FilterList newList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                newFilter = newList;
                for (Filter f : oldList.getFilters()) {
                    if (f instanceof SkipScanFilter) {
                        SkipScanFilter newSkipScanFilter = ((SkipScanFilter)f).intersect(skipScanStartKey, skipScanStopKey);
                        if (newSkipScanFilter == null) {
                            return null;
                        }
                        newList.addFilter(newSkipScanFilter);
                    } else {
                        newList.addFilter(f);
                    }
                }
            }
        }
        Scan newScan = ScanUtil.newScan(scan);
        newScan.setFilter(newFilter);
        // If we have an offset (salted table or local index), we need to make sure to
        // prefix our scan start/stop row by the prefix of the startKey or stopKey that
        // were passed in. Our scan either doesn't have the prefix or has a placeholder
        // for it.
        if (totalKeyOffset > 0) {
            if (scanStartKey != originalStartKey) {
                scanStartKey = prefixKey(scanStartKey, scanKeyOffset, prefixBytes, keyOffset);
            }
            if (scanStopKey != originalStopKey) {
                scanStopKey = prefixKey(scanStopKey, scanKeyOffset, prefixBytes, keyOffset);
            }
        }
        newScan.setStartRow(scanStartKey);
        newScan.setStopRow(scanStopKey);
        
        return newScan;
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
        return useSkipScanFilter;
    }
    
    private static boolean useSkipScanFilter(boolean forceRangeScan, boolean isPointLookup, List<List<KeyRange>> ranges) {
        if (forceRangeScan) {
            return false;
        }
        if (isPointLookup) {
            return getPointLookupCount(isPointLookup, ranges) > 1;
        }
        boolean hasRangeKey = false, useSkipScan = false;
        for (List<KeyRange> orRanges : ranges) {
            useSkipScan |= (orRanges.size() > 1 || hasRangeKey);
            if (useSkipScan) {
                return true;
            }
            for (KeyRange range : orRanges) {
                hasRangeKey |= !range.isSingleKey();
            }
        }
        return false;
    }

    private static boolean isPointLookup(RowKeySchema schema, List<List<KeyRange>> ranges, int[] slotSpan) {
        if (ScanUtil.getTotalSpan(ranges, slotSpan) < schema.getMaxFields()) {
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

    private static List<byte[]> getPointKeys(List<List<KeyRange>> ranges, int[] slotSpan, RowKeySchema schema, Integer bucketNum) {
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
            length = ScanUtil.setKey(schema, ranges, slotSpan, position, Bound.LOWER, key, offset, offset, ranges.size(), offset);
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
        return getPointLookupCount(isPointLookup, ranges);
    }
    
    private static int getPointLookupCount(boolean isPointLookup, List<List<KeyRange>> ranges) {
        return isPointLookup ? ranges.get(0).size() : 0;
    }
    
    public Iterator<KeyRange> getPointLookupKeyIterator() {
        return isPointLookup ? ranges.get(0).iterator() : Iterators.<KeyRange>emptyIterator();
    }

    public KeyRange getMinMaxRange() {
        return minMaxRange;
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
    
    public int getPkColumnSpan() {
        return this == ScanRanges.NOTHING ? 0 : ScanUtil.getTotalSpan(ranges, slotSpan);
    }

    @Override
    public String toString() {
        return "ScanRanges[" + ranges.toString() + "]";
    }

}
