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

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SCAN_ACTUAL_START_ROW;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PDataType.PDataCodec;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;


public class ScanRanges {
    private static final List<List<KeyRange>> EVERYTHING_RANGES = Collections.<List<KeyRange>>emptyList();
    private static final List<List<KeyRange>> NOTHING_RANGES = Collections.<List<KeyRange>>singletonList(Collections.<KeyRange>singletonList(KeyRange.EMPTY_RANGE));
    public static final ScanRanges EVERYTHING = new ScanRanges(null,ScanUtil.SINGLE_COLUMN_SLOT_SPAN,EVERYTHING_RANGES, KeyRange.EVERYTHING_RANGE, false, false, null, null);
    public static final ScanRanges NOTHING = new ScanRanges(null,ScanUtil.SINGLE_COLUMN_SLOT_SPAN,NOTHING_RANGES, KeyRange.EMPTY_RANGE, false, false, null, null);
    private static final Scan HAS_INTERSECTION = new Scan();

    public static ScanRanges createPointLookup(List<KeyRange> keys) {
        return ScanRanges.create(SchemaUtil.VAR_BINARY_SCHEMA, Collections.singletonList(keys), ScanUtil.SINGLE_COLUMN_SLOT_SPAN, null, true, -1);
    }
    
    // For testing
    public static ScanRanges createSingleSpan(RowKeySchema schema, List<List<KeyRange>> ranges) {
        return create(schema, ranges, ScanUtil.getDefaultSlotSpans(ranges.size()), null, true, -1);
    }
    
    // For testing
    public static ScanRanges createSingleSpan(RowKeySchema schema, List<List<KeyRange>> ranges, Integer nBuckets, boolean useSkipSan) {
        return create(schema, ranges, ScanUtil.getDefaultSlotSpans(ranges.size()), nBuckets, useSkipSan, -1);
    }

    public static ScanRanges create(RowKeySchema schema, List<List<KeyRange>> ranges, int[] slotSpan, Integer nBuckets, boolean useSkipScan, int rowTimestampColIndex) {
        return create(schema,ranges,slotSpan,nBuckets,useSkipScan,rowTimestampColIndex,Optional.<byte[]>absent());
    }

    public static ScanRanges create(RowKeySchema schema, List<List<KeyRange>> ranges, int[] slotSpan, Integer nBuckets, boolean useSkipScan, int rowTimestampColIndex, Optional<byte[]> scanMinOffset) {
        int offset = nBuckets == null ? 0 : SaltingUtil.NUM_SALTING_BYTES;
        int nSlots = ranges.size();

        if (nSlots == offset && !scanMinOffset.isPresent()) {
            return EVERYTHING;
        } else if ((nSlots == 1 + offset && ranges.get(offset).size() == 1 && ranges.get(offset).get(0) == KeyRange.EMPTY_RANGE)) {
            return NOTHING;
        }
        TimeRange rowTimestampRange = getRowTimestampColumnRange(ranges, schema, rowTimestampColIndex);
        boolean isPointLookup = isPointLookup(schema, ranges, slotSpan, useSkipScan);
        if (isPointLookup) {
            // TODO: consider keeping original to use for serialization as it would be smaller?
            List<byte[]> keys = ScanRanges.getPointKeys(ranges, slotSpan, schema, nBuckets);
            List<KeyRange> keyRanges = Lists.newArrayListWithExpectedSize(keys.size());
            // We have full keys here, so use field from our varbinary schema
            for (byte[] key : keys) {
                keyRanges.add(KeyRange.getKeyRange(key));
            }
            // while doing a point look up if after intersecting with the MinMaxrange there are
            // no more keyranges left then just return
            if (keyRanges.isEmpty()) {
                return NOTHING;
            }
            ranges = Collections.singletonList(keyRanges);
            useSkipScan = keyRanges.size() > 1;
            // Treat as binary if descending because we've got a separator byte at the end
            // which is not part of the value.
            if (keys.size() > 1 || SchemaUtil.getSeparatorByte(schema.rowKeyOrderOptimizable(), false, schema.getField(schema.getFieldCount()-1)) == QueryConstants.DESC_SEPARATOR_BYTE) {
                schema = SchemaUtil.VAR_BINARY_SCHEMA;
                slotSpan = ScanUtil.SINGLE_COLUMN_SLOT_SPAN;
            } else {
                // Keep original schema and don't use skip scan as it's not necessary
                // when there's a single key.
                slotSpan = new int[] {schema.getMaxFields()-1};
            }
        }

        List<List<KeyRange>> sortedRanges = Lists.newArrayListWithExpectedSize(ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            Field f = schema.getField(i);
            List<KeyRange> sorted = Lists.newArrayList(ranges.get(i));
            Collections.sort(sorted, f.getSortOrder() == SortOrder.ASC ? KeyRange.COMPARATOR : KeyRange.DESC_COMPARATOR);
            sortedRanges.add(ImmutableList.copyOf(sorted));
        }
        
        
        // Don't set minMaxRange for point lookup because it causes issues during intersect
        // by going across region boundaries
        KeyRange scanRange = KeyRange.EVERYTHING_RANGE;
        // if (!isPointLookup && (nBuckets == null || !useSkipScanFilter)) {
        // if (! ( isPointLookup || (nBuckets != null && useSkipScanFilter) ) ) {
        // if (nBuckets == null || (nBuckets != null && (!isPointLookup || !useSkipScanFilter))) {
        if (nBuckets == null || !isPointLookup || !useSkipScan) {
            byte[] minKey = ScanUtil.getMinKey(schema, sortedRanges, slotSpan);
            byte[] maxKey = ScanUtil.getMaxKey(schema, sortedRanges, slotSpan);
            // If the maxKey has crossed the salt byte boundary, then we do not
            // have anything to filter at the upper end of the range
            if (ScanUtil.crossesPrefixBoundary(maxKey, ScanUtil.getPrefix(minKey, offset), offset)) {
                maxKey = KeyRange.UNBOUND;
            }
            // We won't filter anything at the low end of the range if we just have the salt byte
            if (minKey.length <= offset) {
                minKey = KeyRange.UNBOUND;
            }

            //Handle the offset by pushing it into the scanRange
            if(scanMinOffset.isPresent()){

                byte[] minOffset = scanMinOffset.get();
                //If we are salted we have to
                //This should be safe for RVC Offset since we specify a full rowkey which
                // is by definition unique so duplicating the salt bucket is fine
                if(nBuckets != null && nBuckets > 0) {
                    minOffset[0] = 0;  //We use 0 for salt bucket for scans
                }

                //If the offset is more selective than the existing ranges
                if(Bytes.BYTES_COMPARATOR.compare(minOffset,minKey) > 0){
                    minKey=minOffset;
                }
            }

            scanRange = KeyRange.getKeyRange(minKey, maxKey);
        }

        if (scanRange == KeyRange.EMPTY_RANGE) {
            return NOTHING;
        }
        return new ScanRanges(schema, slotSpan, sortedRanges, scanRange, useSkipScan, isPointLookup, nBuckets, rowTimestampRange);
    }

    private SkipScanFilter filter;
    private final List<List<KeyRange>> ranges;
    private final int[] slotSpan;
    private final RowKeySchema schema;
    private final boolean isPointLookup;
    private final boolean isSalted;
    private final boolean useSkipScanFilter;
    private final KeyRange scanRange;
    private final TimeRange rowTimestampRange;

    private ScanRanges (RowKeySchema schema, int[] slotSpan, List<List<KeyRange>> ranges, KeyRange scanRange, boolean useSkipScanFilter, boolean isPointLookup, Integer bucketNum, TimeRange rowTimestampRange) {
        this.isPointLookup = isPointLookup;
        this.isSalted = bucketNum != null;
        this.useSkipScanFilter = useSkipScanFilter;
        this.scanRange = scanRange;
        this.rowTimestampRange = rowTimestampRange;

        if (isSalted && !isPointLookup) {
            ranges.set(0, SaltingUtil.generateAllSaltingRanges(bucketNum));
        }
        this.ranges = ImmutableList.copyOf(ranges);
        this.slotSpan = slotSpan;
        this.schema = schema;
        if (schema != null && !ranges.isEmpty()) {
            if (!this.useSkipScanFilter) {
                int boundSlotCount = this.getBoundSlotCount();
                ranges = ranges.subList(0, boundSlotCount);
                slotSpan = Arrays.copyOf(slotSpan, boundSlotCount);
            }
            this.filter = new SkipScanFilter(ranges, slotSpan, this.schema);
        }
    }
    
    public void initializeScan(Scan scan) {
        scan.setStartRow(scanRange.getLowerRange());
        scan.setStopRow(scanRange.getUpperRange());
    }
    
    public static byte[] prefixKey(byte[] key, int keyOffset, byte[] prefixKey, int prefixKeyOffset) {
        return prefixKey(key, keyOffset, key.length, prefixKey, prefixKeyOffset);
    }

    public static byte[] prefixKey(byte[] key, int keyOffset, int keyLength, byte[] prefixKey,
            int prefixKeyOffset) {
        if (keyLength > 0) {
            byte[] newKey = new byte[keyLength + prefixKeyOffset];
            int totalKeyOffset = keyOffset + prefixKeyOffset;
            if (prefixKey.length >= totalKeyOffset) { // otherwise it's null padded
                System.arraycopy(prefixKey, 0, newKey, 0, totalKeyOffset);
            }
            System.arraycopy(key, keyOffset, newKey, totalKeyOffset, keyLength - keyOffset);
            return newKey;
        } 
        return key;
    }
    
    private static byte[] replaceSaltByte(byte[] key, byte[] saltKey) {
        if (key.length == 0) {
            return key;
        }
        byte[] temp = new byte[key.length];
        if (saltKey.length >= SaltingUtil.NUM_SALTING_BYTES) { // Otherwise it's null padded
            System.arraycopy(saltKey, 0, temp, 0, SaltingUtil.NUM_SALTING_BYTES);
        }
        System.arraycopy(key, SaltingUtil.NUM_SALTING_BYTES, temp, SaltingUtil.NUM_SALTING_BYTES, key.length - SaltingUtil.NUM_SALTING_BYTES);
        return temp;
    }
    
    public static byte[] stripPrefix(byte[] key, int keyOffset) {
        if (key.length == 0) {
            return key;
        }
        byte[] temp = new byte[key.length - keyOffset];
        System.arraycopy(key, keyOffset, temp, 0, key.length - keyOffset);
        return temp;
    }
    
    public Scan intersectScan(Scan scan, final byte[] originalStartKey, final byte[] originalStopKey, final int keyOffset, boolean crossesRegionBoundary) {
        byte[] startKey = originalStartKey;
        byte[] stopKey = originalStopKey;
        if (stopKey.length > 0 && Bytes.compareTo(startKey, stopKey) >= 0) { 
            return null; 
        }
        // Keep the keys as they are if we have a point lookup, as we've already resolved the
        // salt bytes in that case.
        final int scanKeyOffset = this.isSalted && !this.isPointLookup ? SaltingUtil.NUM_SALTING_BYTES : 0;
        assert (scanKeyOffset == 0 || keyOffset == 0);
        // Total offset for startKey/stopKey. Either 1 for salted tables or the prefix length
        // of the current region for local indexes. We'll never have a case where a table is
        // both salted and local.
        final int totalKeyOffset = scanKeyOffset + keyOffset;
        byte[] prefixBytes = ByteUtil.EMPTY_BYTE_ARRAY;
        if (totalKeyOffset > 0) {
            prefixBytes = ScanUtil.getPrefix(startKey, totalKeyOffset);
            /*
             * If our startKey to stopKey crosses a region boundary consider everything after the startKey as our scan
             * is always done within a single region. This prevents us from having to prefix the key prior to knowing
             * whether or not there may be an intersection. We can't calculate whether or not we've crossed a region
             * boundary for local indexes, because we don't know the key offset of the next region, but only for the
             * current one (which is the one passed in). If the next prefix happened to be a subset of the previous
             * prefix, then this wouldn't detect that we crossed a region boundary.
             */
            if (crossesRegionBoundary) {
                stopKey = ByteUtil.EMPTY_BYTE_ARRAY;
            }
        }
        int scanStartKeyOffset = scanKeyOffset;
        byte[] scanStartKey = scan == null ? this.scanRange.getLowerRange() : scan.getStartRow();
        // Compare ignoring key prefix and salt byte
        if (scanStartKey.length - scanKeyOffset > 0) {
            if (startKey.length - totalKeyOffset > 0) {
                if (Bytes.compareTo(scanStartKey, scanKeyOffset, scanStartKey.length - scanKeyOffset, startKey, totalKeyOffset, startKey.length - totalKeyOffset) < 0) {
                    scanStartKey = startKey;
                    scanStartKeyOffset = totalKeyOffset;
                }
            }
        } else {
            scanStartKey = startKey;
            scanStartKeyOffset = totalKeyOffset;
        }
        int scanStopKeyOffset = scanKeyOffset;
        byte[] scanStopKey = scan == null ? this.scanRange.getUpperRange() : scan.getStopRow();
        if (scanStopKey.length - scanKeyOffset > 0) {
            if (stopKey.length - totalKeyOffset > 0) {
                if (Bytes.compareTo(scanStopKey, scanKeyOffset, scanStopKey.length - scanKeyOffset, stopKey, totalKeyOffset, stopKey.length - totalKeyOffset) > 0) {
                    scanStopKey = stopKey;
                    scanStopKeyOffset = totalKeyOffset;
                }
            }
        } else {
            scanStopKey = stopKey;
            scanStopKeyOffset = totalKeyOffset;
        }
        
        // If not scanning anything, return null
        if (scanStopKey.length - scanStopKeyOffset > 0 && 
            Bytes.compareTo(scanStartKey, scanStartKeyOffset, scanStartKey.length - scanStartKeyOffset, 
                            scanStopKey, scanStopKeyOffset, scanStopKey.length - scanStopKeyOffset) >= 0) {
            return null;
        }
        if (originalStopKey.length != 0 && scanStopKey.length == 0) {
            scanStopKey = originalStopKey;
        }
        Filter newFilter = null;
        // Only if the scan is using skip scan filter, intersect and replace the filter.
        // For example, we may be forcing a range scan, in which case we do not want to
        // intersect the start/stop with the filter. Instead, we rely only on the scan
        // start/stop or the scanRanges start/stop.
        if (this.useSkipScanFilter()) {
            byte[] skipScanStartKey = scanStartKey;
            byte[] skipScanStopKey = scanStopKey;
            // If we have a keyOffset and we've used the startKey/stopKey that
            // were passed in (which have the prefix) for the above range check,
            // we need to remove the prefix before running our intersect method.
            if (scanKeyOffset > 0) {
                if (skipScanStartKey != originalStartKey) { // original already has correct salt byte
                    skipScanStartKey = replaceSaltByte(skipScanStartKey, prefixBytes);
                }
                if (skipScanStopKey != originalStopKey) {
                    skipScanStopKey = replaceSaltByte(skipScanStopKey, prefixBytes);
                }
            } else if (keyOffset > 0) {
                if (skipScanStartKey == originalStartKey) {
                    skipScanStartKey = stripPrefix(skipScanStartKey, keyOffset);
                }
                if (skipScanStopKey == originalStopKey) {
                    skipScanStopKey = stripPrefix(skipScanStopKey, keyOffset);
                }
            }
            if (scan == null) {
                return filter.hasIntersect(skipScanStartKey, skipScanStopKey) ? HAS_INTERSECTION : null;
            }
            Filter filter = scan.getFilter();
            SkipScanFilter newSkipScanFilter = null;
            if (filter instanceof SkipScanFilter) {
                SkipScanFilter oldSkipScanFilter = (SkipScanFilter)filter;
                newFilter = newSkipScanFilter = oldSkipScanFilter.intersect(skipScanStartKey, skipScanStopKey);
                if (newFilter == null) {
                    return null;
                }
            } else if (filter instanceof FilterList) {
                FilterList oldList = (FilterList)filter;
                FilterList newList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                newFilter = newList;
                for (Filter f : oldList.getFilters()) {
                    if (f instanceof SkipScanFilter) {
                        newSkipScanFilter = ((SkipScanFilter)f).intersect(skipScanStartKey, skipScanStopKey);
                        if (newSkipScanFilter == null) {
                            return null;
                        }
                        newList.addFilter(newSkipScanFilter);
                    } else {
                        newList.addFilter(f);
                    }
                }
            }
            // TODO: it seems that our SkipScanFilter or HBase runs into problems if we don't
            // have an enclosing range when we do a point lookup.
            if (isPointLookup) {
                scanStartKey = ScanUtil.getMinKey(schema, newSkipScanFilter.getSlots(), slotSpan);
                scanStopKey = ScanUtil.getMaxKey(schema, newSkipScanFilter.getSlots(), slotSpan);
            }
        }
        // If we've got this far, we know we have an intersection
        if (scan == null) {
            return HAS_INTERSECTION;
        }
        if (newFilter == null) {
            newFilter = scan.getFilter();
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
        // Don't let the stopRow of the scan go beyond the originalStopKey
        if (originalStopKey.length > 0 && Bytes.compareTo(scanStopKey, originalStopKey) > 0) {
            scanStopKey = originalStopKey;
        }
        if (scanStopKey.length > 0 && Bytes.compareTo(scanStartKey, scanStopKey) >= 0) { 
            return null; 
        }
        newScan.setAttribute(SCAN_ACTUAL_START_ROW, scanStartKey);
        newScan.setStartRow(scanStartKey);
        newScan.setStopRow(scanStopKey);
        return newScan;
    }

    /**
     * Return true if the region with the start and end key
     * intersects with the scan ranges and false otherwise. 
     * @param regionStartKey lower inclusive key
     * @param regionEndKey upper exclusive key
     * @param isLocalIndex true if the table being scanned is a local index
     * @return true if the scan range intersects with the specified lower/upper key
     * range
     */
    public boolean intersectRegion(byte[] regionStartKey, byte[] regionEndKey, boolean isLocalIndex) {
        if (isEverything()) {
            return true;
        }
        if (isDegenerate()) {
            return false;
        }
        // Every range intersects all regions of a local index table 
        if (isLocalIndex) {
            return true;
        }
        
        boolean crossesSaltBoundary = isSalted && ScanUtil.crossesPrefixBoundary(regionEndKey,
                ScanUtil.getPrefix(regionStartKey, SaltingUtil.NUM_SALTING_BYTES), 
                SaltingUtil.NUM_SALTING_BYTES);        
        return intersectScan(null, regionStartKey, regionEndKey, 0, crossesSaltBoundary) == HAS_INTERSECTION;
    }
    
    public SkipScanFilter getSkipScanFilter() {
        return filter;
    }
    
    public List<List<KeyRange>> getRanges() {
        return ranges;
    }

    public List<List<KeyRange>> getBoundRanges() {
        return ranges.subList(0, getBoundSlotCount());
    }

    public RowKeySchema getSchema() {
        return schema;
    }

    public boolean isEverything() {
        return this == EVERYTHING || (!ranges.isEmpty() && ranges.get(0).get(0) == KeyRange.EVERYTHING_RANGE);
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
    
    /**
     * Finds the total number of row keys spanned by this ranges / slotSpan pair.
     * This accounts for slots in the ranges that may span more than on row key.
     * @param ranges  the KeyRange slots paired with this slotSpan. corresponds to {@link ScanRanges#ranges}
     * @param slotSpan  the extra span per skip scan slot. corresponds to {@link ScanRanges#slotSpan}
     * @return  the total number of row keys spanned yb this ranges / slotSpan pair.
     */
    private static int getBoundPkSpan(List<List<KeyRange>> ranges, int[] slotSpan) {
        int count = 0;
        boolean hasUnbound = false;
        int nRanges = ranges.size();

        for(int i = 0; i < nRanges && !hasUnbound; i++) {
            List<KeyRange> orRanges = ranges.get(i);
            for (KeyRange range : orRanges) {
                if (range == KeyRange.EVERYTHING_RANGE) {
                    return count;
                }
                if (range.isUnbound()) {
                    hasUnbound = true;
                }
            }
            count += slotSpan[i] + 1;
        }

        return count;
    }

    private static boolean isFullyQualified(RowKeySchema schema, List<List<KeyRange>> ranges, int[] slotSpan) {
        return getBoundPkSpan(ranges, slotSpan) == schema.getMaxFields();
    }
    
    private static boolean isPointLookup(RowKeySchema schema, List<List<KeyRange>> ranges, int[] slotSpan, boolean useSkipScan) {
        if (!isFullyQualified(schema, ranges, slotSpan)) {
            return false;
        }
        int lastIndex = ranges.size()-1;
        for (int i = lastIndex; i >= 0; i--) {
            List<KeyRange> orRanges = ranges.get(i);
            if (!useSkipScan && orRanges.size() > 1) {
                return false;
            }
            for (KeyRange keyRange : orRanges) {
                // Special case for single trailing IS NULL. We cannot consider this as a point key because
                // we strip trailing nulls when we form the key.
                if (!keyRange.isSingleKey() || (i == lastIndex && keyRange == KeyRange.IS_NULL_RANGE)) {
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

    /**
     * @return true if this range is salted - i.e. has a salt range
     */
    public boolean isSalted() {
        return isSalted;
    }

    public int getPointLookupCount() {
        return getPointLookupCount(isPointLookup, ranges);
    }
    
    private static int getPointLookupCount(boolean isPointLookup, List<List<KeyRange>> ranges) {
        return isPointLookup ? ranges.get(0).size() : 0;
    }
    
    public Iterator<KeyRange> getPointLookupKeyIterator() {
        return isPointLookup ? ranges.get(0).iterator() : Collections.<KeyRange>emptyIterator();
    }

    public int getBoundPkColumnCount() {
        return getBoundPkSpan(ranges, slotSpan);
    }

    public int getBoundSlotCount() {
        int count = 0;
        boolean hasUnbound = false;
        int nRanges = ranges.size();

        for(int i = 0; i < nRanges && !hasUnbound; i++) {
            List<KeyRange> orRanges = ranges.get(i);
            for (KeyRange range : orRanges) {
                if (range == KeyRange.EVERYTHING_RANGE) {
                    return count;
                }
                if (range.isUnbound()) {
                    hasUnbound = true;
                }
            }
            count++;
        }

        return count;
    }

    @Override
    public String toString() {
        return "ScanRanges[" + ranges.toString() + "]";
    }

    public int[] getSlotSpans() {
        return slotSpan;
    }
    
    public KeyRange getScanRange() {
        return scanRange;
    }

    public boolean hasEqualityConstraint(int pkPosition) {
        int pkOffset = 0;
        int nRanges = ranges.size();

        for(int i = 0; i < nRanges; i++) {
            if (pkOffset + slotSpan[i] >= pkPosition) {
                List<KeyRange> range = ranges.get(i);
                return range.size() == 1 && range.get(0).isSingleKey();
            }
            pkOffset += slotSpan[i] + 1;
        }

        return false;
        
    }
    
    private static TimeRange getRowTimestampColumnRange(List<List<KeyRange>> ranges, RowKeySchema schema, int rowTimestampColPos) {
        try {
            if (rowTimestampColPos != -1) {
                if (ranges != null && ranges.size() > rowTimestampColPos) {
                    List<KeyRange> rowTimestampColRange = ranges.get(rowTimestampColPos);
                    List<KeyRange> sortedRange = new ArrayList<>(rowTimestampColRange);
                    Field f = schema.getField(rowTimestampColPos);
                    Collections.sort(sortedRange, f.getSortOrder() == SortOrder.ASC ? KeyRange.COMPARATOR : KeyRange.DESC_COMPARATOR);
                    SortOrder order = f.getSortOrder();
                    KeyRange lowestRange = sortedRange.get(0);
                    KeyRange highestRange = sortedRange.get(rowTimestampColRange.size() - 1);
                    if (order == SortOrder.DESC) {
                        return getDescTimeRange(lowestRange, highestRange, f);
                    }
                    return getAscTimeRange( lowestRange, highestRange, f);
                }
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return null;
    }

    private static TimeRange getAscTimeRange(KeyRange lowestRange, KeyRange highestRange, Field f)
            throws IOException {
        long low;
        long high;
        PDataCodec codec = PLong.INSTANCE.getCodec();
        if (lowestRange.lowerUnbound()) {
            low = 0;
        } else {
            long lowerRange = codec.decodeLong(lowestRange.getLowerRange(), 0, SortOrder.ASC);
            low = lowestRange.isLowerInclusive() ? lowerRange : safelyIncrement(lowerRange);
        }
        if (highestRange.upperUnbound()) {
            high = HConstants.LATEST_TIMESTAMP;
        } else {
            long upperRange = codec.decodeLong(highestRange.getUpperRange(), 0, SortOrder.ASC);
            if (highestRange.isUpperInclusive()) {
                high = safelyIncrement(upperRange);
            } else {
                high = upperRange;
            }
        }
        return new TimeRange(low, high);
    }
    
    public static TimeRange getDescTimeRange(KeyRange lowestKeyRange, KeyRange highestKeyRange, Field f) throws IOException {
        boolean lowerUnbound = lowestKeyRange.lowerUnbound();
        boolean lowerInclusive = lowestKeyRange.isLowerInclusive();
        boolean upperUnbound = highestKeyRange.upperUnbound();
        boolean upperInclusive = highestKeyRange.isUpperInclusive();
        PDataCodec codec = PLong.INSTANCE.getCodec();
        long low = lowerUnbound ? -1 : codec.decodeLong(lowestKeyRange.getLowerRange(), 0, SortOrder.DESC);
        long high = upperUnbound ? -1 : codec.decodeLong(highestKeyRange.getUpperRange(), 0, SortOrder.DESC);
        long newHigh;
        long newLow;
        if (!lowerUnbound && !upperUnbound) {
            newHigh = lowerInclusive ? safelyIncrement(low) : low;
            newLow = upperInclusive ? high : safelyIncrement(high);
            return new TimeRange(newLow, newHigh);
        } else if (!lowerUnbound && upperUnbound) {
            newHigh = lowerInclusive ? safelyIncrement(low) : low;
            newLow = 0;
            return new TimeRange(newLow, newHigh);
        } else if (lowerUnbound && !upperUnbound) {
            newLow = upperInclusive ? high : safelyIncrement(high);
            newHigh = HConstants.LATEST_TIMESTAMP;
            return new TimeRange(newLow, newHigh);
        } else {
            newLow = 0;
            newHigh = HConstants.LATEST_TIMESTAMP;
            return new TimeRange(newLow, newHigh);
        }
    }
    
    private static long safelyIncrement(long value) {
        return value < HConstants.LATEST_TIMESTAMP ? (value + 1) : HConstants.LATEST_TIMESTAMP;
    }
    
    public TimeRange getRowTimestampRange() {
        return rowTimestampRange;
    }

}
