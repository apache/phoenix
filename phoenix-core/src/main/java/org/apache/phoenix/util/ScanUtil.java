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
package org.apache.phoenix.util;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CUSTOM_ANNOTATIONS;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.filter.BooleanExpressionFilter;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * 
 * Various utilities for scans
 *
 * 
 * @since 0.1
 */
public class ScanUtil {
    public static final int[] SINGLE_COLUMN_SLOT_SPAN = new int[1];
    /*
     * Max length that we fill our key when we turn an inclusive key
     * into a exclusive key.
     */
    private static final byte[] MAX_FILL_LENGTH_FOR_PREVIOUS_KEY = new byte[16];
    static {
        Arrays.fill(MAX_FILL_LENGTH_FOR_PREVIOUS_KEY, (byte)-1);
    }
    private static final byte[] ZERO_BYTE_ARRAY = new byte[1024];

    private ScanUtil() {
    }

    public static void setTenantId(Scan scan, byte[] tenantId) {
        scan.setAttribute(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    }

    public static void setLocalIndex(Scan scan) {
        scan.setAttribute(BaseScannerRegionObserver.LOCAL_INDEX, PDataType.TRUE_BYTES);
    }

    public static boolean isLocalIndex(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserver.LOCAL_INDEX) != null;
    }

    // Use getTenantId and pass in column name to match against
    // in as PSchema attribute. If column name matches in 
    // KeyExpressions, set on scan as attribute
    public static ImmutableBytesWritable getTenantId(Scan scan) {
        // Create Scan with special aggregation column over which to aggregate
        byte[] tenantId = scan.getAttribute(PhoenixRuntime.TENANT_ID_ATTRIB);
        if (tenantId == null) {
            return null;
        }
        return new ImmutableBytesWritable(tenantId);
    }
    
    public static void setCustomAnnotations(Scan scan, byte[] annotations) {
    	scan.setAttribute(CUSTOM_ANNOTATIONS, annotations);
    }
    
    public static byte[] getCustomAnnotations(Scan scan) {
    	return scan.getAttribute(CUSTOM_ANNOTATIONS);
    }

    public static Scan newScan(Scan scan) {
        try {
            Scan newScan = new Scan(scan);
            // Clone the underlying family map instead of sharing it between
            // the existing and cloned Scan (which is the retarded default
            // behavior).
            TreeMap<byte [], NavigableSet<byte []>> existingMap = (TreeMap<byte[], NavigableSet<byte[]>>)scan.getFamilyMap();
            Map<byte [], NavigableSet<byte []>> clonedMap = new TreeMap<byte [], NavigableSet<byte []>>(existingMap);
            newScan.setFamilyMap(clonedMap);
            // Carry over the reversed attribute
            newScan.setReversed(scan.isReversed());
            newScan.setSmall(scan.isSmall());
            return newScan;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Intersects the scan start/stop row with the startKey and stopKey
     * @param scan
     * @param startKey
     * @param stopKey
     * @return false if the Scan cannot possibly return rows and true otherwise
     */
    public static boolean intersectScanRange(Scan scan, byte[] startKey, byte[] stopKey) {
        return intersectScanRange(scan, startKey, stopKey, false);
    }

    public static boolean intersectScanRange(Scan scan, byte[] startKey, byte[] stopKey, boolean useSkipScan) {
        boolean mayHaveRows = false;
        int offset = 0;
        if (ScanUtil.isLocalIndex(scan)) {
            offset = startKey.length != 0 ? startKey.length : stopKey.length;
        }
        byte[] existingStartKey = scan.getStartRow();
        byte[] existingStopKey = scan.getStopRow();
        if (existingStartKey.length > 0) {
            if (startKey.length == 0 || Bytes.compareTo(existingStartKey, startKey) > 0) {
                startKey = existingStartKey;
            }
        } else {
            mayHaveRows = true;
        }
        if (existingStopKey.length > 0) {
            if (stopKey.length == 0 || Bytes.compareTo(existingStopKey, stopKey) < 0) {
                stopKey = existingStopKey;
            }
        } else {
            mayHaveRows = true;
        }
        scan.setStartRow(startKey);
        scan.setStopRow(stopKey);
        if (offset > 0 && useSkipScan) {
            byte[] temp = null;
            if (startKey.length != 0) {
                temp =new byte[startKey.length - offset];
                System.arraycopy(startKey, offset, temp, 0, startKey.length - offset);
                startKey = temp;
            }
            if (stopKey.length != 0) {
                temp = new byte[stopKey.length - offset];
                System.arraycopy(stopKey, offset, temp, 0, stopKey.length - offset);
                stopKey = temp;
            }
        }
        mayHaveRows = mayHaveRows || Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) < 0;
        
        // If the scan is using skip scan filter, intersect and replace the filter.
        if (mayHaveRows && useSkipScan) {
            Filter filter = scan.getFilter();
            if (filter instanceof SkipScanFilter) {
                SkipScanFilter oldFilter = (SkipScanFilter)filter;
                SkipScanFilter newFilter = oldFilter.intersect(startKey, stopKey);
                if (newFilter == null) {
                    return false;
                }
                // Intersect found: replace skip scan with intersected one
                scan.setFilter(newFilter);
            } else if (filter instanceof FilterList) {
                FilterList oldList = (FilterList)filter;
                FilterList newList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                for (Filter f : oldList.getFilters()) {
                    if (f instanceof SkipScanFilter) {
                        SkipScanFilter newFilter = ((SkipScanFilter)f).intersect(startKey, stopKey);
                        if (newFilter == null) {
                            return false;
                        }
                        newList.addFilter(newFilter);
                    } else {
                        newList.addFilter(f);
                    }
                }
                scan.setFilter(newList);
           }
        }
        return mayHaveRows;
    }

    public static void andFilterAtBeginning(Scan scan, Filter andWithFilter) {
        if (andWithFilter == null) {
            return;
        }
        Filter filter = scan.getFilter();
        if (filter == null) {
            scan.setFilter(andWithFilter); 
        } else if (filter instanceof FilterList && ((FilterList)filter).getOperator() == FilterList.Operator.MUST_PASS_ALL) {
            FilterList filterList = (FilterList)filter;
            List<Filter> allFilters = new ArrayList<Filter>(filterList.getFilters().size() + 1);
            allFilters.add(andWithFilter);
            allFilters.addAll(filterList.getFilters());
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,allFilters));
        } else {
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,Arrays.asList(andWithFilter, filter)));
        }
    }

    public static void andFilterAtEnd(Scan scan, Filter andWithFilter) {
        if (andWithFilter == null) {
            return;
        }
        Filter filter = scan.getFilter();
        if (filter == null) {
            scan.setFilter(andWithFilter); 
        } else if (filter instanceof FilterList && ((FilterList)filter).getOperator() == FilterList.Operator.MUST_PASS_ALL) {
            FilterList filterList = (FilterList)filter;
            List<Filter> allFilters = new ArrayList<Filter>(filterList.getFilters().size() + 1);
            allFilters.addAll(filterList.getFilters());
            allFilters.add(andWithFilter);
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,allFilters));
        } else {
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,Arrays.asList(filter, andWithFilter)));
        }
    }

    public static void setTimeRange(Scan scan, long ts) {
        try {
            scan.setTimeRange(MetaDataProtocol.MIN_TABLE_TIMESTAMP, ts);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setTimeRange(Scan scan, TimeRange range) {
        try {
            scan.setTimeRange(range.getMin(), range.getMax());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] getMinKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] slotSpan) {
        return getKey(schema, slots, slotSpan, Bound.LOWER);
    }

    public static byte[] getMaxKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] slotSpan) {
        return getKey(schema, slots, slotSpan, Bound.UPPER);
    }

    private static byte[] getKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] slotSpan, Bound bound) {
        if (slots.isEmpty()) {
            return KeyRange.UNBOUND;
        }
        int[] position = new int[slots.size()];
        int maxLength = 0;
        for (int i = 0; i < position.length; i++) {
            position[i] = bound == Bound.LOWER ? 0 : slots.get(i).size()-1;
            KeyRange range = slots.get(i).get(position[i]);
            maxLength += range.getRange(bound).length + (schema.getField(i + slotSpan[i]).getDataType().isFixedWidth() ? 0 : 1);
        }
        byte[] key = new byte[maxLength];
        int length = setKey(schema, slots, slotSpan, position, bound, key, 0, 0, position.length);
        if (length == 0) {
            return KeyRange.UNBOUND;
        }
        if (length == maxLength) {
            return key;
        }
        byte[] keyCopy = new byte[length];
        System.arraycopy(key, 0, keyCopy, 0, length);
        return keyCopy;
    }

    /*
     * Set the key by appending the keyRanges inside slots at positions as specified by the position array.
     * 
     * We need to increment part of the key range, or increment the whole key at the end, depending on the
     * bound we are setting and whether the key range is inclusive or exclusive. The logic for determining
     * whether to increment or not is:
     * range/single    boundary       bound      increment
     *  range          inclusive      lower         no
     *  range          inclusive      upper         yes, at the end if occurs at any slots.
     *  range          exclusive      lower         yes
     *  range          exclusive      upper         no
     *  single         inclusive      lower         no
     *  single         inclusive      upper         yes, at the end if it is the last slots.
     */
    public static int setKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] slotSpan, int[] position,
            Bound bound, byte[] key, int byteOffset, int slotStartIndex, int slotEndIndex) {
        return setKey(schema, slots, slotSpan, position, bound, key, byteOffset, slotStartIndex, slotEndIndex, slotStartIndex);
    }

    public static int setKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] slotSpan, int[] position,
            Bound bound, byte[] key, int byteOffset, int slotStartIndex, int slotEndIndex, int schemaStartIndex) {
        int offset = byteOffset;
        boolean lastInclusiveUpperSingleKey = false;
        boolean anyInclusiveUpperRangeKey = false;
        // The index used for slots should be incremented by 1,
        // but the index for the field it represents in the schema
        // should be incremented by 1 + value in the current slotSpan index
        // slotSpan stores the number of columns beyond one that the range spans
        int i = slotStartIndex, fieldIndex = ScanUtil.getRowKeyPosition(slotSpan, slotStartIndex);
        for (i = slotStartIndex; i < slotEndIndex; i++) {
            // Build up the key by appending the bound of each key range
            // from the current position of each slot. 
            KeyRange range = slots.get(i).get(position[i]);
            // Use last slot in a multi-span column to determine if fixed width
            boolean isFixedWidth = schema.getField(fieldIndex + slotSpan[i]).getDataType().isFixedWidth();
            fieldIndex += slotSpan[i] + 1;
            /*
             * If the current slot is unbound then stop if:
             * 1) setting the upper bound. There's no value in
             *    continuing because nothing will be filtered.
             * 2) setting the lower bound when the type is fixed length
             *    for the same reason. However, if the type is variable width
             *    continue building the key because null values will be filtered
             *    since our separator byte will be appended and incremented.
             */
            if (  range.isUnbound(bound) &&
                ( bound == Bound.UPPER || isFixedWidth) ){
                break;
            }
            byte[] bytes = range.getRange(bound);
            System.arraycopy(bytes, 0, key, offset, bytes.length);
            offset += bytes.length;
            /*
             * We must add a terminator to a variable length key even for the last PK column if
             * the lower key is non inclusive or the upper key is inclusive. Otherwise, we'd be
             * incrementing the key value itself, and thus bumping it up too much.
             */
            boolean inclusiveUpper = range.isInclusive(bound) && bound == Bound.UPPER;
            boolean exclusiveLower = !range.isInclusive(bound) && bound == Bound.LOWER;
            // If we are setting the upper bound of using inclusive single key, we remember 
            // to increment the key if we exit the loop after this iteration.
            // 
            // We remember to increment the last slot if we are setting the upper bound with an
            // inclusive range key.
            //
            // We cannot combine the two flags together in case for single-inclusive key followed
            // by the range-exclusive key. In that case, we do not need to increment the end at the
            // end. But if we combine the two flag, the single inclusive key in the middle of the
            // key slots would cause the flag to become true.
            lastInclusiveUpperSingleKey = range.isSingleKey() && inclusiveUpper;
            anyInclusiveUpperRangeKey |= !range.isSingleKey() && inclusiveUpper;
            
            if (!isFixedWidth && ( fieldIndex < schema.getMaxFields() || inclusiveUpper || exclusiveLower)) {
                key[offset++] = QueryConstants.SEPARATOR_BYTE;
                // Set lastInclusiveUpperSingleKey back to false if this is the last pk column
                // as we don't want to increment the null byte in this case
                lastInclusiveUpperSingleKey &= i < schema.getMaxFields()-1;
            }
            // If we are setting the lower bound with an exclusive range key, we need to bump the
            // slot up for each key part. For an upper bound, we bump up an inclusive key, but
            // only after the last key part.
            if (!range.isSingleKey() && exclusiveLower) {
                if (!ByteUtil.nextKey(key, offset)) {
                    // Special case for not being able to increment.
                    // In this case we return a negative byteOffset to
                    // remove this part from the key being formed. Since the
                    // key has overflowed, this means that we should not
                    // have an end key specified.
                    return -byteOffset;
                }
            }
        }
        if (lastInclusiveUpperSingleKey || anyInclusiveUpperRangeKey) {
            if (!ByteUtil.nextKey(key, offset)) {
                // Special case for not being able to increment.
                // In this case we return a negative byteOffset to
                // remove this part from the key being formed. Since the
                // key has overflowed, this means that we should not
                // have an end key specified.
                return -byteOffset;
            }
        }
        // Remove trailing separator bytes, since the columns may have been added
        // after the table has data, in which case there won't be a separator
        // byte.
        if (bound == Bound.LOWER) {
            while (--i >= schemaStartIndex && offset > byteOffset && 
                    !schema.getField(--fieldIndex).getDataType().isFixedWidth() && 
                    key[offset-1] == QueryConstants.SEPARATOR_BYTE) {
                offset--;
                fieldIndex -= slotSpan[i];
            }
        }
        return offset - byteOffset;
    }

    /**
     * Perform a binary lookup on the list of KeyRange for the tightest slot such that the slotBound
     * of the current slot is higher or equal than the slotBound of our range. 
     * @return  the index of the slot whose slot bound equals or are the tightest one that is 
     *          smaller than rangeBound of range, or slots.length if no bound can be found.
     */
    public static int searchClosestKeyRangeWithUpperHigherThanPtr(List<KeyRange> slots, ImmutableBytesWritable ptr, int lower) {
        int upper = slots.size() - 1;
        int mid;
        while (lower <= upper) {
            mid = (lower + upper) / 2;
            int cmp = slots.get(mid).compareUpperToLowerBound(ptr, true);
            if (cmp < 0) {
                lower = mid + 1;
            } else if (cmp > 0) {
                upper = mid - 1;
            } else {
                return mid;
            }
        }
        mid = (lower + upper) / 2;
        if (mid == 0 && slots.get(mid).compareUpperToLowerBound(ptr, true) > 0) {
            return mid;
        } else {
            return ++mid;
        }
    }
    
    public static ScanRanges newScanRanges(List<Mutation> mutations) throws SQLException {
        List<KeyRange> keys = Lists.newArrayListWithExpectedSize(mutations.size());
        for (Mutation m : mutations) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(m.getRow()));
        }
        ScanRanges keyRanges = ScanRanges.create(SchemaUtil.VAR_BINARY_SCHEMA, Collections.singletonList(keys), ScanUtil.SINGLE_COLUMN_SLOT_SPAN);
        return keyRanges;
    }

    /**
     * Converts a partially qualified KeyRange into a KeyRange with a
     * inclusive lower bound and an exclusive upper bound, widening
     * as necessary.
     */
    public static KeyRange convertToInclusiveExclusiveRange (KeyRange partialRange, RowKeySchema schema, ImmutableBytesWritable ptr) {
        // Ensure minMaxRange is lower inclusive and upper exclusive, as that's
        // what we need to intersect against for the HBase scan.
        byte[] lowerRange = partialRange.getLowerRange();
        if (!partialRange.lowerUnbound()) {
            if (!partialRange.isLowerInclusive()) {
                lowerRange = ScanUtil.nextKey(lowerRange, schema, ptr);
            }
        }
        
        byte[] upperRange = partialRange.getUpperRange();
        if (!partialRange.upperUnbound()) {
            if (partialRange.isUpperInclusive()) {
                upperRange = ScanUtil.nextKey(upperRange, schema, ptr);
            }
        }
        if (partialRange.getLowerRange() != lowerRange || partialRange.getUpperRange() != upperRange) {
            partialRange = KeyRange.getKeyRange(lowerRange, upperRange);
        }
        return partialRange;
    }
    
    private static byte[] nextKey(byte[] key, RowKeySchema schema, ImmutableBytesWritable ptr) {
        int pos = 0;
        int maxOffset = schema.iterator(key, ptr);
        while (schema.next(ptr, pos, maxOffset) != null) {
            pos++;
        }
        if (!schema.getField(pos-1).getDataType().isFixedWidth()) {
            byte[] newLowerRange = new byte[key.length + 1];
            System.arraycopy(key, 0, newLowerRange, 0, key.length);
            newLowerRange[key.length] = QueryConstants.SEPARATOR_BYTE;
            key = newLowerRange;
        } else {
            key = Arrays.copyOf(key, key.length);
        }
        ByteUtil.nextKey(key, key.length);
        return key;
    }

    public static boolean isReversed(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserver.REVERSE_SCAN) != null;
    }
    
    public static void setReversed(Scan scan) {
        scan.setAttribute(BaseScannerRegionObserver.REVERSE_SCAN, PDataType.TRUE_BYTES);
    }

    // Start/stop row must be swapped if scan is being done in reverse
    public static void setupReverseScan(Scan scan) {
        if (isReversed(scan)) {
            byte[] startRow = scan.getStartRow();
            byte[] stopRow = scan.getStopRow();
            byte[] newStartRow = startRow;
            byte[] newStopRow = stopRow;
            if (startRow.length != 0) {
                /*
                 * Must get previous key because this is going from an inclusive start key to an exclusive stop key, and
                 * we need the start key to be included. We get the previous key by decrementing the last byte by one.
                 * However, with variable length data types, we need to fill with the max byte value, otherwise, if the
                 * start key is 'ab', we lower it to 'aa' which would cause 'aab' to be included (which isn't correct).
                 * So we fill with a 0xFF byte to prevent this. A single 0xFF would be enough for our primitive types (as
                 * that byte wouldn't occur), but for an arbitrary VARBINARY key we can't know how many bytes to tack
                 * on. It's lame of HBase to force us to do this.
                 */
                newStartRow = Arrays.copyOf(startRow, startRow.length + MAX_FILL_LENGTH_FOR_PREVIOUS_KEY.length);
                if (ByteUtil.previousKey(newStartRow, startRow.length)) {
                    System.arraycopy(MAX_FILL_LENGTH_FOR_PREVIOUS_KEY, 0, newStartRow, startRow.length, MAX_FILL_LENGTH_FOR_PREVIOUS_KEY.length);
                } else {
                    newStartRow = HConstants.EMPTY_START_ROW;
                }
            }
            if (stopRow.length != 0) {
                // Must add null byte because we need the start to be exclusive while it was inclusive
                newStopRow = ByteUtil.concat(stopRow, QueryConstants.SEPARATOR_BYTE_ARRAY);
            }
            scan.setStartRow(newStopRow);
            scan.setStopRow(newStartRow);
            scan.setReversed(true);
        }
    }

    public static int getRowKeyOffset(byte[] regionStartKey, byte[] regionEndKey) {
        return regionStartKey.length > 0 ? regionStartKey.length : regionEndKey.length;
    }
    
    private static void setRowKeyOffset(Filter filter, int offset) {
        if (filter instanceof BooleanExpressionFilter) {
            BooleanExpressionFilter boolFilter = (BooleanExpressionFilter)filter;
            IndexUtil.setRowKeyExpressionOffset(boolFilter.getExpression(), offset);
        } else if (filter instanceof SkipScanFilter) {
            SkipScanFilter skipScanFilter = (SkipScanFilter)filter;
            skipScanFilter.setOffset(offset);
        }
    }

    public static void setRowKeyOffset(Scan scan, int offset) {
        Filter filter = scan.getFilter();
        if (filter == null) {
            return;
        }
        if (filter instanceof FilterList) {
            FilterList filterList = (FilterList)filter;
            for (Filter childFilter : filterList.getFilters()) {
                setRowKeyOffset(childFilter, offset);
            }
        } else {
            setRowKeyOffset(filter, offset);
        }
    }

    public static int[] getDefaultSlotSpans(int nSlots) {
        return new int[nSlots];
    }

    /**
     * Finds the total number of row keys spanned by this ranges / slotSpan pair.
     * This accounts for slots in the ranges that may span more than on row key.
     * @param ranges  the KeyRange slots paired with this slotSpan. corresponds to {@link ScanRanges#ranges}
     * @param slotSpan  the extra span per skip scan slot. corresponds to {@link ScanRanges#slotSpan}
     * @return  the total number of row keys spanned yb this ranges / slotSpan pair.
     * @see #getRowKeyPosition(int[], int)
     */
    public static int getTotalSpan(List<List<KeyRange>> ranges, int[] slotSpan) {
        // finds the position at the "end" of the ranges, which is also the total span
        return getRowKeyPosition(slotSpan, ranges.size());
    }

    /**
     * Finds the position in the row key schema for a given position in the scan slots.
     * For example, with a slotSpan of {0, 1, 0}, the slot at index 1 spans an extra column in the row key. This means
     * that the slot at index 2 has a slot index of 2 but a row key index of 3.
     * To calculate the "adjusted position" index, we simply add up the number of extra slots spanned and offset
     * the slotPosition by that much.
     * @param slotSpan  the extra span per skip scan slot. corresponds to {@link ScanRanges#slotSpan}
     * @param slotPosition  the index of a slot in the SkipScan slots list.
     * @return  the equivalent row key position in the RowKeySchema
     * @see #getTotalSpan(java.util.List, int[])
     */
    public static int getRowKeyPosition(int[] slotSpan, int slotPosition) {
        int offset = 0;

        for(int i = 0; i < slotPosition; i++) {
            offset += slotSpan[i];
        }

        return offset + slotPosition;
    }

    public static boolean isAnalyzeTable(Scan scan) {
        return scan.getAttribute((BaseScannerRegionObserver.ANALYZE_TABLE)) != null;
    }

    public static boolean crossesPrefixBoundary(byte[] key, byte[] prefixBytes, int prefixLength) {
        if (key.length < prefixLength) {
            return true;
        }
        if (prefixBytes.length >= prefixLength) {
            return Bytes.compareTo(prefixBytes, 0, prefixLength, key, 0, prefixLength) != 0;
        }
        return hasNonZeroLeadingBytes(key, prefixLength);
    }

    public static byte[] getPrefix(byte[] startKey, int prefixLength) {
        // If startKey is at beginning, then our prefix will be a null padded byte array
        return startKey.length >= prefixLength ? startKey : ByteUtil.EMPTY_BYTE_ARRAY;
    }

    private static boolean hasNonZeroLeadingBytes(byte[] key, int nBytesToCheck) {
        if (nBytesToCheck > ZERO_BYTE_ARRAY.length) {
            do {
                if (Bytes.compareTo(key, nBytesToCheck - ZERO_BYTE_ARRAY.length, ZERO_BYTE_ARRAY.length, ScanUtil.ZERO_BYTE_ARRAY, 0, ScanUtil.ZERO_BYTE_ARRAY.length) != 0) {
                    return true;
                }
                nBytesToCheck -= ZERO_BYTE_ARRAY.length;
            } while (nBytesToCheck > ZERO_BYTE_ARRAY.length);
        }
        return Bytes.compareTo(key, 0, nBytesToCheck, ZERO_BYTE_ARRAY, 0, nBytesToCheck) != 0;
    }
    
    public static PName padTenantIdIfNecessary(RowKeySchema schema, boolean isSalted, PName tenantId) {
        int pkPos = isSalted ? 1 : 0;
        String tenantIdStr = tenantId.getString();
        Field field = schema.getField(pkPos);
        PDataType dataType = field.getDataType();
        boolean isFixedWidth = dataType.isFixedWidth();
        Integer maxLength = field.getMaxLength();
        if (isFixedWidth && maxLength != null) {
            if (tenantIdStr.length() < maxLength) {
                tenantIdStr = (String)dataType.pad(tenantIdStr, maxLength);
                return PNameFactory.newName(tenantIdStr);
            }
        }
        return tenantId;
    }

    public static Iterator<Filter> getFilterIterator(Scan scan) {
        Iterator<Filter> filterIterator;
        Filter topLevelFilter = scan.getFilter();
        if (topLevelFilter == null) {
            filterIterator = Iterators.emptyIterator();
        } else if (topLevelFilter instanceof FilterList) {
            filterIterator = ((FilterList) topLevelFilter).getFilters().iterator();
        } else {
            filterIterator = Iterators.singletonIterator(topLevelFilter);
        }
        return filterIterator;
    }
}