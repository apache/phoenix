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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;

import com.google.common.collect.Lists;


/**
 * 
 * Various utilities for scans
 *
 * 
 * @since 0.1
 */
public class ScanUtil {

    private ScanUtil() {
    }

    public static void setTenantId(Scan scan, byte[] tenantId) {
        scan.setAttribute(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
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

    public static Scan newScan(Scan scan) {
        try {
            Scan newScan = new Scan(scan);
            // Clone the underlying family map instead of sharing it between
            // the existing and cloned Scan (which is the retarded default
            // behavior).
            TreeMap<byte [], NavigableSet<byte []>> existingMap = (TreeMap<byte[], NavigableSet<byte[]>>)scan.getFamilyMap();
            Map<byte [], NavigableSet<byte []>> clonedMap = new TreeMap<byte [], NavigableSet<byte []>>(existingMap);
            newScan.setFamilyMap(clonedMap);
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
                FilterList filterList = (FilterList)filter;
                Filter firstFilter = filterList.getFilters().get(0);
                if (firstFilter instanceof SkipScanFilter) {
                    SkipScanFilter oldFilter = (SkipScanFilter)firstFilter;
                    SkipScanFilter newFilter = oldFilter.intersect(startKey, stopKey);
                    if (newFilter == null) {
                        return false;
                    }
                    // Intersect found: replace skip scan with intersected one
                    List<Filter> allFilters = new ArrayList<Filter>(filterList.getFilters().size());
                    allFilters.addAll(filterList.getFilters());
                    allFilters.set(0, newFilter);
                    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,allFilters));
                }
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

    public static byte[] getMinKey(RowKeySchema schema, List<List<KeyRange>> slots) {
        return getKey(schema, slots, Bound.LOWER);
    }

    public static byte[] getMaxKey(RowKeySchema schema, List<List<KeyRange>> slots) {
        return getKey(schema, slots, Bound.UPPER);
    }

    private static byte[] getKey(RowKeySchema schema, List<List<KeyRange>> slots, Bound bound) {
        if (slots.isEmpty()) {
            return null;
        }
        int[] position = new int[slots.size()];
        int maxLength = 0;
        for (int i = 0; i < position.length; i++) {
            position[i] = bound == Bound.LOWER ? 0 : slots.get(i).size()-1;
            KeyRange range = slots.get(i).get(position[i]);
            maxLength += range.getRange(bound).length + (schema.getField(i).getDataType().isFixedWidth() ? 0 : 1);
        }
        byte[] key = new byte[maxLength];
        int length = setKey(schema, slots, position, bound, key, 0, 0, position.length);
        if (length == 0) {
            return null;
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
    public static int setKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] position, Bound bound,
            byte[] key, int byteOffset, int slotStartIndex, int slotEndIndex) {
        return setKey(schema, slots, position, bound, key, byteOffset, slotStartIndex, slotEndIndex, slotStartIndex);
    }

    public static int setKey(RowKeySchema schema, List<List<KeyRange>> slots, int[] position, Bound bound,
            byte[] key, int byteOffset, int slotStartIndex, int slotEndIndex, int schemaStartIndex) {
        int offset = byteOffset;
        boolean lastInclusiveUpperSingleKey = false;
        boolean anyInclusiveUpperRangeKey = false;
        for (int i = slotStartIndex; i < slotEndIndex; i++) {
            // Build up the key by appending the bound of each key range
            // from the current position of each slot. 
            KeyRange range = slots.get(i).get(position[i]);
            boolean isFixedWidth = schema.getField(schemaStartIndex++).getDataType().isFixedWidth();
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
            
            if (!isFixedWidth && ( i < schema.getMaxFields()-1 || inclusiveUpper || exclusiveLower)) {
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
            while (schemaStartIndex > 0 && offset > byteOffset && 
                    !schema.getField(--schemaStartIndex).getDataType().isFixedWidth() && 
                    key[offset-1] == QueryConstants.SEPARATOR_BYTE) {
                offset--;
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
            keys.add(PDataType.VARBINARY.getKeyRange(m.getRow()));
        }
        ScanRanges keyRanges = ScanRanges.create(Collections.singletonList(keys), SchemaUtil.VAR_BINARY_SCHEMA);
        return keyRanges;
    }

    public static byte[] nextKey(byte[] key, PTable table, ImmutableBytesWritable ptr) {
        int pos = 0;
        RowKeySchema schema = table.getRowKeySchema();
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

    private static final String REVERSED_ATTR = "_reversed_";
    
    public static void setReversed(Scan scan) {
        // TODO: set attribute dynamically here to prevent dependency on newer HBase release
        scan.setAttribute(REVERSED_ATTR, PDataType.TRUE_BYTES);
    }

    // Start/stop row must be swapped if scan is being done in reverse
    public static void swapStartStopRowIfReversed(Scan scan) {
        if (isReversed(scan)) {
            byte[] startRow = scan.getStartRow();
            byte[] stopRow = scan.getStopRow();
            scan.setStartRow(stopRow);
            scan.setStopRow(startRow);
        }
    }

    public static boolean isReversed(Scan scan) {
        byte[] reversed = scan.getAttribute(REVERSED_ATTR);
        return (PDataType.TRUE_BYTES.equals(reversed));
    }
}