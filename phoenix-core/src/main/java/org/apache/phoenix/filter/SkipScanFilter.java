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
package org.apache.phoenix.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;


/**
 * 
 * Filter that seeks based on CNF containing anded and ored key ranges
 * 
 * TODO: figure out when to reset/not reset position array
 *
 * 
 * @since 0.1
 */
public class SkipScanFilter extends FilterBase implements Writable {
    private enum Terminate {AT, AFTER};
    // Conjunctive normal form of or-ed ranges or point lookups
    private List<List<KeyRange>> slots;
    // How far each slot spans minus one. We only handle a single column span currently
    private int[] slotSpan;
    // schema of the row key
    private RowKeySchema schema;
    // current position for each slot
    private int[] position;
    // buffer used for skip hint
    private int maxKeyLength;
    private byte[] startKey;
    private int startKeyLength;
    private byte[] endKey; 
    private int endKeyLength;
    private boolean isDone;
    private int offset;
    private Map<ImmutableBytesWritable, Cell> nextCellHintMap =
            new HashMap<ImmutableBytesWritable, Cell>();

    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

    /**
     * We know that initially the first row will be positioned at or 
     * after the first possible key.
     */
    public SkipScanFilter() {
    }

    public SkipScanFilter(List<List<KeyRange>> slots, RowKeySchema schema) {
        this(slots, ScanUtil.getDefaultSlotSpans(slots.size()), schema);
    }

    public SkipScanFilter(List<List<KeyRange>> slots, int[] slotSpan, RowKeySchema schema) {
        init(slots, slotSpan, schema);
    }
    
    public void setOffset(int offset) {
        this.offset = offset;
    }

    private void init(List<List<KeyRange>> slots, int[] slotSpan, RowKeySchema schema) {
        for (List<KeyRange> ranges : slots) {
            if (ranges.isEmpty()) {
                throw new IllegalStateException();
            }
        }
        this.slots = slots;
        this.slotSpan = slotSpan;
        this.schema = schema;
        this.maxKeyLength = SchemaUtil.getMaxKeyLength(schema, slots);
        this.position = new int[slots.size()];
        startKey = new byte[maxKeyLength];
        endKey = new byte[maxKeyLength];
        endKeyLength = 0;
    }

    // Exposed for testing.
    public List<List<KeyRange>> getSlots() {
        return slots;
    }

    @Override
    public boolean filterAllRemaining() {
        return isDone;
    }

    @Override
    public ReturnCode filterKeyValue(Cell kv) {
        ReturnCode code = navigate(kv.getRowArray(), kv.getRowOffset() + offset,kv.getRowLength()- offset,Terminate.AFTER);
        if (code == ReturnCode.SEEK_NEXT_USING_HINT) {
            setNextCellHint(kv);
        }
        return code;
    }

    private void setNextCellHint(Cell kv) {
        ImmutableBytesWritable family = new ImmutableBytesWritable(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());
        Cell nextCellHint = null;
        if (offset == 0) {
            nextCellHint = new KeyValue(startKey, 0, startKeyLength,
                    null, 0, 0, null, 0, 0, HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0);
        } else { // Prepend key of NextCellHint with bytes before offset
            byte[] nextKey = new byte[offset + startKeyLength];
            System.arraycopy(kv.getRowArray(), kv.getRowOffset(), nextKey, 0, offset);
            System.arraycopy(startKey, 0, nextKey, offset, startKeyLength);
            nextCellHint = new KeyValue(nextKey, 0, nextKey.length,
                    null, 0, 0, null, 0, 0, HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0);
        }
        Cell previousCellHint = nextCellHintMap.put(family, nextCellHint);
        // we should either have no previous hint, or the next hint should always come after the previous hint
        assert previousCellHint == null
                || Bytes.compareTo(nextCellHint.getRowArray(), nextCellHint.getRowOffset(),
                    nextCellHint.getRowLength(), previousCellHint.getRowArray(), previousCellHint
                            .getRowOffset(), previousCellHint.getRowLength()) > 0 : "next hint must come after previous hint (prev="
                + previousCellHint + ", next=" + nextCellHint + ", kv=" + kv + ")";
    }
    
    @Override
    public Cell getNextCellHint(Cell kv) {
        return isDone ? null : nextCellHintMap.get(new ImmutableBytesWritable(kv.getFamilyArray(),
                kv.getFamilyOffset(), kv.getFamilyLength()));
    }

    public boolean hasIntersect(byte[] lowerInclusiveKey, byte[] upperExclusiveKey) {
        return intersect(lowerInclusiveKey, upperExclusiveKey, null);
    }
    /**
     * Intersect the ranges of this filter with the ranges form by lowerInclusive and upperInclusive
     * key and filter out the ones that are not included in the region. Return the new intersected
     * SkipScanFilter or null if there is no intersection.
     */
    public SkipScanFilter intersect(byte[] lowerInclusiveKey, byte[] upperExclusiveKey) {
        List<List<KeyRange>> newSlots = Lists.newArrayListWithCapacity(slots.size());
        if (intersect(lowerInclusiveKey, upperExclusiveKey, newSlots)) {
            return new SkipScanFilter(newSlots, slotSpan, schema);
        }
        return null;
    }
    
    private boolean areSlotsSingleKey(int startPosInclusive, int endPosExclusive) {
        for (int i = startPosInclusive; i < endPosExclusive; i++) {
            if (!slots.get(i).get(position[i]).isSingleKey()) {
                return false;
            }
        }
        return true;
    }
    
    private void resetState() {
        isDone = false;
        endKeyLength = 0;
        Arrays.fill(position, 0);
    }
    
    private boolean intersect(final byte[] lowerInclusiveKey, final byte[] upperExclusiveKey, List<List<KeyRange>> newSlots) {
        resetState();
        boolean lowerUnbound = (lowerInclusiveKey.length == 0);
        int startPos = 0;
        int lastSlot = slots.size()-1;
        if (!lowerUnbound) {
            // Find the position of the first slot of the lower range
            schema.next(ptr, 0, schema.iterator(lowerInclusiveKey,ptr), slotSpan[0]);
            startPos = ScanUtil.searchClosestKeyRangeWithUpperHigherThanPtr(slots.get(0), ptr, 0);
            // Lower range is past last upper range of first slot, so cannot possibly be in range
            if (startPos >= slots.get(0).size()) {
                return false;
            }
        }
        boolean upperUnbound = (upperExclusiveKey.length == 0);
        int endPos = slots.get(0).size()-1;
        if (!upperUnbound) {
            // Find the position of the first slot of the upper range
            schema.next(ptr, 0, schema.iterator(upperExclusiveKey,ptr), slotSpan[0]);
            endPos = ScanUtil.searchClosestKeyRangeWithUpperHigherThanPtr(slots.get(0), ptr, startPos);
            // Upper range lower than first lower range of first slot, so cannot possibly be in range
//            if (endPos == 0 && Bytes.compareTo(upperExclusiveKey, slots.get(0).get(0).getLowerRange()) <= 0) {
//                return false;
//            }
            // Past last position, so we can include everything from the start position
            if (endPos >= slots.get(0).size()) {
                upperUnbound = true;
                endPos = slots.get(0).size()-1;
            } else if (slots.get(0).get(endPos).compareLowerToUpperBound(upperExclusiveKey) >= 0) {
                // We know that the endPos range is higher than the previous range, but we need
                // to test if it ends before the next range starts.
                endPos--;
            }
            if (endPos < startPos) {
                return false;
            }
            
        }
        // Short circuit out if we only have a single set of keys
        if (slots.size() == 1) {
//            int offset = slots.get(0).get(endPos).compareLowerToUpperBound(upperExclusiveKey) < 0 ? 1 : 0;
//            if (endPos + offset <= startPos) {
//                return false;
//            }
//            List<KeyRange> newRanges = slots.get(0).subList(startPos, endPos + offset);
            if (newSlots != null) {
                List<KeyRange> newRanges = slots.get(0).subList(startPos, endPos+1);
                newSlots.add(newRanges);
            }
            return true;
        }
        if (!lowerUnbound) {
            position[0] = startPos;
            navigate(lowerInclusiveKey, 0, lowerInclusiveKey.length, Terminate.AFTER);
            if (filterAllRemaining()) {
                return false;
            }
        }
        if (upperUnbound) {
            if (newSlots != null) {
                newSlots.add(slots.get(0).subList(startPos, endPos+1));
                newSlots.addAll(slots.subList(1, slots.size()));
            }
            return true;
        }
        int[] lowerPosition = Arrays.copyOf(position, position.length);
        // Navigate to the upperExclusiveKey, but not past it
        // TODO: We're including everything between the lowerPosition and end position, which is
        // more than we need. We can optimize this by tracking whether each range in each slot position
        // intersects.
        ReturnCode endCode = navigate(upperExclusiveKey, 0, upperExclusiveKey.length, Terminate.AT);
        if (endCode == ReturnCode.INCLUDE) {
            setStartKey();
            // If the upperExclusiveKey is equal to the start key, we've gone one position too far, since
            // our upper key is exclusive. In that case, go to the previous key
            if (Bytes.compareTo(startKey, 0, startKeyLength, upperExclusiveKey, 0, upperExclusiveKey.length) == 0 &&
                    (previousPosition(lastSlot) < 0 || position[0] < lowerPosition[0])) {
                // If by backing up one position we have an empty range, then return
                return false;
            }
        } else if (endCode == ReturnCode.SEEK_NEXT_USING_HINT) {
            // The upperExclusive key is smaller than the slots stored in the position. Check if it's the same position
            // as the slots for lowerInclusive. If so, there is no intersection.
            if (Arrays.equals(lowerPosition, position) && areSlotsSingleKey(0, position.length-1)) {
                return false;
            }
        } else if (filterAllRemaining()) {
            // We wrapped around the position array. We know there's an intersection, but it can only at the last
            // slot position. So reset the position array here to the last position index for each slot. This will
            // be used below as the end bounds to formulate the list of intersecting slots.
            for (int i = 0; i <= lastSlot; i++) {
                position[i] = slots.get(i).size() - 1;
            }
        }
        int prevRowKeyPos = -1;
        ImmutableBytesWritable lowerPtr = new ImmutableBytesWritable();
        ImmutableBytesWritable upperPtr = new ImmutableBytesWritable();
        schema.iterator(lowerInclusiveKey, lowerPtr);
        schema.iterator(upperExclusiveKey, upperPtr);
        // Copy inclusive all positions 
        for (int i = 0; i <= lastSlot; i++) {
            List<KeyRange> newRanges = slots.get(i).subList(lowerPosition[i], Math.min(position[i] + 1, slots.get(i).size()));
            if (newRanges.isEmpty()) {
                return false;
            }
            if (newSlots != null) {
                newSlots.add(newRanges);
            }
            // Must include all "less-significant" slot values if:
            // 1) a more-significant slot was incremented
            if (position[i] > lowerPosition[i]) {
                if (newSlots != null) {
                    newSlots.addAll(slots.subList(i+1, slots.size()));
                }
                break;
            }
            // 2) we're at a slot containing a range and the values differ between the lower and upper range,
            //    since less-significant slots may be lower after traversal than where they started.
            if (!slots.get(i).get(position[i]).isSingleKey()) {
                int rowKeyPos = ScanUtil.getRowKeyPosition(slotSpan, i);
                // Position lowerPtr/upperPtr within lowerInclusiveKey/upperExclusiveKey at value for slot i
                // The reposition method will do this incrementally, where we we're initially have prevRowKeyPos = -1. 
                schema.reposition(lowerPtr, prevRowKeyPos, rowKeyPos, 0, lowerInclusiveKey.length, slotSpan[i]);
                schema.reposition(upperPtr, prevRowKeyPos, rowKeyPos, 0, upperExclusiveKey.length, slotSpan[i]);
                // If we have a range and the values differ, we must include all slots that are less significant.
                // For example: [A-D][1,23], the lower/upper keys could be B5/C2, where the C is in range and the
                // next slot value of 2 is less than the next corresponding slot value of the 5.
                if (lowerPtr.compareTo(upperPtr) != 0) {
                    if (newSlots != null) {
                        newSlots.addAll(slots.subList(i+1, slots.size()));
                    }
                    break;
                }
                prevRowKeyPos = rowKeyPos;
            }
        }
        return true;
    }

    private int previousPosition(int i) {
        while (i >= 0 && --position[i] < 0) {
            position[i] = slots.get(i).size()-1;
            i--;
        }
        return i;
    }
    
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="QBA_QUESTIONABLE_BOOLEAN_ASSIGNMENT", 
            justification="Assignment designed to work this way.")
    private ReturnCode navigate(final byte[] currentKey, final int offset, final int length, Terminate terminate) {
        int nSlots = slots.size();
        // First check to see if we're in-range until we reach our end key
        if (endKeyLength > 0) {
            if (Bytes.compareTo(currentKey, offset, length, endKey, 0, endKeyLength) < 0) {
                return ReturnCode.INCLUDE;
            }

            // If key range of last slot is a single key, we can increment our position
            // since we know we'll be past the current row after including it.
            if (slots.get(nSlots-1).get(position[nSlots-1]).isSingleKey()) {
                if (nextPosition(nSlots-1) < 0) {
                    // Current row will be included, but we have no more
                    isDone = true;
                    return ReturnCode.NEXT_ROW;
                }
            }
            else {
                // Reset the positions to zero from the next slot after the earliest ranged slot, since the
                // next key could be bigger at this ranged slot, and smaller than the current position of
                // less significant slots.
                int earliestRangeIndex = nSlots-1;
                for (int i = 0; i < nSlots; i++) {
                    if (!slots.get(i).get(position[i]).isSingleKey()) {
                        earliestRangeIndex = i;
                        break;
                    }
                }
                Arrays.fill(position, earliestRangeIndex+1, position.length, 0);
            }
        }
        endKeyLength = 0;
        
        // We could have included the previous
        if (isDone) {
            return ReturnCode.NEXT_ROW;
        }

        int i = 0;
        boolean seek = false;
        int earliestRangeIndex = nSlots-1;
        int minOffset = offset;
        int maxOffset = schema.iterator(currentKey, minOffset, length, ptr);
        schema.next(ptr, ScanUtil.getRowKeyPosition(slotSpan, i), maxOffset, slotSpan[i]);
        while (true) {
            // Increment to the next range while the upper bound of our current slot is less than our current key
            while (position[i] < slots.get(i).size() && slots.get(i).get(position[i]).compareUpperToLowerBound(ptr) < 0) {
                position[i]++;
            }
            Arrays.fill(position, i+1, position.length, 0);
            if (position[i] >= slots.get(i).size()) {
                // Our current key is bigger than the last range of the current slot.
                // If navigating after current key, backtrack and increment the key of the previous slot values.
                // If navigating to current key, just return
                if (terminate == Terminate.AT) {
                    return ReturnCode.SEEK_NEXT_USING_HINT;
                }
                if (i == 0) {
                    isDone = true;
                    return ReturnCode.NEXT_ROW;
                }
                // Increment key and backtrack until in range. We know at this point that we'll be
                // issuing a seek next hint.
                seek = true;
                Arrays.fill(position, i, position.length, 0);
                int j  = i - 1;
                // If we're positioned at a single key, no need to copy the current key and get the next key .
                // Instead, just increment to the next key and continue.
                boolean incremented = false;
                while (j >= 0 && slots.get(j).get(position[j]).isSingleKey() && (incremented=true) && (position[j] = (position[j] + 1) % slots.get(j).size()) == 0) {
                    j--;
                    incremented = false;
                }
                if (j < 0) {
                    isDone = true;
                    return ReturnCode.NEXT_ROW;
                }
                if (incremented) {
                    // Continue the loop after setting the start key, because our start key maybe smaller than
                    // the current key, so we'll end up incrementing the start key until it's bigger than the
                    // current key.
                    setStartKey();
                    schema.reposition(ptr, ScanUtil.getRowKeyPosition(slotSpan, i), ScanUtil.getRowKeyPosition(slotSpan, j), minOffset, maxOffset, slotSpan[j]);
                } else {
                    int currentLength = setStartKey(ptr, minOffset, j+1);
                    // From here on, we use startKey as our buffer (resetting minOffset and maxOffset)
                    // We've copied the part of the current key above that we need into startKey
                    // Reinitialize the iterator to be positioned at previous slot position
                    minOffset = 0;
                    maxOffset = startKeyLength;
                    schema.iterator(startKey, minOffset, maxOffset, ptr, ScanUtil.getRowKeyPosition(slotSpan, j)+1);
                    // Do nextKey after setting the accessor b/c otherwise the null byte may have
                    // been incremented causing us not to find it
                    ByteUtil.nextKey(startKey, currentLength);
                }
                i = j;
            } else if (slots.get(i).get(position[i]).compareLowerToUpperBound(ptr) > 0) {
                // Our current key is less than the lower range of the current position in the current slot.
                // Seek to the lower range, since it's bigger than the current key
                setStartKey(ptr, minOffset, i);
                return ReturnCode.SEEK_NEXT_USING_HINT;
            } else { // We're in range, check the next slot
                if (!slots.get(i).get(position[i]).isSingleKey() && i < earliestRangeIndex) {
                    earliestRangeIndex = i;
                }
                // If we're past the last slot or we know we're seeking to the next (in
                // which case the previously updated slot was verified to be within the
                // range, so we don't need to check the rest of the slots. If we were
                // to check the rest of the slots, we'd get into trouble because we may
                // have a null byte that was incremented which screws up our schema.next call)
                if (i == nSlots-1 || seek) {
                    break;
                }
                i++;
                // If we run out of slots in our key, it means we have a partial key.
                if (schema.next(ptr, ScanUtil.getRowKeyPosition(slotSpan, i), maxOffset, slotSpan[i]) == null) {
                    // If the rest of the slots are checking for IS NULL, then break because
                    // that's the case (since we don't store trailing nulls).
                    if (allTrailingNulls(i)) {
                        break;
                    }
                    // Otherwise we seek to the next start key because we're before it now
                    setStartKey(ptr, minOffset, i);
                    return ReturnCode.SEEK_NEXT_USING_HINT;
                }
            }
        }
            
        if (seek) {
            return ReturnCode.SEEK_NEXT_USING_HINT;
        }
        // Else, we're in range for all slots and can include this row plus all rows 
        // up to the upper range of our last slot. We do this for ranges and single keys
        // since we potentially have multiple key values for the same row key.
        setEndKey(ptr, minOffset, i);
        return ReturnCode.INCLUDE;
    }

    private boolean allTrailingNulls(int i) {
        for (; i < slots.size(); i++) {
            List<KeyRange> keyRanges = slots.get(i);
            if (keyRanges.size() != 1) {
                return false;
            }
            KeyRange keyRange = keyRanges.get(0);
            if (!keyRange.isSingleKey()) {
                return false;
            }
            if (keyRange.getLowerRange().length != 0) {
                return false;
            }
        }
        return true;
    }
    
    private int nextPosition(int i) {
        while (i >= 0 && slots.get(i).get(position[i]).isSingleKey() && (position[i] = (position[i] + 1) % slots.get(i).size()) == 0) {
            i--;
        }
        return i;
    }

    private void setStartKey() {
        startKeyLength = setKey(Bound.LOWER, startKey, 0, 0);
    }

    private int setStartKey(ImmutableBytesWritable ptr, int offset, int i) {
        int length = ptr.getOffset() - offset;
        startKey = copyKey(startKey, length + this.maxKeyLength, ptr.get(), offset, length);
        startKeyLength = length;
        // Add separator byte if we're at the end of the buffer, since trailing separator bytes are stripped
        if (ptr.getOffset() + ptr.getLength() == offset + length && i-1 > 0 && !schema.getField(i-1).getDataType().isFixedWidth()) {
            startKey[startKeyLength++] = QueryConstants.SEPARATOR_BYTE;
        }
        startKeyLength += setKey(Bound.LOWER, startKey, startKeyLength, i);
        return length;
    }
    
    private int setEndKey(ImmutableBytesWritable ptr, int offset, int i) {
        int length = ptr.getOffset() - offset;
        endKey = copyKey(endKey, length + this.maxKeyLength, ptr.get(), offset, length);
        endKeyLength = length;
        endKeyLength += setKey(Bound.UPPER, endKey, length, i);
        return length;
    }
    
    private int setKey(Bound bound, byte[] key, int keyOffset, int slotStartIndex) {
        return ScanUtil.setKey(schema, slots, slotSpan, position, bound, key, keyOffset, slotStartIndex, position.length);
    }

    private static byte[] copyKey(byte[] targetKey, int targetLength, byte[] sourceKey, int offset, int length) {
        if (targetLength > targetKey.length) {
            targetKey = new byte[targetLength];
        }
        System.arraycopy(sourceKey, offset, targetKey, 0, length);
        return targetKey;
    }

    private static final int KEY_RANGE_LENGTH_BITS = 21;
    private static final int SLOT_SPAN_BITS = 32 - KEY_RANGE_LENGTH_BITS;
    
    @Override
    public void readFields(DataInput in) throws IOException {
        RowKeySchema schema = new RowKeySchema();
        schema.readFields(in);
        int andLen = in.readInt();
        int[] slotSpan = new int[andLen];
        List<List<KeyRange>> slots = Lists.newArrayListWithExpectedSize(andLen);
        for (int i=0; i<andLen; i++) {
            int orLenWithSlotSpan = in.readInt();
            int orLen = orLenWithSlotSpan;
            /*
             * For 4.2+ clients, we serialize the slotSpan array. To maintain backward
             * compatibility, we encode the slotSpan values with the size of the list
             * of key ranges. We reserve 21 bits for the key range list and 10 bits
             * for the slotSpan value (up to 1024 which should be plenty).
             */
            if (orLenWithSlotSpan < 0) {
                orLenWithSlotSpan = -orLenWithSlotSpan - 1;
                slotSpan[i] = orLenWithSlotSpan >>> KEY_RANGE_LENGTH_BITS;
                orLen = (orLenWithSlotSpan << SLOT_SPAN_BITS) >>> SLOT_SPAN_BITS;            
            }
            List<KeyRange> orClause = Lists.newArrayListWithExpectedSize(orLen);
            slots.add(orClause);
            for (int j=0; j<orLen; j++) {
                KeyRange range = new KeyRange();
                range.readFields(in);
                orClause.add(range);
            }
        }
        this.init(slots, slotSpan, schema);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        assert(slots.size() == slotSpan.length);
        schema.write(out);
        out.writeInt(slots.size());
        for (int i = 0; i < slots.size(); i++) {
            List<KeyRange> orLen = slots.get(i);
            int span = slotSpan[i];
            int orLenWithSlotSpan = -( ( (span << KEY_RANGE_LENGTH_BITS) | orLen.size() ) + 1);
            out.writeInt(orLenWithSlotSpan);
            for (KeyRange range : orLen) {
                range.write(out);
            }
        }
    }
    
    @Override
    public byte[] toByteArray() throws IOException {
        return Writables.getBytes(this);
    }
    
    public static SkipScanFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (SkipScanFilter)Writables.getWritable(pbBytes, new SkipScanFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public int hashCode() {
        HashFunction hf = Hashing.goodFastHash(32);
        Hasher h = hf.newHasher();
        h.putInt(slots.size());
        for (int i=0; i<slots.size(); i++) {
            h.putInt(slots.get(i).size());
            for (int j=0; j<slots.size(); j++) {
                h.putBytes(slots.get(i).get(j).getLowerRange());
                h.putBytes(slots.get(i).get(j).getUpperRange());
            }
        }
        return h.hash().asInt();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SkipScanFilter)) return false;
        SkipScanFilter other = (SkipScanFilter)obj;
        return Objects.equal(slots, other.slots) && Objects.equal(schema, other.schema);
    }

    @Override
    public String toString() {
        return "SkipScanFilter "+ slots.toString() ;
    }
}
