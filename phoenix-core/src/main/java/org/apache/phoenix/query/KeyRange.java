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
package org.apache.phoenix.query;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ScanUtil.BytesComparator;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 *
 * Class that represents an upper/lower bound key range.
 *
 * 
 * @since 0.1
 */
public class KeyRange implements Writable {
    public enum Bound { LOWER, UPPER };
    private static final byte[] DEGENERATE_KEY = new byte[] {1};
    public static final byte[] UNBOUND = new byte[0];
    /**
     * KeyRange for variable length null values. Since we need to represent this using an empty byte array (which
     * is what we use for upper/lower bound), we create this range using the private constructor rather than
     * going through the static creation method (where this would not be possible).
     */
    public static final KeyRange IS_NULL_RANGE = new KeyRange(ByteUtil.EMPTY_BYTE_ARRAY, true, ByteUtil.EMPTY_BYTE_ARRAY, true);
    /**
     * KeyRange for non null variable length values. Since we need to represent this using an empty byte array (which
     * is what we use for upper/lower bound), we create this range using the private constructor rather than going
     * through the static creation method (where this would not be possible).
     */
    public static final KeyRange IS_NOT_NULL_RANGE = new KeyRange(ByteUtil.nextKey(QueryConstants.SEPARATOR_BYTE_ARRAY), true, UNBOUND, false);
    
    /**
     * KeyRange for an empty key range
     */
    public static final KeyRange EMPTY_RANGE = new KeyRange(DEGENERATE_KEY, false, DEGENERATE_KEY, false);
    
    /**
     * KeyRange that contains all values
     */
    public static final KeyRange EVERYTHING_RANGE = new KeyRange(UNBOUND, false, UNBOUND, false);
    
    public static final Function<byte[], KeyRange> POINT = new Function<byte[], KeyRange>() {
        @Override 
        public KeyRange apply(byte[] input) {
            return new KeyRange(input, true, input, true);
        }
    };
    public static final Comparator<KeyRange> COMPARATOR = new Comparator<KeyRange>() {
        @Override public int compare(KeyRange o1, KeyRange o2) {
            int result = Boolean.compare(o2.lowerUnbound(), o1.lowerUnbound());
            if (result != 0) {
                return result;
            }
            result = Bytes.BYTES_COMPARATOR.compare(o1.getLowerRange(), o2.getLowerRange());
            if (result != 0) {
                return result;
            }
            result = Boolean.compare(o2.isLowerInclusive(), o1.isLowerInclusive());
            if (result != 0) {
                return result;
            }
            result = Boolean.compare(o1.upperUnbound(), o2.upperUnbound());
            if (result != 0) {
                return result;
            }
            result = Bytes.BYTES_COMPARATOR.compare(o1.getUpperRange(), o2.getUpperRange());
            if (result != 0) {
                return result;
            }
            return Boolean.compare(o2.isUpperInclusive(), o1.isUpperInclusive());
        }
    };

    private byte[] lowerRange;
    private boolean lowerInclusive;
    private byte[] upperRange;
    private boolean upperInclusive;
    private boolean isSingleKey;

    public static KeyRange getKeyRange(byte[] point) {
        return getKeyRange(point, true, point, true);
    }
    
    public static KeyRange getKeyRange(byte[] lowerRange, byte[] upperRange) {
        return getKeyRange(lowerRange, true, upperRange, false);
    }

    private static KeyRange getSingleton(byte[] lowerRange, boolean lowerInclusive,
            byte[] upperRange, boolean upperInclusive) {
        if (lowerRange == null || upperRange == null) {
            return EMPTY_RANGE;
        }
        if (lowerRange.length == 0 && upperRange.length == 0) {
            // Need singleton to represent NULL range so it gets treated differently
            // than an unbound RANGE.
            return lowerInclusive && upperInclusive ? IS_NULL_RANGE : EVERYTHING_RANGE;
        }
        if (lowerRange.length != 0 && upperRange.length != 0) {
            int cmp = Bytes.compareTo(lowerRange, upperRange);
            if (cmp > 0 || (cmp == 0 && !(lowerInclusive && upperInclusive))) {
                return EMPTY_RANGE;
            }
        }
        return null;
    }
    
    public static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive,
            byte[] upperRange, boolean upperInclusive) {
        KeyRange range = getSingleton(lowerRange, lowerInclusive, upperRange, upperInclusive);
        if (range != null) {
            return range;
        }
        boolean unboundLower = false;
        boolean unboundUpper = false;
        if (lowerRange.length == 0) {
            lowerRange = UNBOUND;
            lowerInclusive = false;
            unboundLower = true;
        }
        if (upperRange.length == 0) {
            upperRange = UNBOUND;
            upperInclusive = false;
            unboundUpper = true;
        }

        return new KeyRange(lowerRange, unboundLower ? false : lowerInclusive,
                upperRange, unboundUpper ? false : upperInclusive);
    }

    public static KeyRange read(DataInput input) throws IOException {
        KeyRange range = new KeyRange();
        range.readFields(input);
        // Translate to singleton after reading
        KeyRange singletonRange = getSingleton(range.lowerRange, range.lowerInclusive, range.upperRange, range.upperInclusive);
        if (singletonRange != null) {
            return singletonRange;
        }
        // Otherwise, just keep the range we read
        return range;
    }
    
    private KeyRange() {
        this.lowerRange = DEGENERATE_KEY;
        this.lowerInclusive = false;
        this.upperRange = DEGENERATE_KEY;
        this.upperInclusive = false;
        this.isSingleKey = false;
    }
    
    private KeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        this.lowerRange = lowerRange;
        this.lowerInclusive = lowerInclusive;
        this.upperRange = upperRange;
        this.upperInclusive = upperInclusive;
        init();
    }
    
    private void init() {
        this.isSingleKey = lowerRange != UNBOUND && upperRange != UNBOUND
                && lowerInclusive && upperInclusive && Bytes.compareTo(lowerRange, upperRange) == 0;
    }

    public byte[] getRange(Bound bound) {
        return bound == Bound.LOWER ? getLowerRange() : getUpperRange();
    }
    
    public boolean isInclusive(Bound bound) {
        return bound == Bound.LOWER ? isLowerInclusive() : isUpperInclusive();
    }
    
    public boolean isUnbound(Bound bound) {
        return bound == Bound.LOWER ? lowerUnbound() : upperUnbound();
    }
    
    public boolean isSingleKey() {
        return isSingleKey;
    }
    
    public int compareLowerToUpperBound(ImmutableBytesWritable ptr, boolean isInclusive, BytesComparator comparator) {
        return compareLowerToUpperBound(ptr.get(), ptr.getOffset(), ptr.getLength(), isInclusive, comparator);
    }
    
    public int compareLowerToUpperBound(ImmutableBytesWritable ptr, BytesComparator comparator) {
        return compareLowerToUpperBound(ptr, true, comparator);
    }
    
    public int compareUpperToLowerBound(ImmutableBytesWritable ptr, boolean isInclusive, BytesComparator comparator) {
        return compareUpperToLowerBound(ptr.get(), ptr.getOffset(), ptr.getLength(), isInclusive, comparator);
    }
    
    public int compareUpperToLowerBound(ImmutableBytesWritable ptr, BytesComparator comparator) {
        return compareUpperToLowerBound(ptr, true, comparator);
    }
    
    public int compareLowerToUpperBound( byte[] b, int o, int l, BytesComparator comparator) {
        return compareLowerToUpperBound(b,o,l,true, comparator);
    }

    public int compareLowerToUpperBound( byte[] b, BytesComparator comparator) {
        return compareLowerToUpperBound(b,0,b.length, comparator);
    }

    /**
     * Compares a lower bound against an upper bound
     * @param b upper bound byte array
     * @param o upper bound offset
     * @param l upper bound length
     * @param isInclusive upper bound inclusive
     * @param comparator comparator used to do compare the byte array using offset and length
     * @return -1 if the lower bound is less than the upper bound,
     *          1 if the lower bound is greater than the upper bound,
     *          and 0 if they are equal.
     */
    public int compareLowerToUpperBound( byte[] b, int o, int l, boolean isInclusive, BytesComparator comparator) {
        if (lowerUnbound() || b == KeyRange.UNBOUND) {
            return -1;
        }
        int cmp = comparator.compare(lowerRange, 0, lowerRange.length, b, o, l);
        if (cmp > 0) {
            return 1;
        }
        if (cmp < 0) {
            return -1;
        }
        if (lowerInclusive && isInclusive) {
            return 0;
        }
        return 1;
    }
    
    public int compareUpperToLowerBound(byte[] b, BytesComparator comparator) {
        return compareUpperToLowerBound(b,0,b.length, comparator);
    }
    
    public int compareUpperToLowerBound(byte[] b, int o, int l, BytesComparator comparator) {
        return compareUpperToLowerBound(b,o,l, true, comparator);
    }
    
    public int compareUpperToLowerBound(byte[] b, int o, int l, boolean isInclusive, BytesComparator comparator) {
        if (upperUnbound() || b == KeyRange.UNBOUND) {
            return 1;
        }
        int cmp = comparator.compare(upperRange, 0, upperRange.length, b, o, l);
        if (cmp > 0) {
            return 1;
        }
        if (cmp < 0) {
            return -1;
        }
        if (upperInclusive && isInclusive) {
            return 0;
        }
        return -1;
    }
    
    public byte[] getLowerRange() {
        return lowerRange;
    }

    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    public byte[] getUpperRange() {
        return upperRange;
    }

    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    public boolean isUnbound() {
        return lowerUnbound() || upperUnbound();
    }

    public boolean upperUnbound() {
        return upperRange == UNBOUND;
    }

    public boolean lowerUnbound() {
        return lowerRange == UNBOUND;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(lowerRange);
        if (lowerRange != null)
            result = prime * result + (lowerInclusive ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(upperRange);
        if (upperRange != null)
            result = prime * result + (upperInclusive ? 1231 : 1237);
        return result;
    }

    @Override
    public String toString() {
        if (isSingleKey()) {
            return Bytes.toStringBinary(lowerRange);
        }
        return (lowerInclusive ? "[" : 
            "(") + (lowerUnbound() ? "*" : 
                Bytes.toStringBinary(lowerRange)) + " - " + (upperUnbound() ? "*" : 
                    Bytes.toStringBinary(upperRange)) + (upperInclusive ? "]" : ")" );
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyRange)) {
            return false;
        }
        KeyRange that = (KeyRange)o;
        return Bytes.compareTo(this.lowerRange,that.lowerRange) == 0 && this.lowerInclusive == that.lowerInclusive &&
               Bytes.compareTo(this.upperRange, that.upperRange) == 0 && this.upperInclusive == that.upperInclusive;
    }

    public KeyRange intersect(KeyRange range) {
        byte[] newLowerRange;
        byte[] newUpperRange;
        boolean newLowerInclusive;
        boolean newUpperInclusive;
        // Special case for null, is it is never included another range
        // except for null itself.
        if (this == IS_NULL_RANGE && range == IS_NULL_RANGE) {
                return IS_NULL_RANGE;
        } else if(this == IS_NULL_RANGE || range == IS_NULL_RANGE) {
            return EMPTY_RANGE;
        }
        if (lowerUnbound()) {
            newLowerRange = range.lowerRange;
            newLowerInclusive = range.lowerInclusive;
        } else if (range.lowerUnbound()) {
            newLowerRange = lowerRange;
            newLowerInclusive = lowerInclusive;
        } else {
            int cmp = Bytes.compareTo(lowerRange, range.lowerRange);
            if (cmp != 0 || lowerInclusive == range.lowerInclusive) {
                if (cmp <= 0) {
                    newLowerRange = range.lowerRange;
                    newLowerInclusive = range.lowerInclusive;
                } else {
                    newLowerRange = lowerRange;
                    newLowerInclusive = lowerInclusive;
                }
            } else { // Same lower range, but one is not inclusive
                newLowerRange = range.lowerRange;
                newLowerInclusive = false;
            }
        }
        if (upperUnbound()) {
            newUpperRange = range.upperRange;
            newUpperInclusive = range.upperInclusive;
        } else if (range.upperUnbound()) {
            newUpperRange = upperRange;
            newUpperInclusive = upperInclusive;
        } else {
            int cmp = Bytes.compareTo(upperRange, range.upperRange);
            if (cmp != 0 || upperInclusive == range.upperInclusive) {
                if (cmp >= 0) {
                    newUpperRange = range.upperRange;
                    newUpperInclusive = range.upperInclusive;
                } else {
                    newUpperRange = upperRange;
                    newUpperInclusive = upperInclusive;
                }
            } else { // Same upper range, but one is not inclusive
                newUpperRange = range.upperRange;
                newUpperInclusive = false;
            }
        }
        if (newLowerRange == lowerRange && newLowerInclusive == lowerInclusive
                && newUpperRange == upperRange && newUpperInclusive == upperInclusive) {
            return this;
        }
        return getKeyRange(newLowerRange, newLowerInclusive, newUpperRange, newUpperInclusive);
    }

    public static boolean isDegenerate(byte[] lowerRange, byte[] upperRange) {
        return lowerRange == KeyRange.EMPTY_RANGE.getLowerRange() && upperRange == KeyRange.EMPTY_RANGE.getUpperRange();
    }

    /**
     * @return list of at least size 1
     */
    @NonNull
    public static List<KeyRange> coalesce(List<KeyRange> keyRanges) {
        List<KeyRange> tmp = new ArrayList<KeyRange>();
        for (KeyRange keyRange : keyRanges) {
            if (EMPTY_RANGE == keyRange) {
                continue;
            }
            if (EVERYTHING_RANGE == keyRange) {
                tmp.clear();
                tmp.add(keyRange);
                break;
            }
            tmp.add(keyRange);
        }
        if (tmp.size() == 1) {
            return tmp;
        }
        if (tmp.size() == 0) {
            return Collections.singletonList(EMPTY_RANGE);
        }

        Collections.sort(tmp, COMPARATOR);
        List<KeyRange> tmp2 = new ArrayList<KeyRange>();
        KeyRange range = tmp.get(0);
        for (int i=1; i<tmp.size(); i++) {
            KeyRange otherRange = tmp.get(i);
            KeyRange intersect = range.intersect(otherRange);
            if (EMPTY_RANGE == intersect) {
                tmp2.add(range);
                range = otherRange;
            } else {
                range = range.union(otherRange);
            }
        }
        tmp2.add(range);
        List<KeyRange> tmp3 = new ArrayList<KeyRange>();
        range = tmp2.get(0);
        for (int i=1; i<tmp2.size(); i++) {
            KeyRange otherRange = tmp2.get(i);
            assert !range.upperUnbound();
            assert !otherRange.lowerUnbound();
            if (range.isUpperInclusive() != otherRange.isLowerInclusive()
                    && Bytes.equals(range.getUpperRange(), otherRange.getLowerRange())) {
                range = KeyRange.getKeyRange(range.getLowerRange(), range.isLowerInclusive(), otherRange.getUpperRange(), otherRange.isUpperInclusive());
            } else {
                tmp3.add(range);
                range = otherRange;
            }
        }
        tmp3.add(range);
        
        return tmp3;
    }

    public KeyRange union(KeyRange other) {
        if (EMPTY_RANGE == other) return this;
        if (EMPTY_RANGE == this) return other;
        byte[] newLower, newUpper;
        boolean newLowerInclusive, newUpperInclusive;
        if (this.lowerUnbound() || other.lowerUnbound()) {
            newLower = UNBOUND;
            newLowerInclusive = false;
        } else {
            int lowerCmp = Bytes.compareTo(this.lowerRange, other.lowerRange);
            if (lowerCmp < 0) {
                newLower = lowerRange;
                newLowerInclusive = lowerInclusive;
            } else if (lowerCmp == 0) {
                newLower = lowerRange;
                newLowerInclusive = this.lowerInclusive || other.lowerInclusive;
            } else {
                newLower = other.lowerRange;
                newLowerInclusive = other.lowerInclusive;
            }
        }

        if (this.upperUnbound() || other.upperUnbound()) {
            newUpper = UNBOUND;
            newUpperInclusive = false;
        } else {
            int upperCmp = Bytes.compareTo(this.upperRange, other.upperRange);
            if (upperCmp > 0) {
                newUpper = upperRange;
                newUpperInclusive = this.upperInclusive;
            } else if (upperCmp == 0) {
                newUpper = upperRange;
                newUpperInclusive = this.upperInclusive || other.upperInclusive;
            } else {
                newUpper = other.upperRange;
                newUpperInclusive = other.upperInclusive;
            }
        }
        return KeyRange.getKeyRange(newLower, newLowerInclusive, newUpper, newUpperInclusive);
    }

    public static List<KeyRange> of(List<byte[]> keys) {
        return Lists.transform(keys, POINT);
    }

    private static int compareUpperRange(KeyRange rowKeyRange1,KeyRange rowKeyRange2) {
        int result = Boolean.compare(rowKeyRange1.upperUnbound(), rowKeyRange2.upperUnbound());
        if (result != 0) {
            return result;
        }
        result = Bytes.BYTES_COMPARATOR.compare(rowKeyRange1.getUpperRange(), rowKeyRange2.getUpperRange());
        if (result != 0) {
            return result;
        }
        return Boolean.compare(rowKeyRange2.isUpperInclusive(), rowKeyRange1.isUpperInclusive());
    }

    public static List<KeyRange> intersect(List<KeyRange> rowKeyRanges1, List<KeyRange> rowKeyRanges2) {
        List<KeyRange> newRowKeyRanges1=coalesce(rowKeyRanges1);
        List<KeyRange> newRowKeyRanges2=coalesce(rowKeyRanges2);
        Iterator<KeyRange> iter1=newRowKeyRanges1.iterator();
        Iterator<KeyRange> iter2=newRowKeyRanges2.iterator();

        List<KeyRange> result = new LinkedList<KeyRange>();
        KeyRange rowKeyRange1=null;
        KeyRange rowKeyRange2=null;
        while(true) {
            if(rowKeyRange1==null) {
                if(!iter1.hasNext()) {
                    break;
                }
                rowKeyRange1=iter1.next();
            }
            if(rowKeyRange2==null) {
                if(!iter2.hasNext()) {
                    break;
                }
                rowKeyRange2=iter2.next();
            }
            KeyRange intersectedRowKeyRange=rowKeyRange1.intersect(rowKeyRange2);
            if(intersectedRowKeyRange!=EMPTY_RANGE) {
                result.add(intersectedRowKeyRange);
            }
            int cmp=compareUpperRange(rowKeyRange1, rowKeyRange2);
            if(cmp < 0) {
                //move iter1
                rowKeyRange1=null;
            } else if(cmp > 0) {
                //move iter2
                rowKeyRange2=null;
            } else {
                //move iter1 and iter2
                rowKeyRange1=rowKeyRange2=null;
            }
        }
        if (result.size() == 0) {
            return Collections.singletonList(KeyRange.EMPTY_RANGE);
        }
        return result;
    }
    
    public KeyRange invert() {
        byte[] lower = this.getLowerRange();
        if (!this.lowerUnbound()) {
            lower = SortOrder.invert(lower, 0, lower.length);
        }
        byte[] upper;
        if (this.isSingleKey()) {
            upper = lower;
        } else {
            upper = this.getUpperRange();
            if (!this.upperUnbound()) {
                upper = SortOrder.invert(upper, 0, upper.length);
            }
        }
        return KeyRange.getKeyRange(lower, this.isLowerInclusive(), upper, this.isUpperInclusive());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = WritableUtils.readVInt(in);
        if (len == 0) {
            lowerRange = KeyRange.UNBOUND;
            lowerInclusive = false;
        } else {
            if (len < 0) {
                lowerInclusive = false;
                lowerRange = new byte[-len - 1];
                in.readFully(lowerRange);
            } else {
                lowerInclusive = true;
                lowerRange = new byte[len - 1];
                in.readFully(lowerRange);
            }
        }
        len = WritableUtils.readVInt(in);
        if (len == 0) {
            upperRange = KeyRange.UNBOUND;
            upperInclusive = false;
        } else {
            if (len < 0) {
                upperInclusive = false;
                upperRange = new byte[-len - 1];
                in.readFully(upperRange);
            } else {
                upperInclusive = true;
                upperRange = new byte[len - 1];
                in.readFully(upperRange);
            }
        }
        init();
    }

    private void writeBound(Bound bound, DataOutput out) throws IOException {
        // Encode unbound by writing a zero
        if (isUnbound(bound)) {
            WritableUtils.writeVInt(out, 0);
            return;
        }
        // Otherwise, inclusive is positive and exclusive is negative, offset by 1
        byte[] range = getRange(bound);
        if (isInclusive(bound)){
            WritableUtils.writeVInt(out, range.length+1);
        } else {
            WritableUtils.writeVInt(out, -(range.length+1));
        }
        out.write(range);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        writeBound(Bound.LOWER, out);
        writeBound(Bound.UPPER, out);
    }

    public KeyRange prependRange(byte[] bytes, int offset, int length) {
        if (length == 0 || this == EVERYTHING_RANGE) {
            return this;
        }
        byte[] lowerRange = this.getLowerRange();
        if (!this.lowerUnbound()) {
            byte[] newLowerRange = new byte[length + lowerRange.length];
            System.arraycopy(bytes, offset, newLowerRange, 0, length);
            System.arraycopy(lowerRange, 0, newLowerRange, length, lowerRange.length);
            lowerRange = newLowerRange;
        }
        byte[] upperRange = this.getUpperRange();
        if (!this.upperUnbound()) {
            byte[] newUpperRange = new byte[length + upperRange.length];
            System.arraycopy(bytes, offset, newUpperRange, 0, length);
            System.arraycopy(upperRange, 0, newUpperRange, length, upperRange.length);
            upperRange = newUpperRange;
        }
        return getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
    }
}