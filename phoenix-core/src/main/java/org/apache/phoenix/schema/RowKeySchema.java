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
package org.apache.phoenix.schema;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;


/**
 * 
 * Schema for the bytes in a RowKey. For the RowKey, we use a null byte
 * to terminate a variable length type, while for KeyValue bytes we
 * write the length of the var char preceding the value. We can't do
 * that for a RowKey because it would affect the sort order.
 *
 * 
 * @since 0.1
 */
public class RowKeySchema extends ValueSchema {
    public static final RowKeySchema EMPTY_SCHEMA = new RowKeySchema(0,Collections.<Field>emptyList(), true)
    ;
    
    public RowKeySchema() {
    }
    
    protected RowKeySchema(int minNullable, List<Field> fields, boolean rowKeyOrderOptimizable) {
        super(minNullable, fields, rowKeyOrderOptimizable);
    }

    public static class RowKeySchemaBuilder extends ValueSchemaBuilder {
        private boolean rowKeyOrderOptimizable = false;
        
        public RowKeySchemaBuilder(int maxFields) {
            super(maxFields);
            setMaxFields(maxFields);
        }
        
        @Override
        public RowKeySchemaBuilder addField(PDatum datum, boolean isNullable, SortOrder sortOrder) {
            super.addField(datum, isNullable, sortOrder);
            return this;
        }

        public RowKeySchemaBuilder rowKeyOrderOptimizable(boolean rowKeyOrderOptimizable) {
            this.rowKeyOrderOptimizable = rowKeyOrderOptimizable;
            return this;
        }

        @Override
        public RowKeySchema build() {
            List<Field> condensedFields = buildFields();
            return new RowKeySchema(this.minNullable, condensedFields, rowKeyOrderOptimizable);
        }
    }

    public boolean rowKeyOrderOptimizable() {
        return rowKeyOrderOptimizable;
    }

    public int getMaxFields() {
        return this.getMinNullable();
    }

    // "iterator" initialization methods that initialize a bytes ptr with a row key for further navigation
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean iterator(byte[] src, int srcOffset, int srcLength, ImmutableBytesWritable ptr, int position,int extraColumnSpan) {
        Boolean hasValue = null;
        ptr.set(src, srcOffset, 0);
        int maxOffset = srcOffset + srcLength;
        for (int i = 0; i < position; i++) {
            hasValue = next(ptr, i, maxOffset);
        }
        if(extraColumnSpan > 0) {
            readExtraFields(ptr, position, maxOffset, extraColumnSpan);
        }
        return hasValue;
    }

    public Boolean iterator(byte[] src, int srcOffset, int srcLength, ImmutableBytesWritable ptr, int position) {
        return iterator(src, srcOffset,srcLength, ptr, position,0);
    }
    
    public Boolean iterator(ImmutableBytesWritable srcPtr, ImmutableBytesWritable ptr, int position) {
        return iterator(srcPtr.get(), srcPtr.getOffset(), srcPtr.getLength(), ptr, position);
    }
    
    public Boolean iterator(byte[] src, ImmutableBytesWritable ptr, int position) {
        return iterator(src, 0, src.length, ptr, position);
    }
    
    public int iterator(byte[] src, int srcOffset, int srcLength, ImmutableBytesWritable ptr) {
        int maxOffset = srcOffset + srcLength;
        iterator(src, srcOffset, srcLength, ptr, 0);
        return maxOffset;
    }
    
    public int iterator(byte[] src, ImmutableBytesWritable ptr) {
        return iterator(src, 0, src.length, ptr);
    }
    
    public int iterator(ImmutableBytesWritable ptr) {
        return iterator(ptr.get(),ptr.getOffset(),ptr.getLength(), ptr);
    }

    // navigation methods that "select" different chunks of the row key held in a bytes ptr

    /**
     * Move the bytes ptr to the next position in the row key relative to its current position. You
     * must have a complete row key. Use @link {@link #position(ImmutableBytesWritable, int, int)}
     * if you have a partial row key.
     * @param ptr bytes pointer pointing to the value at the positional index provided.
     * @param position zero-based index of the next field in the value schema
     * @param maxOffset max possible offset value when iterating
     * @return true if a value was found and ptr was set, false if the value is null and ptr was not
     * set, and null if the value is null and there are no more values
      */
    public Boolean next(ImmutableBytesWritable ptr, int position, int maxOffset) {
        return next(ptr, position, maxOffset, false);
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    private Boolean next(ImmutableBytesWritable ptr, int position, int maxOffset, boolean isFirst) {
        if (ptr.getOffset() + ptr.getLength() >= maxOffset) {
            ptr.set(ptr.get(), maxOffset, 0);
            return null;
        }
        if (position >= getFieldCount()) {
            return null;
        }
        // Move the pointer past the current value and set length
        // to 0 to ensure you never set the ptr past the end of the
        // backing byte array.
        ptr.set(ptr.get(), ptr.getOffset() + ptr.getLength(), 0);
        // If positioned at SEPARATOR_BYTE, skip it.
        // Don't look back at previous fields if this is our first next call, as
        // we may have a partial key for RVCs that doesn't include the leading field.
        if (position > 0 && !isFirst && !getField(position-1).getDataType().isFixedWidth()) {
            ptr.set(ptr.get(), ptr.getOffset()+ptr.getLength()+1, 0);
        }
        Field field = this.getField(position);
        if (field.getDataType().isFixedWidth()) {
            // It is possible that the number of remaining row key bytes are less than the fixed
            // width size. See PHOENIX-3968.
            ptr.set(ptr.get(), ptr.getOffset(), Math.min(maxOffset - ptr.getOffset(), field.getByteSize()));
        } else {
            if (position+1 == getFieldCount() ) {
                // Last field has no terminator unless it's descending sort order
                int len = maxOffset - ptr.getOffset();
                ptr.set(ptr.get(), ptr.getOffset(), maxOffset - ptr.getOffset() - (SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable, len == 0, field) == QueryConstants.DESC_SEPARATOR_BYTE ? 1 : 0));
            } else {
                byte[] buf = ptr.get();
                int offset = ptr.getOffset();
                // First byte 
                if (offset < maxOffset && buf[offset] != QueryConstants.SEPARATOR_BYTE) {
                    byte sepByte = SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable, false, field);
                    do {
                        offset++;
                    } while (offset < maxOffset && buf[offset] != sepByte);
                }
                ptr.set(buf, ptr.getOffset(), offset - ptr.getOffset());
            }
        }
        return ptr.getLength() > 0;
    }

    /**
     * Like {@link #next(org.apache.hadoop.hbase.io.ImmutableBytesWritable, int, int)}, but also
     * includes the next {@code extraSpan} additional fields in the bytes ptr.
     * This allows multiple fields to be treated as one concatenated whole.
     * @param ptr  bytes pointer pointing to the value at the positional index provided.
     * @param position zero-based index of the next field in the value schema
     * @param maxOffset max possible offset value when iterating
     * @param extraSpan the number of extra fields to expand the ptr to contain
     * @return true if a value was found and ptr was set, false if the value is null and ptr was not
     * set, and null if the value is null and there are no more values
     */
    public int next(ImmutableBytesWritable ptr, int position, int maxOffset, int extraSpan) {
        if (next(ptr, position, maxOffset) == null) {
            return position-1;
        }
        return readExtraFields(ptr, position + 1, maxOffset, extraSpan);
    }
    
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean previous(ImmutableBytesWritable ptr, int position, int minOffset) {
        if (position < 0) {
            return null;
        }
        Field field = this.getField(position);
        if (field.getDataType().isFixedWidth()) {
            ptr.set(ptr.get(), ptr.getOffset()-field.getByteSize(), field.getByteSize());
            return true;
        }
        // If ptr has length of zero, it is assumed that we're at the end of the row key
        int offsetAdjustment = position + 1 == this.getFieldCount() || ptr.getLength() == 0 ? 0 : 1;
        if (position == 0) {
            ptr.set(ptr.get(), minOffset, ptr.getOffset() - minOffset - offsetAdjustment);
            return true;
        }
        field = this.getField(position-1);
        // Field before the one we want to position at is variable length
        // In this case, we can search backwards for our separator byte
        // to determine the length
        if (!field.getDataType().isFixedWidth()) {
            byte[] buf = ptr.get();
            int offset = ptr.getOffset()-1-offsetAdjustment;
            // Separator always zero byte if zero length
            if (offset > minOffset && buf[offset] != QueryConstants.SEPARATOR_BYTE) {
                byte sepByte = SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable, false, field);
                do {
                    offset--;
                } while (offset > minOffset && buf[offset] != sepByte);
            }
            if (offset == minOffset) { // shouldn't happen
                ptr.set(buf, minOffset, ptr.getOffset()-minOffset-1);
            } else {
                ptr.set(buf,offset+1,ptr.getOffset()-1-offsetAdjustment-offset); // Don't include null terminator in length
            }
            return true;
        }
        int i,fixedOffset = field.getByteSize();
        for (i = position-2; i >= 0 && this.getField(i).getDataType().isFixedWidth(); i--) {
            fixedOffset += this.getField(i).getByteSize();
        }
        // All of the previous fields are fixed width, so we can calculate the offset
        // based on the total fixed offset
        if (i < 0) {
            int length = ptr.getOffset() - fixedOffset - minOffset - offsetAdjustment;
            ptr.set(ptr.get(),minOffset+fixedOffset, length);
            return true;
        }
        // Otherwise we're stuck with starting from the minOffset and working all the way forward,
        // because we can't infer the length of the previous position.
        return iterator(ptr.get(), minOffset, ptr.getOffset() - minOffset - offsetAdjustment, ptr, position+1);
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL", 
            justification="Designed to return null.")
    public Boolean reposition(ImmutableBytesWritable ptr, int oldPosition, int newPosition, int minOffset, int maxOffset) {
        if (newPosition == oldPosition) {
            return ptr.getLength() > 0;
        }
        Boolean hasValue = null;
        if (newPosition > oldPosition) {
            do {
                hasValue = next(ptr, ++oldPosition, maxOffset);
            }  while (hasValue != null && oldPosition < newPosition) ;
        } else {
            int nVarLengthFromBeginning = 0;
            for (int i = 0; i <= newPosition; i++) {
                if (!this.getField(i).getDataType().isFixedWidth()) {
                    nVarLengthFromBeginning++;
                }
            }
            int nVarLengthBetween = 0;
            for (int i = oldPosition - 1; i >= newPosition; i--) {
                if (!this.getField(i).getDataType().isFixedWidth()) {
                    nVarLengthBetween++;
                }
            }
            if (nVarLengthBetween > nVarLengthFromBeginning) {
                return iterator(ptr.get(), minOffset, maxOffset-minOffset, ptr, newPosition+1);
            }
            do  {
                hasValue = previous(ptr, --oldPosition, minOffset);
            } while (hasValue != null && oldPosition > newPosition);
        }
        
        return hasValue;
    }

    /**
     * Like {@link #reposition(org.apache.hadoop.hbase.io.ImmutableBytesWritable, int, int, int, int)},
     * but also includes the next {@code extraSpan} additional fields in the bytes ptr.
     * This allows multiple fields to be treated as one concatenated whole.
     * @param extraSpan  the number of extra fields to expand the ptr to contain.
     */
    public Boolean reposition(ImmutableBytesWritable ptr, int oldPosition, int newPosition, int minOffset, int maxOffset, int extraSpan) {
        Boolean returnValue = reposition(ptr, oldPosition, newPosition, minOffset, maxOffset);
        readExtraFields(ptr, newPosition + 1, maxOffset, extraSpan);
        return returnValue;
    }
    

    /**
     * Positions ptr at the part of the row key for the field at endPosition, 
     * starting from the field at position.
     * @param ptr bytes pointer that points to row key being traversed.
     * @param position the starting field position
     * @param endPosition the ending field position
     * @return true if the row key has a value at endPosition with ptr pointing to
     * that value and false otherwise with ptr not necessarily set.
     */
    public boolean position(ImmutableBytesWritable ptr, int position, int endPosition) {
        int maxOffset = ptr.getLength();
        this.iterator(ptr); // initialize for iteration
        boolean isFirst = true;
        while (position <= endPosition) {
            if (this.next(ptr, position++, maxOffset, isFirst) == null) {
                return false;
            }
            isFirst = false;
        }
        return true;
    }


    /**
     * Extends the boundaries of the {@code ptr} to contain the next {@code extraSpan} fields in the row key.
     * @param ptr  bytes pointer pointing to the value at the positional index provided.
     * @param position  row key position of the first extra key to read
     * @param maxOffset  the maximum offset into the bytes pointer to allow
     * @param extraSpan  the number of extra fields to expand the ptr to contain.
     */
    private int readExtraFields(ImmutableBytesWritable ptr, int position, int maxOffset, int extraSpan) {
        int initialOffset = ptr.getOffset();

        int i = 0;
        Boolean hasValue = Boolean.FALSE;
        for(i = 0; i < extraSpan; i++) {
            hasValue = next(ptr, position + i, maxOffset);

            if(hasValue == null) {
                break;
            }
        }

        int finalLength = ptr.getOffset() - initialOffset + ptr.getLength();
        ptr.set(ptr.get(), initialOffset, finalLength);
        return position + i - (Boolean.FALSE.equals(hasValue) ? 1 : 0);
    }

    public int computeMaxSpan(int pkPos, KeyRange result, ImmutableBytesWritable ptr) {
        int maxOffset = iterator(result.getLowerRange(), ptr);
        int lowerSpan = 0;
        int i = pkPos;
        while (this.next(ptr, i++, maxOffset) != null) {
            lowerSpan++;
        }
        int upperSpan = 0;
        i = pkPos;
        maxOffset = iterator(result.getUpperRange(), ptr);
        while (this.next(ptr, i++, maxOffset) != null) {
            upperSpan++;
        }
        return Math.max(Math.max(lowerSpan, upperSpan), 1);
    }

    public int computeMinSpan(int pkPos, KeyRange keyRange, ImmutableBytesWritable ptr) {
        if (keyRange == KeyRange.EVERYTHING_RANGE) {
            return 0;
        }
        int lowerSpan = Integer.MAX_VALUE;
        byte[] range = keyRange.getLowerRange();
        if (range != KeyRange.UNBOUND) {
            lowerSpan = 0;
            int maxOffset = iterator(range, ptr);
            int i = pkPos;
            while (this.next(ptr, i++, maxOffset) != null) {
                lowerSpan++;
            }
        }
        int upperSpan = Integer.MAX_VALUE;
        range = keyRange.getUpperRange();
        if (range != KeyRange.UNBOUND) {
            upperSpan = 0;
            int maxOffset = iterator(range, ptr);
            int i = pkPos;
            while (this.next(ptr, i++, maxOffset) != null) {
                upperSpan++;
            }
        }
        return Math.min(lowerSpan, upperSpan);
    }

    /**
     * Clip the left hand portion of the keyRange up to the spansToClip. If keyRange is shorter in
     * spans than spansToClip, the portion of the range that exists will be returned.
     * @param pkPos the leading pk position of the keyRange.
     * @param keyRange the key range to clip
     * @param spansToClip the number of spans to clip
     * @param ptr an ImmutableBytesWritable to use for temporary storage.
     * @return the clipped portion of the keyRange
     */
    public KeyRange clipLeft(int pkPos, KeyRange keyRange, int spansToClip, ImmutableBytesWritable ptr) {
        if (spansToClip < 0) {
            throw new IllegalArgumentException("Cannot specify a negative spansToClip (" + spansToClip + ")");
        }
        if (spansToClip == 0) {
            return keyRange;
        }
        byte[] lowerRange = keyRange.getLowerRange();
        if (lowerRange != KeyRange.UNBOUND) {
            ptr.set(lowerRange);
            this.position(ptr, pkPos, pkPos+spansToClip-1);
            ptr.set(lowerRange, 0, ptr.getOffset() + ptr.getLength());
            lowerRange = ByteUtil.copyKeyBytesIfNecessary(ptr);
        }
        byte[] upperRange = keyRange.getUpperRange();
        if (upperRange != KeyRange.UNBOUND) {
            ptr.set(upperRange);
            this.position(ptr, pkPos, pkPos+spansToClip-1);
            ptr.set(upperRange, 0, ptr.getOffset() + ptr.getLength());
            upperRange = ByteUtil.copyKeyBytesIfNecessary(ptr);
        }
        return KeyRange.getKeyRange(lowerRange, true, upperRange, true);
    }
}
