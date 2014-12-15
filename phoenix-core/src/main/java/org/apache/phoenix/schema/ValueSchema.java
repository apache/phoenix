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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SizedUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * 
 * Simple flat schema over a byte array where fields may be any of {@link org.apache.phoenix.schema.types.PDataType}.
 * Optimized for positional access by index.
 *
 * 
 * @since 0.1
 */
public abstract class ValueSchema implements Writable {
    public static final int ESTIMATED_VARIABLE_LENGTH_SIZE = 10;
    private int[] fieldIndexByPosition;
    private List<Field> fields;
    private int estimatedLength;
    private boolean isFixedLength;
    private boolean isMaxLength;
    private int minNullable;
    
    public ValueSchema() {
    }
    
    protected ValueSchema(int minNullable, List<Field> fields) {
        init(minNullable, fields);
    }
    
    @Override
    public String toString() {
        return fields.toString();
    }
    
    public int getEstimatedSize() { // Memory size of ValueSchema
        int count = fieldIndexByPosition.length;
        return SizedUtil.OBJECT_SIZE + SizedUtil.POINTER_SIZE + SizedUtil.INT_SIZE * (4 + count) + 
                SizedUtil.ARRAY_SIZE + count * Field.ESTIMATED_SIZE + SizedUtil.sizeOfArrayList(count);
    }

    private void init(int minNullable, List<Field> fields) {
        this.minNullable = minNullable;
        this.fields = ImmutableList.copyOf(fields);
        int estimatedLength = 0;
        boolean isMaxLength = true, isFixedLength = true;
        int positions = 0;
        for (Field field : fields) {
            int fieldEstLength = 0;
            PDataType type = field.getDataType();
            Integer byteSize = type.getByteSize();
            if (type.isFixedWidth()) {
                fieldEstLength += field.getByteSize();
            } else {
                isFixedLength = false;
                // Account for vint for length if not fixed
                if (byteSize == null) {
                    isMaxLength = false;
                    fieldEstLength += ESTIMATED_VARIABLE_LENGTH_SIZE;
                } else {
                    fieldEstLength += WritableUtils.getVIntSize(byteSize);
                    fieldEstLength = byteSize;
                }
            }
            positions += field.getCount();
            estimatedLength += fieldEstLength * field.getCount();
        }
        fieldIndexByPosition = new int[positions];
        for (int i = 0, j= 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Arrays.fill(fieldIndexByPosition, j, j + field.getCount(), i);
            j += field.getCount();
        }
        this.isFixedLength = isFixedLength;
        this.isMaxLength = isMaxLength;
        this.estimatedLength = estimatedLength;
    }
    
    public int getFieldCount() {
        return fieldIndexByPosition.length;
    }
    
    public List<Field> getFields() {
        return fields;
    }
    
    /**
     * @return true if all types are fixed width
     */
    public boolean isFixedLength() {
        return isFixedLength;
    }
    
    /**
     * @return true if {@link #getEstimatedValueLength()} returns the maximum length
     * of a serialized value for this schema
     */
    public boolean isMaxLength() {
        return isMaxLength;
    }
    
    /**
     * @return estimated size in bytes of a serialized value for this schema
     */
    public int getEstimatedValueLength() {
        return estimatedLength;
    }
    
    /**
     * Non-nullable fields packed to the left so that we do not need to store trailing nulls.
     * Knowing the minimum position of a nullable field enables this.
     * @return the minimum position of a nullable field
     */
    public int getMinNullable() {
        return minNullable;
    }
    
    public static final class Field implements Writable, PDatum {
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + byteSize;
            result = prime * result + type.hashCode();            
            result = prime * result + sortOrder.hashCode();
            result = prime * result + (isNullable ? 1231 : 1237);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            Field other = (Field)obj;
            if (byteSize != other.byteSize) return false;
            if (sortOrder != other.sortOrder) return false;
            if (isNullable != other.isNullable) return false;
            if (type != other.type) return false;
            return true;
        }
        
        public static final int ESTIMATED_SIZE = SizedUtil.OBJECT_SIZE + SizedUtil.POINTER_SIZE * 2 + SizedUtil.INT_SIZE * 3;

        private int count;
        private PDataType type;
        private int byteSize = 0;
        private boolean isNullable;
        private SortOrder sortOrder;
        
        public Field() {
        }
        
        private Field(PDatum datum, boolean isNullable, int count, SortOrder sortOrder) {
            Preconditions.checkNotNull(sortOrder);
            this.type = datum.getDataType();
            this.sortOrder = sortOrder;
            this.count = count;
            this.isNullable = isNullable;
            if (this.type.isFixedWidth() && this.type.getByteSize() == null) {
                if (datum.getMaxLength() != null) {
                    this.byteSize = datum.getMaxLength();
                }
            }
        }
        
        @Override
        public String toString() {
            return (count == 1 ? "" : count + " * ") 
                    + type.toString() 
                    + (byteSize == 0 ? "" : "(" + byteSize + ")") 
                    + (isNullable ? "" : " NOT NULL") 
                    + (sortOrder == SortOrder.ASC ? "" : " " + sortOrder);
        }
        
        private Field(Field field, int count) {
            this.type = field.getDataType();
            this.byteSize = field.byteSize;
            this.count = count;
            this.sortOrder = SortOrder.getDefault();
        }
        
        @Override
        public final SortOrder getSortOrder() {
            return sortOrder;
        }
        
        @Override
        public final PDataType getDataType() {
            return type;
        }
        
        @Override
        public final boolean isNullable() {
            return isNullable;
        }
        
        public final int getByteSize() {
            return type.getByteSize() == null ? byteSize : type.getByteSize();
        }
        
        public final int getCount() {
            return count;
        }

        @Override
        public Integer getMaxLength() {
            return type.isFixedWidth() ? byteSize : null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            // Encode isNullable in sign bit of type ordinal (offset by 1, since ordinal could be 0)
            int typeOrdinal = WritableUtils.readVInt(input);
            if (typeOrdinal < 0) {
                typeOrdinal *= -1;
                this.isNullable = true;
            }
            this.type = PDataType.values()[typeOrdinal-1];
            this.count = WritableUtils.readVInt(input);
            if (this.count < 0) {
                this.count *= -1;
                this.sortOrder = SortOrder.DESC;
            } else {
            	this.sortOrder = SortOrder.ASC;
            }
            if (this.type.isFixedWidth() && this.type.getByteSize() == null) {
                this.byteSize = WritableUtils.readVInt(input);
            }
        }

        @Override
        public void write(DataOutput output) throws IOException {
            WritableUtils.writeVInt(output, (type.ordinal() + 1) * (this.isNullable ? -1 : 1));
            WritableUtils.writeVInt(output, count * (sortOrder == SortOrder.ASC ? 1 : -1));
            if (type.isFixedWidth() && type.getByteSize() == null) {
                WritableUtils.writeVInt(output, byteSize);
            }
        }
    }
    
    public abstract static class ValueSchemaBuilder {
        protected List<Field> fields = new ArrayList<Field>();
        protected int nFields = Integer.MAX_VALUE;
        protected final int minNullable;
        
        public ValueSchemaBuilder(int minNullable) {
            this.minNullable = minNullable;
        }
        
        protected List<Field> buildFields() {
            List<Field> condensedFields = new ArrayList<Field>(fields.size());
            for (int i = 0; i < Math.min(nFields,fields.size()); ) {
                Field field = fields.get(i);
                int count = 1;
                while ( ++i < fields.size() && field.equals(fields.get(i))) {
                    count++;
                }
                condensedFields.add(count == 1 ? field : new Field(field,count));
            }
            return condensedFields;
        }

        abstract public ValueSchema build();

        public ValueSchemaBuilder setMaxFields(int nFields) {
            this.nFields = nFields;
            return this;
        }
        
        protected ValueSchemaBuilder addField(PDatum datum, boolean isNullable, SortOrder sortOrder) {
            if(fields.size() >= nFields) {
                throw new IllegalArgumentException("Adding too many fields to Schema (max " + nFields + ")");
            }
            fields.add(new Field(datum, isNullable, 1, sortOrder));
            return this;
        }

        public ValueSchemaBuilder addField(Field field) {
            fields.add(field);
            return this;
        }
    }
    
    public int getEstimatedByteSize() {
        int size = 0;
        size += WritableUtils.getVIntSize(minNullable);
        size += WritableUtils.getVIntSize(fields.size());
        size += fields.size() * 3;
        return size;
    }
    
    public void serialize(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, minNullable);
        WritableUtils.writeVInt(output, fields.size());
        for (int i = 0; i < fields.size(); i++) {
            fields.get(i).write(output);
        }
    }
    
    public Field getField(int position) {
        return fields.get(fieldIndexByPosition[position]);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + minNullable;
        for (Field field : fields) {
        	result = prime * result + field.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ValueSchema other = (ValueSchema)obj;
        if (minNullable != other.minNullable) return false;
        if (fields.size() != other.fields.size()) return false;
        for (int i = 0; i < fields.size(); i++) {
        	if (!fields.get(i).equals(other.fields.get(i)))
        		return false;
        }
        return true;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        int minNullable = WritableUtils.readVInt(in);
        int nFields = WritableUtils.readVInt(in);
        List<Field> fields = Lists.newArrayListWithExpectedSize(nFields);
        for (int i = 0; i < nFields; i++) {
            Field field = new Field();
            field.readFields(in);
            fields.add(field);
        }
        init(minNullable, fields);
    }
         
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, minNullable);
        WritableUtils.writeVInt(out, fields.size());
        for (int i = 0; i < fields.size(); i++) {
            fields.get(i).write(out);
        }
    }

}
