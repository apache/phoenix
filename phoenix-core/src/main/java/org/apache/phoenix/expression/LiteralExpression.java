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
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;


import com.google.common.base.Preconditions;



/**
 * 
 * Accessor for a literal value.
 *
 * 
 * @since 0.1
 */
public class LiteralExpression extends BaseTerminalExpression {
	private static final LiteralExpression[] NULL_EXPRESSIONS = new LiteralExpression[Determinism.values().length];
    private static final LiteralExpression[] TYPED_NULL_EXPRESSIONS = new LiteralExpression[PDataType.values().length * Determinism.values().length];
    private static final LiteralExpression[] BOOLEAN_EXPRESSIONS = new LiteralExpression[2 * Determinism.values().length];

    static {
    	for (Determinism determinism : Determinism.values()) {
            NULL_EXPRESSIONS[determinism.ordinal()] = new LiteralExpression(null, null,
                    ByteUtil.EMPTY_BYTE_ARRAY, null, null, SortOrder.getDefault(), determinism);
	        for (int i = 0; i < PDataType.values().length; i++) {
	            PDataType type = PDataType.values()[i];
                TYPED_NULL_EXPRESSIONS[i+PDataType.values().length*determinism.ordinal()] =
                        new LiteralExpression(null, type, ByteUtil.EMPTY_BYTE_ARRAY,
                                !type.isFixedWidth() ? null : type.getMaxLength(null), null,
                                SortOrder.getDefault(), determinism);
	        }
            BOOLEAN_EXPRESSIONS[determinism.ordinal()] = new LiteralExpression(Boolean.FALSE,
                    PBoolean.INSTANCE, PBoolean.INSTANCE.toBytes(Boolean.FALSE),
                    !PBoolean.INSTANCE.isFixedWidth() ? null :
                            PBoolean.INSTANCE.getMaxLength(null), null, SortOrder.getDefault(),
                    determinism);

            BOOLEAN_EXPRESSIONS[Determinism.values().length + determinism.ordinal()] =
                    new LiteralExpression(Boolean.TRUE, PBoolean.INSTANCE,
                PBoolean.INSTANCE.toBytes(Boolean.TRUE), !PBoolean.INSTANCE.isFixedWidth() ? null
                            : PBoolean.INSTANCE.getMaxLength(null), null, SortOrder.getDefault(),
                            determinism);
    	}
    }

    private Object value;
    private PDataType type;
    private Determinism determinism;
    private byte[] byteValue;
    private Integer maxLength;
    private Integer scale;
    private SortOrder sortOrder;

    private static LiteralExpression getNullLiteralExpression(Determinism determinism) {
        return NULL_EXPRESSIONS[determinism.ordinal()] ;
    }

    private static LiteralExpression getTypedNullLiteralExpression(PDataType type, Determinism determinism){
        return TYPED_NULL_EXPRESSIONS[type.ordinal()+PDataType.values().length*determinism.ordinal()];
    }

    private static LiteralExpression getBooleanLiteralExpression(Boolean bool, Determinism determinism){
        return BOOLEAN_EXPRESSIONS[ (Boolean.FALSE.equals(bool) ?  0 : Determinism.values().length) + determinism.ordinal()];
    }

    public static boolean isFalse(Expression child) {
        if (child != null) {
            return child == BOOLEAN_EXPRESSIONS[child.getDeterminism().ordinal()];
        }
        return false;
    }

    public static boolean isTrue(Expression child) {
        if (child != null) {
            return child == BOOLEAN_EXPRESSIONS[Determinism.values().length+child.getDeterminism().ordinal()];
        }
        return false;
    }

    public static boolean isBooleanNull(Expression child) {
        if (child != null) {
            return child == TYPED_NULL_EXPRESSIONS[PBoolean.INSTANCE.ordinal()+PDataType.values().length*child.getDeterminism().ordinal()];
        }
        return false;
    }

    public static boolean isBooleanFalseOrNull(Expression child) {
        if (child != null) {
            return child == BOOLEAN_EXPRESSIONS[child.getDeterminism().ordinal()]
                    || child == TYPED_NULL_EXPRESSIONS[PBoolean.INSTANCE.ordinal()+PDataType.values().length*child.getDeterminism().ordinal()];
        }
        return false;
    }

    public static class Builder {
        private Object value;
        private PDataType type;
        private Determinism determinism;
        private byte[] byteValue;
        private Integer maxLength;
        private Integer scale;
        private SortOrder sortOrder;
        private Boolean rowKeyOrderOptimizable;
    }

    public static class BuilderA extends Builder {

        private Object value;
        private PDataType type;
        private Determinism determinism;
        private byte[] byteValue;
        private Integer maxLength;
        private Integer scale;
        private SortOrder sortOrder;
        private Boolean rowKeyOrderOptimizable; //Changed type of this field from primitive to object since its value should be true if not explicitly set in Builder

        public BuilderA setValue(Object value) {
            this.value=value;
            return this;
        }

        public BuilderA setDataType(PDataType type) {
            this.type=type;
            return this;
        }

        public BuilderA setDeterminism(Determinism determinism) {
            this.determinism=determinism;
            return this;
        }

        public BuilderA setRowKeyOrderOptimizable(Boolean rowKeyOrderOptimizable) {
            this.rowKeyOrderOptimizable=rowKeyOrderOptimizable;
            return this;
        }

        public BuilderA setMaxLength(Integer maxLength) {
            this.maxLength=maxLength;
            return this;
        }

        public BuilderA setScale(Integer scale) {
            this.scale=scale;
            return this;
        }

        public BuilderA setSortOrder(SortOrder sortOrder) {
            this.sortOrder=sortOrder;
            return this;
        }

        public BuilderA setByteValue(byte[] byteValue) {
            this.byteValue=byteValue;
            return this;
        }

        /**
         * Method to create LiteralExpression after deriving certain attributes
         *
         * ONLY use this build method for LiteralExpressions with:
         * (A) no attributes set,
         * (B) value and/or determinism set (either CAN be null),
         * (C) ONLY byteValue set (CAN be null)
         * If any other attributes are set, use build()
         * @param byteValueOnly TRUE if the only known attribute before build is byteValue; FALSE otherwise
         * @return LiteralExpression
         */
        public LiteralExpression buildSimple(boolean byteValueOnly) {
            if (byteValueOnly) {
                this.byteValue = this.byteValue != null ? this.byteValue : ByteUtil.EMPTY_BYTE_ARRAY;
                this.sortOrder = SortOrder.ASC;
                this.determinism = Determinism.ALWAYS;
                return new LiteralExpression(this);
            }
            if (this.determinism == null) {
                this.determinism = Determinism.ALWAYS;
            }
            if (this.value instanceof Boolean) {
                return LiteralExpression.getBooleanLiteralExpression((Boolean)this.value,
                        this.determinism);
            } else if (this.value == null) {
                return LiteralExpression.getNullLiteralExpression(this.determinism);
            }
            if (this.sortOrder == null) {
                this.sortOrder = SortOrder.getDefault();
            }
            PDataType type = PDataType.fromLiteral(this.value);
            this.byteValue = type.toBytes(this.value);
            if (type.isNull(this.byteValue)) {
                return LiteralExpression.getTypedNullLiteralExpression(type,
                        this.determinism);
            }
            if (type == PVarchar.INSTANCE) {
                String s = (String) this.value;
                if (s.length() == this.byteValue.length) { // single byte characters only
                    type = PChar.INSTANCE;
                }
            }
            this.type = type;
            this.maxLength = type == null || !type.isFixedWidth() ? null
                    : type.getMaxLength(this.value);
            this.sortOrder = SortOrder.getDefault();
            return new LiteralExpression(this);
        }

        /**

         * Method to initialize/derive attributes for Literal Expressions
         * @return LiteralExpression builder
         * @throws SQLException
         */
        public LiteralExpression build() throws SQLException{
            if (this.determinism == null) {
                this.determinism = Determinism.ALWAYS;
            }
            if (this.sortOrder == null) {
                this.sortOrder = SortOrder.getDefault();
            }
            if (this.rowKeyOrderOptimizable == null) {
                this.rowKeyOrderOptimizable = true;
            }
            if (this.value == null) {
                if (this.type == null) {
                    return LiteralExpression.getNullLiteralExpression(this.determinism);
                } else {
                    return LiteralExpression.getTypedNullLiteralExpression(this.type,
                            this.determinism);
                }
            } else if (this.value instanceof Boolean){
                return LiteralExpression.getBooleanLiteralExpression((Boolean)this.value,
                        this.determinism);
            }
            PDataType actualType = PDataType.fromLiteral(this.value);
            this.type = this.type == null ? actualType : this.type;
            try {
                this.value = this.type.toObject(this.value, actualType);
            } catch (IllegalDataException e) {
                throw TypeMismatchException.newException(this.type, actualType,
                        this.value.toString());
            }
            this.byteValue = this.type.isArrayType()
                    ? ((PArrayDataType)this.type).toBytes(this.value,
                    PArrayDataType.arrayBaseType(this.type), this.sortOrder,
                    this.rowKeyOrderOptimizable) : this.type.toBytes(this.value, this.sortOrder);
            if (this.type == PVarchar.INSTANCE || this.type == PChar.INSTANCE) {
                if (this.type == PChar.INSTANCE && this.maxLength != null  && this.byteValue.length
                        < this.maxLength) {
                    if (this.rowKeyOrderOptimizable) {
                        this.byteValue = this.type.pad(this.byteValue, this.maxLength,
                                this.sortOrder);
                    } else {
                        this.byteValue = StringUtil.padChar(this.byteValue, this.maxLength);
                    }
                } else if (this.value != null) {
                    this.maxLength = ((String)this.value).length();
                }
            } else if (this.type.isArrayType()) {
                this.maxLength = ((PhoenixArray)this.value).getMaxLength();
            }
            if (this.byteValue.length == 0) {
                return LiteralExpression.getTypedNullLiteralExpression(this.type, this.determinism);
            }
            if (this.maxLength == null) {
                this.maxLength = this.type.isFixedWidth()
                        ? this.type.getMaxLength(this.value) : null;
            }
            return new LiteralExpression(this);
        }

    }

    public static class BuilderB extends Builder {
        private Object value;
        private PDataType type;
        private Determinism determinism;
        private byte[] byteValue;
        private Integer maxLength;
        private Integer scale;
        private SortOrder sortOrder;
        private Boolean rowKeyOrderOptimizable;

        public BuilderB setValue(Object value) {
            this.value = value;
            return this;
        }

        public BuilderB setDeterminism(Determinism determinism) {
            this.determinism = determinism;
            return this;
        }

        public BuilderB setByteValue(byte[] byteValue) {
            this.byteValue = byteValue;
            return this;
        }

        public LiteralExpression build() {
            if (this.determinism == null) {
                this.determinism = Determinism.ALWAYS;
            }
            if (this.value instanceof Boolean) {
                return LiteralExpression.getBooleanLiteralExpression((Boolean)this.value,
                        this.determinism);
            } else if (this.value == null) {
                return LiteralExpression.getNullLiteralExpression(this.determinism);
            }
            if (this.sortOrder == null) {
                this.sortOrder = SortOrder.getDefault();
            }
            PDataType type = PDataType.fromLiteral(this.value);
            this.byteValue = type.toBytes(this.value);
            if (type.isNull(this.byteValue)) {
                return LiteralExpression.getTypedNullLiteralExpression(type,
                        this.determinism);
            }
            if (type == PVarchar.INSTANCE) {
                String s = (String) this.value;
                if (s.length() == this.byteValue.length) { // single byte characters only
                    type = PChar.INSTANCE;
                }
            }
            this.type = type;
            this.maxLength = type == null || !type.isFixedWidth() ? null
                    : type.getMaxLength(this.value);
            this.sortOrder = SortOrder.getDefault();
            return new LiteralExpression(this);
        }
    }

    /**
     * Constructs a LiteralExpression from a Builder after its attributes have been initialized/derived
     * @param builder
     */
    private LiteralExpression(Builder builder) {
    	Preconditions.checkNotNull(builder.sortOrder);
        this.value = builder.value;
        this.type = builder.type;
        this.byteValue = builder.byteValue;
        this.maxLength = builder.maxLength;
        this.scale = builder.scale != null ? builder.scale : builder.type == null ? null : builder.type.getScale(builder.value);
        this.sortOrder = builder.sortOrder;
        this.determinism = builder.determinism;
    }

    /**
     * Only used for initializing the static arrays used in build()
     * @param value
     * @param type
     * @param byteValue
     * @param maxLength
     * @param scale
     * @param sortOrder
     * @param deterministic
     */
    private LiteralExpression(Object value, PDataType type, byte[] byteValue, Integer maxLength,
                              Integer scale, SortOrder sortOrder, Determinism deterministic) {
        Preconditions.checkNotNull(sortOrder);
        this.value = value;
        this.type = type;
        this.byteValue = byteValue;
        this.maxLength = maxLength;
        this.scale = scale != null ? scale : type == null ? null : type.getScale(value);
        this.sortOrder = sortOrder;
        this.determinism = deterministic;
    }

    @VisibleForTesting
    public LiteralExpression () {
    }

    @Override
    public Determinism getDeterminism() {
        return determinism;
    }
    
    @Override
    public String toString() {
        if (value == null && byteValue!=null) {
            return Bytes.toStringBinary(byteValue);
        } else if (value == null) {
            return "null";
        }
        // TODO: move into PDataType?
        if (type.isCoercibleTo(PTimestamp.INSTANCE)) {
            return type + " " + type.toStringLiteral(value, null);
        }
        return type.toStringLiteral(value, null);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        LiteralExpression other = (LiteralExpression)obj;
        if (value == null) {
            if (other.value != null) return false;
        } else if (!value.equals(other.value)) return false;
        return true;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
    	int encodedByteLengthAndBool = WritableUtils.readVInt(input);
        int byteLength = Math.abs(encodedByteLengthAndBool)-1;
        this.byteValue = new byte[byteLength];
        input.readFully(byteValue, 0, byteLength);
        int sortOrderAndDeterminism = WritableUtils.readVInt(input);
        if (sortOrderAndDeterminism<=2) {
        	//client is on an older version
        	this.determinism = encodedByteLengthAndBool > 0 ? Determinism.ALWAYS : Determinism.PER_ROW;  	
        	this.sortOrder = SortOrder.fromSystemValue(sortOrderAndDeterminism);;
        } else {
        	int determinismOrdinal = (sortOrderAndDeterminism>>2)-1;
        	this.determinism = Determinism.values()[determinismOrdinal];
        	int sortOrderValue = sortOrderAndDeterminism & ((1 << 2) - 1); //get the least 2 significant bits
        	this.sortOrder = SortOrder.fromSystemValue(sortOrderValue);
        } 
        int typeOrdinal = WritableUtils.readVInt(input);
        if (typeOrdinal < 0) {
            this.type = null;
        } else {
            this.type = PDataType.values()[typeOrdinal];
        }
        if (this.byteValue.length == 0) {
            this.value = null;
        } else {
            this.value = this.type.toObject(byteValue, 0, byteValue.length, this.type, sortOrder);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
    	WritableUtils.writeVInt(output, (byteValue.length + 1) * (this.determinism==Determinism.ALWAYS ? 1 : -1));
        output.write(byteValue);
        // since we need to support clients of a lower version, serialize the determinism enum ordinal in the int used to 
        // serialize sort order system value (which is either 1 or 2)
        int sortOrderAndDeterminism = ((this.determinism.ordinal()+1)<<2) + sortOrder.getSystemValue();
        WritableUtils.writeVInt(output, sortOrderAndDeterminism);
        WritableUtils.writeVInt(output, this.type == null ? -1 : this.type.ordinal());
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // Literal always evaluates, even when it returns null
        ptr.set(byteValue);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return type;
    }

    @Override
    public Integer getMaxLength() {
        // For literals representing arrays of CHAR or BINARY, the byte size is null and the max
        // length of the expression is also null, so we must get the max length of the
        // actual underlying array
        if (maxLength == null && getDataType() != null && getDataType().isArrayType() &&
                PDataType.arrayBaseType(getDataType()).getByteSize() == null) {
            Object value = getValue();
            if (value instanceof PhoenixArray) {
                // Return the max length of the underlying PhoenixArray data
                return ((PhoenixArray) value).getMaxLength();
            }
        }
        return maxLength;
    }

    @Override
    public Integer getScale() {
        return scale;
    }
    
    @Override
    public SortOrder getSortOrder() {
        return sortOrder;
    }

    @Override
    public boolean isNullable() {
        return value == null;
    }

    public Object getValue() {
        return value;
    }

    public byte[] getBytes() {
        return byteValue;
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public boolean isStateless() {
        return true;
    }
}
