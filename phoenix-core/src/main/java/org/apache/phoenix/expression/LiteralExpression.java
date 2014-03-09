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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
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
    public static final LiteralExpression NULL_EXPRESSION = new LiteralExpression(null, false);
    private static final LiteralExpression ND_NULL_EXPRESSION = new LiteralExpression(null, true);
    private static final LiteralExpression[] TYPED_NULL_EXPRESSIONS = new LiteralExpression[PDataType.values().length * 2];
    static {
        for (int i = 0; i < PDataType.values().length; i++) {
            TYPED_NULL_EXPRESSIONS[i] = new LiteralExpression(PDataType.values()[i], true);
        }
        for (int i = 0; i < PDataType.values().length; i++) {
            TYPED_NULL_EXPRESSIONS[i+PDataType.values().length] = new LiteralExpression(PDataType.values()[i], false);
        }
    }
    private static final LiteralExpression FALSE_EXPRESSION = new LiteralExpression(Boolean.FALSE, PDataType.BOOLEAN, PDataType.BOOLEAN.toBytes(Boolean.FALSE), true);
    private static final LiteralExpression TRUE_EXPRESSION = new LiteralExpression(Boolean.TRUE, PDataType.BOOLEAN, PDataType.BOOLEAN.toBytes(Boolean.TRUE), true);
    private static final LiteralExpression ND_FALSE_EXPRESSION = new LiteralExpression(Boolean.FALSE, PDataType.BOOLEAN, PDataType.BOOLEAN.toBytes(Boolean.FALSE), false);
    private static final LiteralExpression ND_TRUE_EXPRESSION = new LiteralExpression(Boolean.TRUE, PDataType.BOOLEAN, PDataType.BOOLEAN.toBytes(Boolean.TRUE), false);

    private Object value;
    private PDataType type;
    private boolean isDeterministic;
    private byte[] byteValue;
    private Integer maxLength;
    private Integer scale;
    private SortOrder sortOrder;


    public static boolean isFalse(Expression child) {
        return child == FALSE_EXPRESSION || child == ND_FALSE_EXPRESSION;
    }
    
    public static boolean isTrue(Expression child) {
        return child == TRUE_EXPRESSION || child == ND_TRUE_EXPRESSION;
    }
    
    public static LiteralExpression newConstant(Object value) {
        return newConstant(value, true);
    }
    
    // TODO: cache?
    public static LiteralExpression newConstant(Object value, boolean isDeterministic) {
        if (Boolean.FALSE.equals(value)) {
            return isDeterministic ? FALSE_EXPRESSION : ND_FALSE_EXPRESSION;
        }
        if (Boolean.TRUE.equals(value)) {
            return isDeterministic ? TRUE_EXPRESSION : ND_TRUE_EXPRESSION;
        }
        if (value == null) {
            return isDeterministic ? NULL_EXPRESSION : ND_NULL_EXPRESSION;
        }
        PDataType type = PDataType.fromLiteral(value);
        byte[] b = type.toBytes(value);
        if (type.isNull(b)) {
            return TYPED_NULL_EXPRESSIONS[type.ordinal() + ( isDeterministic ? 0 : TYPED_NULL_EXPRESSIONS.length/2)];
        }
        if (type == PDataType.VARCHAR) {
            String s = (String) value;
            if (s.length() == b.length) { // single byte characters only
                type = PDataType.CHAR;
            }
        }
        return new LiteralExpression(value, type, b, isDeterministic);
    }

    public static LiteralExpression newConstant(Object value, PDataType type) throws SQLException {
        return newConstant(value, type, true);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, boolean isDeterministic) throws SQLException {
        return newConstant(value, type, SortOrder.getDefault(), isDeterministic);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, SortOrder sortOrder) throws SQLException {
        return newConstant(value, type, null, null, sortOrder, true);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, SortOrder sortOrder, boolean isDeterministic) throws SQLException {
        return newConstant(value, type, null, null, sortOrder, isDeterministic);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, Integer maxLength, Integer scale) throws SQLException {
        return newConstant(value, type, maxLength, scale, SortOrder.getDefault(), true);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, Integer maxLength, Integer scale, boolean isDeterministic) throws SQLException { // remove?
        return newConstant(value, type, maxLength, scale, SortOrder.getDefault(), isDeterministic);
    }

    // TODO: cache?
    public static LiteralExpression newConstant(Object value, PDataType type, Integer maxLength, Integer scale, SortOrder sortOrder, boolean isDeterministic)
            throws SQLException {
        if (value == null) {
            if (type == null) {
                return NULL_EXPRESSION;
            }
            return TYPED_NULL_EXPRESSIONS[type.ordinal()];
        }
        if (Boolean.FALSE.equals(value)) {
            return isDeterministic ? FALSE_EXPRESSION : ND_FALSE_EXPRESSION;
        }
        if (Boolean.TRUE.equals(value)) {
            return isDeterministic ? TRUE_EXPRESSION : ND_TRUE_EXPRESSION;
        }
        PDataType actualType = PDataType.fromLiteral(value);
        // For array we should check individual element in it?
        // It would be costly though!!!!!
        if (!actualType.isCoercibleTo(type, value)) {
            throw TypeMismatchException.newException(type, actualType, value.toString());
        }
        value = type.toObject(value, actualType);
        try {
            byte[] b = type.toBytes(value, sortOrder);
            if (type == PDataType.VARCHAR || type == PDataType.CHAR) {
                if (type == PDataType.CHAR && maxLength != null  && b.length < maxLength) {
                    b = StringUtil.padChar(b, maxLength);
                } else if (value != null) {
                    maxLength = ((String)value).length();
                }
            } else if (type.isArrayType()) {
                maxLength = ((PhoenixArray)value).getMaxLength();
            }
            if (b.length == 0) {
                return TYPED_NULL_EXPRESSIONS[type.ordinal()];
            }
            if (maxLength == null) {
                maxLength = type == null || !type.isFixedWidth() ? null : type.getMaxLength(value);
            }
            return new LiteralExpression(value, type, b, maxLength, scale, sortOrder, isDeterministic);
        } catch (IllegalDataException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA).setRootCause(e).build().buildException();
        }
    }

    public LiteralExpression() {
    }

    private LiteralExpression(PDataType type, boolean isDeterministic) {
        this(null, type, ByteUtil.EMPTY_BYTE_ARRAY, isDeterministic);
    }

    private LiteralExpression(Object value, PDataType type, byte[] byteValue, boolean isDeterministic) {
        this(value, type, byteValue, type == null || !type.isFixedWidth() ? null : type.getMaxLength(value), null, SortOrder.getDefault(), isDeterministic);
    }

    private LiteralExpression(Object value, PDataType type, byte[] byteValue,
            Integer maxLength, Integer scale, SortOrder sortOrder, boolean isDeterministic) {
    	Preconditions.checkNotNull(sortOrder);
        this.value = value;
        this.type = type;
        this.byteValue = byteValue;
        this.maxLength = maxLength;
        this.scale = scale != null ? scale : type == null ? null : type.getScale(value);
        this.sortOrder = sortOrder;
        this.isDeterministic = isDeterministic;
    }

    @Override
    public boolean isDeterministic() {
        return isDeterministic;
    }
    
    @Override
    public String toString() {
        return value == null ? "null" : type.toStringLiteral(byteValue, null);
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
        this.isDeterministic = encodedByteLengthAndBool > 0;
        int byteLength = Math.abs(encodedByteLengthAndBool)-1;
        this.byteValue = new byte[byteLength];
        input.readFully(byteValue, 0, byteLength);
        sortOrder = SortOrder.fromSystemValue(WritableUtils.readVInt(input));
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
        WritableUtils.writeVInt(output, (byteValue.length + 1) * (this.isDeterministic ? 1 : -1));
        output.write(byteValue);
        WritableUtils.writeVInt(output, sortOrder.getSystemValue());
        WritableUtils.writeVInt(output, type == null ? -1 : this.type.ordinal());
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
