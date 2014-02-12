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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;



/**
 * 
 * Accessor for a literal value.
 *
 * 
 * @since 0.1
 */
public class LiteralExpression extends BaseTerminalExpression {
    public static final LiteralExpression NULL_EXPRESSION = new LiteralExpression(null);
    private static final LiteralExpression[] TYPED_NULL_EXPRESSIONS = new LiteralExpression[PDataType.values().length];
    static {
        for (int i = 0; i < TYPED_NULL_EXPRESSIONS.length; i++) {
            TYPED_NULL_EXPRESSIONS[i] = new LiteralExpression(PDataType.values()[i]);
        }
    }
    public static final LiteralExpression FALSE_EXPRESSION = new LiteralExpression(Boolean.FALSE, PDataType.BOOLEAN, PDataType.BOOLEAN.toBytes(Boolean.FALSE));
    public static final LiteralExpression TRUE_EXPRESSION = new LiteralExpression(Boolean.TRUE, PDataType.BOOLEAN, PDataType.BOOLEAN.toBytes(Boolean.TRUE));

    private Object value;
    private PDataType type;
    private byte[] byteValue;
    private Integer byteSize;
    private Integer maxLength;
    private Integer scale;
    private ColumnModifier columnModifier;

    // TODO: cache?
    public static LiteralExpression newConstant(Object value) {
        if (Boolean.FALSE.equals(value)) {
            return FALSE_EXPRESSION;
        }
        if (Boolean.TRUE.equals(value)) {
            return TRUE_EXPRESSION;
        }
        if (value == null) {
            return NULL_EXPRESSION;
        }
        PDataType type = PDataType.fromLiteral(value);
        byte[] b = type.toBytes(value);
        if (b.length == 0) {
            return TYPED_NULL_EXPRESSIONS[type.ordinal()];
        }
        if (type == PDataType.VARCHAR) {
            String s = (String) value;
            if (s.length() == b.length) { // single byte characters only
                type = PDataType.CHAR;
            }
        }
        return new LiteralExpression(value, type, b);
    }

    public static LiteralExpression newConstant(Object value, PDataType type) throws SQLException {
        return newConstant(value, type, null);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, ColumnModifier columnModifier) throws SQLException {
        return newConstant(value, type, null, null, columnModifier);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, Integer maxLength, Integer scale) throws SQLException { // remove?
        return newConstant(value, type, maxLength, scale, null);
    }

    // TODO: cache?
    public static LiteralExpression newConstant(Object value, PDataType type, Integer maxLength, Integer scale, ColumnModifier columnModifier)
            throws SQLException {
        if (value == null) {
            if (type == null) {
                return NULL_EXPRESSION;
            }
            return TYPED_NULL_EXPRESSIONS[type.ordinal()];
        }
        PDataType actualType = PDataType.fromLiteral(value);
        if (!actualType.isCoercibleTo(type, value)) {
            throw new TypeMismatchException(type, actualType, value.toString());
        }
        value = type.toObject(value, actualType);
        try {
            byte[] b = type.toBytes(value, columnModifier);
            if (type == PDataType.VARCHAR || type == PDataType.CHAR) {
                if (type == PDataType.CHAR && maxLength != null  && b.length < maxLength) {
                    b = StringUtil.padChar(b, maxLength);
                } else if (value != null) {
                    maxLength = ((String)value).length();
                }
            }
            if (b.length == 0) {
                return TYPED_NULL_EXPRESSIONS[type.ordinal()];
            }
            return new LiteralExpression(value, type, b, maxLength, scale, columnModifier);
        } catch (IllegalDataException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA).setRootCause(e).build().buildException();
        }
    }

    public LiteralExpression() {
    }

    private LiteralExpression(PDataType type) {
        this(null, type, ByteUtil.EMPTY_BYTE_ARRAY);
    }

    private LiteralExpression(Object value, PDataType type, byte[] byteValue) {
        this(value, type, byteValue, type == null? null : type.getMaxLength(value), type == null? null : type.getScale(value), null);
    }

    private LiteralExpression(Object value, PDataType type, byte[] byteValue,
            Integer maxLength, Integer scale, ColumnModifier columnModifier) {
        this.value = value;
        this.type = type;
        this.byteValue = byteValue;
        this.byteSize = byteValue.length;
        this.maxLength = maxLength;
        this.scale = scale;
        this.columnModifier = columnModifier;
    }

    @Override
    public String toString() {
        return type != null && type.isCoercibleTo(PDataType.VARCHAR) ? "'" + value + "'" : "" + value;
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
        this.byteValue = Bytes.readByteArray(input);
        columnModifier = ColumnModifier.fromSystemValue(WritableUtils.readVInt(input));
        if (this.byteValue.length > 0) {
            this.type = PDataType.values()[WritableUtils.readVInt(input)];
            this.value = this.type.toObject(byteValue, 0, byteValue.length, this.type, columnModifier);
        }
        byteSize = this.byteValue.length;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Bytes.writeByteArray(output, byteValue);
        WritableUtils.writeVInt(output, ColumnModifier.toSystemValue(columnModifier));
        if (this.byteValue.length > 0) {
            WritableUtils.writeVInt(output, this.type.ordinal());
        }
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
    public Integer getByteSize() {
        return byteSize;
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
    public ColumnModifier getColumnModifier() {
        return columnModifier;
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
    public boolean isConstant() {
        return true;
    }
}
