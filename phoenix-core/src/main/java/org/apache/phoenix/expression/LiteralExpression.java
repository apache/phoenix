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
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTime;
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
    		NULL_EXPRESSIONS[determinism.ordinal()] = new LiteralExpression(null, determinism);
	        for (int i = 0; i < PDataType.values().length; i++) {
	            TYPED_NULL_EXPRESSIONS[i+PDataType.values().length*determinism.ordinal()] = new LiteralExpression(PDataType.values()[i], determinism);
	        }        
	        BOOLEAN_EXPRESSIONS[determinism.ordinal()] = new LiteralExpression(Boolean.FALSE,
              PBoolean.INSTANCE, PBoolean.INSTANCE.toBytes(Boolean.FALSE), determinism);
	        BOOLEAN_EXPRESSIONS[Determinism.values().length+determinism.ordinal()] = new LiteralExpression(Boolean.TRUE, PBoolean.INSTANCE, PBoolean.INSTANCE.toBytes(Boolean.TRUE), determinism);
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
    	return BOOLEAN_EXPRESSIONS[ (bool==Boolean.FALSE ?  0 : Determinism.values().length) + determinism.ordinal()];
    }

    public static boolean isFalse(Expression child) {
    	if (child!=null) {
    		return child == BOOLEAN_EXPRESSIONS[child.getDeterminism().ordinal()];
    	}
    	return false;
    }
    
    public static boolean isTrue(Expression child) {
    	if (child!=null) {
    		return child == BOOLEAN_EXPRESSIONS[Determinism.values().length+child.getDeterminism().ordinal()];
    	}
    	return false;
    }
    
    public static LiteralExpression newConstant(Object value) {
        return newConstant(value, Determinism.ALWAYS);
    }
    
    // TODO: cache?
    public static LiteralExpression newConstant(Object value, Determinism determinism) {
        if (value instanceof Boolean) {
            return getBooleanLiteralExpression((Boolean)value, determinism);
        }
        else if (value == null) {
            return getNullLiteralExpression(determinism);
        }
        PDataType type = PDataType.fromLiteral(value);
        byte[] b = type.toBytes(value);
        if (type.isNull(b)) {
            return getTypedNullLiteralExpression(type, determinism);
        }
        if (type == PVarchar.INSTANCE) {
            String s = (String) value;
            if (s.length() == b.length) { // single byte characters only
                type = PChar.INSTANCE;
            }
        }
        return new LiteralExpression(value, type, b, determinism);
    }

    public static LiteralExpression newConstant(Object value, PDataType type) throws SQLException {
        return newConstant(value, type, Determinism.ALWAYS);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, Determinism determinism) throws SQLException {
        return newConstant(value, type, SortOrder.getDefault(), determinism);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, SortOrder sortOrder) throws SQLException {
        return newConstant(value, type, null, null, sortOrder, Determinism.ALWAYS);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, SortOrder sortOrder, Determinism determinism) throws SQLException {
        return newConstant(value, type, null, null, sortOrder, determinism);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, Integer maxLength, Integer scale) throws SQLException {
        return newConstant(value, type, maxLength, scale, SortOrder.getDefault(), Determinism.ALWAYS);
    }
    
    public static LiteralExpression newConstant(Object value, PDataType type, Integer maxLength, Integer scale, Determinism determinism) throws SQLException { // remove?
        return newConstant(value, type, maxLength, scale, SortOrder.getDefault(), determinism);
    }

    // TODO: cache?
    public static LiteralExpression newConstant(Object value, PDataType type, Integer maxLength, Integer scale, SortOrder sortOrder, Determinism determinism)
            throws SQLException {
        if (value == null) {
            return  (type == null) ?  getNullLiteralExpression(determinism) : getTypedNullLiteralExpression(type, determinism);
        }
        else if (value instanceof Boolean) {
            return getBooleanLiteralExpression((Boolean)value, determinism);
        }
        PDataType actualType = PDataType.fromLiteral(value);
        // For array we should check individual element in it?
        // It would be costly though!!!!!
        // UpsertStatement can try to cast varchar to date type but PVarchar can't CoercibleTo Date or Timestamp
        // otherwise TO_NUMBER like functions will fail
        if (!actualType.isCoercibleTo(type, value) &&
                (!actualType.equals(PVarchar.INSTANCE) ||
                        !(type.equals(PDate.INSTANCE) || type.equals(PTimestamp.INSTANCE) || type.equals(PTime.INSTANCE)))) {
            throw TypeMismatchException.newException(type, actualType, value.toString());
        }
        value = type.toObject(value, actualType);
        byte[] b = type.toBytes(value, sortOrder);
        if (type == PVarchar.INSTANCE || type == PChar.INSTANCE) {
            if (type == PChar.INSTANCE && maxLength != null  && b.length < maxLength) {
                b = StringUtil.padChar(b, maxLength);
            } else if (value != null) {
                maxLength = ((String)value).length();
            }
        } else if (type.isArrayType()) {
            maxLength = ((PhoenixArray)value).getMaxLength();
        }
        if (b.length == 0) {
            return getTypedNullLiteralExpression(type, determinism);
        }
        if (maxLength == null) {
            maxLength = type == null || !type.isFixedWidth() ? null : type.getMaxLength(value);
        }
        return new LiteralExpression(value, type, b, maxLength, scale, sortOrder, determinism);
    }

    public LiteralExpression() {
    }

    private LiteralExpression(PDataType type, Determinism determinism) {
        this(null, type, ByteUtil.EMPTY_BYTE_ARRAY, determinism);
    }

    private LiteralExpression(Object value, PDataType type, byte[] byteValue, Determinism determinism) {
        this(value, type, byteValue, type == null || !type.isFixedWidth() ? null : type.getMaxLength(value), null, SortOrder.getDefault(), determinism);
    }

    private LiteralExpression(Object value, PDataType type, byte[] byteValue,
            Integer maxLength, Integer scale, SortOrder sortOrder, Determinism deterministic) {
    	Preconditions.checkNotNull(sortOrder);
        this.value = value;
        this.type = type;
        this.byteValue = byteValue;
        this.maxLength = maxLength;
        this.scale = scale != null ? scale : type == null ? null : type.getScale(value);
        this.sortOrder = sortOrder;
        this.determinism = deterministic;
    }

    @Override
    public Determinism getDeterminism() {
        return determinism;
    }
    
    @Override
    public String toString() {
        if (value == null) {
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
        }
        else {
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
