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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ExpressionUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/*
 * Implementation of a SQL foo IN (a,b,c) expression. Other than the first
 * expression, child expressions must be constants.
 *
 */
public class InListExpression extends BaseSingleExpression {
    private Set<ImmutableBytesPtr> values;
    private ImmutableBytesPtr minValue;
    private ImmutableBytesPtr maxValue;
    private int valuesByteLength;
    private int fixedWidth = -1;
    private List<Expression> keyExpressions; // client side only

    public static Expression create (List<Expression> children, boolean isNegate, ImmutableBytesWritable ptr) throws SQLException {
        Expression firstChild = children.get(0);
        
        if (firstChild.isStateless() && (!firstChild.evaluate(null, ptr) || ptr.getLength() == 0)) {
            return LiteralExpression.newConstant(null, PBoolean.INSTANCE, firstChild.getDeterminism());
        }
        if (children.size() == 2) {
            return ComparisonExpression.create(isNegate ? CompareOp.NOT_EQUAL : CompareOp.EQUAL, children, ptr);
        }
        
        boolean addedNull = false;
        SQLException sqlE = null;
        List<Expression> coercedKeyExpressions = Lists.newArrayListWithExpectedSize(children.size());
        coercedKeyExpressions.add(firstChild);
        for (int i = 1; i < children.size(); i++) {
            try {
                Expression rhs = BaseExpression.coerce(firstChild, children.get(i), CompareOp.EQUAL);
                coercedKeyExpressions.add(rhs);
            } catch (SQLException e) {
                // Type mismatch exception or invalid data exception.
                // Ignore and filter the element from the list and it means it cannot possibly
                // be in the list. If list is empty, we'll throw the last exception we ignored,
                // as this is an error condition.
                sqlE = e;
            }
        }
        if (coercedKeyExpressions.size() == 1) {
            throw sqlE;
        }
        if (coercedKeyExpressions.size() == 2 && addedNull) {
            return LiteralExpression.newConstant(null, PBoolean.INSTANCE, Determinism.ALWAYS);
        }
        Expression expression = new InListExpression(coercedKeyExpressions);
        if (isNegate) { 
            expression = NotExpression.create(expression, ptr);
        }
        if (ExpressionUtil.isConstant(expression)) {
            return ExpressionUtil.getConstantExpression(expression, ptr);
        }
        return expression;
    }
    
    public InListExpression() {
    }

    public InListExpression(List<Expression> keyExpressions) {
        super(keyExpressions.get(0));
        this.keyExpressions = keyExpressions.subList(1, keyExpressions.size());
        Set<ImmutableBytesPtr> values = Sets.newHashSetWithExpectedSize(keyExpressions.size()-1);
        int fixedWidth = -1;
        boolean isFixedLength = true;
        for (int i = 1; i < keyExpressions.size(); i++) {
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            Expression child = keyExpressions.get(i);
            child.evaluate(null, ptr);
            if (ptr.getLength() > 0) { // filter null as it has no impact
                if (values.add(ptr)) {
                    int length = ptr.getLength();
                    if (fixedWidth == -1) {
                        fixedWidth = length;
                    } else {
                        isFixedLength &= fixedWidth == length;
                    }
                    
                    valuesByteLength += ptr.getLength();
                }
            }
        }
        this.fixedWidth = isFixedLength ? fixedWidth : -1;
        // Sort values by byte value so we can get min/max easily
        ImmutableBytesPtr[] valuesArray = values.toArray(new ImmutableBytesPtr[values.size()]);
        Arrays.sort(valuesArray, ByteUtil.BYTES_PTR_COMPARATOR);
        if (values.isEmpty()) {
            this.minValue = ByteUtil.EMPTY_BYTE_ARRAY_PTR;
            this.maxValue = ByteUtil.EMPTY_BYTE_ARRAY_PTR;
            this.values = Collections.emptySet();
        } else {
            this.minValue = valuesArray[0];
            this.maxValue = valuesArray[valuesArray.length-1];
            // Use LinkedHashSet on client-side so that we don't need to serialize the
            // minValue and maxValue but can infer them based on the first and last position.
            this.values = new LinkedHashSet<ImmutableBytesPtr>(Arrays.asList(valuesArray));
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getChild().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) { // null IN (...) is always null
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        if (values.contains(ptr)) {
            ptr.set(PDataType.TRUE_BYTES);
            return true;
        }
        ptr.set(PDataType.FALSE_BYTES);
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + values.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        InListExpression other = (InListExpression)obj;
        if (!values.equals(other.values)) return false;
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PBoolean.INSTANCE;
    }

    private int readValue(DataInput input, byte[] valuesBytes, int offset, ImmutableBytesPtr ptr) throws IOException {
        int valueLen = fixedWidth == -1 ? WritableUtils.readVInt(input) : fixedWidth;
        values.add(new ImmutableBytesPtr(valuesBytes,offset,valueLen));
        return offset + valueLen;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        input.readBoolean(); // Unused, but left for b/w compat. TODO: remove in next major release
        fixedWidth = WritableUtils.readVInt(input);
        byte[] valuesBytes = Bytes.readByteArray(input);
        valuesByteLength = valuesBytes.length;
        int len = fixedWidth == -1 ? WritableUtils.readVInt(input) : valuesByteLength / fixedWidth;
        // TODO: consider using a regular HashSet as we never serialize from the server-side
        values = Sets.newLinkedHashSetWithExpectedSize(len);
        int offset = 0;
        int i  = 0;
        if (i < len) {
            offset = readValue(input, valuesBytes, offset, minValue = new ImmutableBytesPtr());
            while (++i < len-1) {
                offset = readValue(input, valuesBytes, offset, new ImmutableBytesPtr());
            }
            if (i < len) {
                offset = readValue(input, valuesBytes, offset, maxValue = new ImmutableBytesPtr());
            } else {
                maxValue = minValue;
            }
        } else {
            minValue = maxValue = new ImmutableBytesPtr(ByteUtil.EMPTY_BYTE_ARRAY);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        output.writeBoolean(false); // Unused, but left for b/w compat. TODO: remove in next major release
        WritableUtils.writeVInt(output, fixedWidth);
        WritableUtils.writeVInt(output, valuesByteLength);
        for (ImmutableBytesPtr ptr : values) {
            output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
        }
        if (fixedWidth == -1) {
            WritableUtils.writeVInt(output, values.size());
            for (ImmutableBytesPtr ptr : values) {
                WritableUtils.writeVInt(output, ptr.getLength());
            }
        }
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }

    public List<Expression> getKeyExpressions() {
        return keyExpressions;
    }

    public ImmutableBytesWritable getMinKey() {
        return minValue;
    }

    public ImmutableBytesWritable getMaxKey() {
        return maxValue;
    }

    @Override
    public String toString() {
        int maxToStringLen = 200;
        Expression firstChild = children.get(0);
        PDataType type = firstChild.getDataType();
        StringBuilder buf = new StringBuilder(firstChild + " IN (");
        for (ImmutableBytesPtr value : values) {
            if (firstChild.getSortOrder() != null) {
                type.coerceBytes(value, type, firstChild.getSortOrder(), SortOrder.getDefault());
            }
            buf.append(type.toStringLiteral(value, null));
            buf.append(',');
            if (buf.length() >= maxToStringLen) {
                buf.append("... ");
                break;
            }
        }
        buf.setCharAt(buf.length()-1,')');
        return buf.toString();
    }
}
