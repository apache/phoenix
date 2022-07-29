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
import java.util.ArrayList;

import org.apache.hadoop.hbase.CompareOperator;
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
import org.apache.phoenix.util.StringUtil;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

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
    private boolean rowKeyOrderOptimizable; // client side only

    // reduce hashCode() complexity
    private int hashCode = -1;
    private boolean hashCodeSet = false;

    public static Expression create (List<Expression> children, boolean isNegate, ImmutableBytesWritable ptr, boolean rowKeyOrderOptimizable) throws SQLException {
        if (children.size() == 1) {
            throw new SQLException("No element in the IN list");
        }

        Expression firstChild = children.get(0);

        if (firstChild.isStateless() && (!firstChild.evaluate(null, ptr) || ptr.getLength() == 0)) {
            return LiteralExpression.newConstant(null, PBoolean.INSTANCE, firstChild.getDeterminism());
        }

        List<Expression> childrenWithoutNulls = Lists.newArrayList();
        for (Expression child : children){
            if(!child.equals(LiteralExpression.newConstant(null))){
                childrenWithoutNulls.add(child);
            }
        }
        if (childrenWithoutNulls.size() <= 1 ) {
            // In case of after removing nulls there is no remaining element in the IN list
            return LiteralExpression.newConstant(false);
        }

        if (firstChild instanceof RowValueConstructorExpression) {
            List<InListColumnKeyValuePair> inListColumnKeyValuePairList =
                    getSortedInListColumnKeyValuePair(childrenWithoutNulls);
            if (inListColumnKeyValuePairList != null) {
                childrenWithoutNulls = getSortedRowValueConstructorExpressionList(
                        inListColumnKeyValuePairList, firstChild.isStateless(),children.size() - 1);
                firstChild = childrenWithoutNulls.get(0);
            }
        }

        boolean nullInList = children.size() != childrenWithoutNulls.size();

        if (childrenWithoutNulls.size() == 2 && !nullInList) {
            return ComparisonExpression.create(isNegate ? CompareOperator.NOT_EQUAL : CompareOperator.EQUAL,
                    childrenWithoutNulls, ptr, rowKeyOrderOptimizable);
        }

        SQLException sqlE = null;
        List<Expression> coercedKeyExpressions = Lists.newArrayListWithExpectedSize(childrenWithoutNulls.size());
        coercedKeyExpressions.add(firstChild);
        for (int i = 1; i < childrenWithoutNulls.size(); i++) {
            try {
                Expression rhs = BaseExpression.coerce(firstChild, childrenWithoutNulls.get(i),
                        CompareOperator.EQUAL, rowKeyOrderOptimizable);
                coercedKeyExpressions.add(rhs);
            } catch (SQLException e) {
                // Type mismatch exception or invalid data exception.
                // Ignore and filter the element from the list and it means it cannot possibly
                // be in the list. If list is empty, we'll throw the last exception we ignored,
                // as this is an error condition.
                sqlE = e;
            }
        }
        if (coercedKeyExpressions.size() <= 1 ) {
            if(nullInList || sqlE == null){
                // In case of after removing nulls there is no remaining element in the IN list
                return LiteralExpression.newConstant(false);
            } else {
                throw sqlE;
            }
        }
        if (coercedKeyExpressions.size() == 2) {
            return ComparisonExpression.create(isNegate ? CompareOperator.NOT_EQUAL : CompareOperator.EQUAL,
                    coercedKeyExpressions, ptr, rowKeyOrderOptimizable);
        }
        Expression expression = new InListExpression(coercedKeyExpressions, rowKeyOrderOptimizable);
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

    @VisibleForTesting
    protected InListExpression(List<ImmutableBytesPtr> values) {
        this.children = Collections.emptyList();
        this.values = Sets.newHashSet(values);
    }

    public InListExpression(List<Expression> keyExpressions, boolean rowKeyOrderOptimizable) {
        super(keyExpressions.get(0));
        this.rowKeyOrderOptimizable = rowKeyOrderOptimizable;
        Expression firstChild = keyExpressions.get(0);
        this.keyExpressions = keyExpressions.subList(1, keyExpressions.size());
        Set<ImmutableBytesPtr> values = Sets.newHashSetWithExpectedSize(keyExpressions.size()-1);
        Integer maxLength = firstChild.getDataType().isFixedWidth() ? firstChild.getMaxLength() : null;
        int fixedWidth = -1;
        boolean isFixedLength = true;
        for (int i = 1; i < keyExpressions.size(); i++) {
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            Expression child = keyExpressions.get(i);
            child.evaluate(null, ptr);
            if (ptr.getLength() > 0) { // filter null as it has no impact
                if (rowKeyOrderOptimizable) {
                    firstChild.getDataType().pad(ptr, maxLength, firstChild.getSortOrder());
                } else if (maxLength != null) {
                    byte[] paddedBytes = StringUtil.padChar(ByteUtil.copyKeyBytesIfNecessary(ptr), maxLength);
                    ptr.set(paddedBytes);
                }
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
        this.hashCodeSet = false;
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
        if (!hashCodeSet) {
            final int prime = 31;
            int result = 1;
            result = prime * result + children.hashCode() + values.hashCode();
            hashCode = result;
            hashCodeSet = true;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        InListExpression other = (InListExpression)obj;
        if (!children.equals(other.children) || !values.equals(other.values)) return false;
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
        hashCodeSet = false;
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
            ImmutableBytesWritable currValue = value;
            if (firstChild.getSortOrder() != null && !firstChild.getSortOrder().equals(SortOrder.getDefault())) {
                // if we have to invert the bytes create a new ImmutableBytesWritable so that the
                // original value is not changed
                currValue = new ImmutableBytesWritable(value);
                type.coerceBytes(currValue, type, firstChild.getSortOrder(),
                    SortOrder.getDefault());
            }
            buf.append(type.toStringLiteral(currValue, null));
            buf.append(',');
            if (buf.length() >= maxToStringLen) {
                buf.append("... ");
                break;
            }
        }
        buf.setCharAt(buf.length()-1,')');
        return buf.toString();
    }

    public InListExpression clone(List<Expression> l) {
        return new InListExpression(l, this.rowKeyOrderOptimizable);
    }

    /**
     * get list of InListColumnKeyValuePair with a PK ordered structure
     * @param children children from rvc
     * @return the list of InListColumnKeyValuePair
     */
    public static List<InListColumnKeyValuePair> getSortedInListColumnKeyValuePair(List<Expression> children) {
        List<InListColumnKeyValuePair> inListColumnKeyValuePairList = new ArrayList<>();
        int numberOfColumns = 0;

        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            if (i == 0) {
                numberOfColumns = child.getChildren().size();
                for (int j = 0; j < child.getChildren().size(); j++) {
                    if (child.getChildren().get(j) instanceof RowKeyColumnExpression) {
                        RowKeyColumnExpression rowKeyColumnExpression =
                                (RowKeyColumnExpression)child.getChildren().get(j);
                        InListColumnKeyValuePair inListColumnKeyValuePair =
                                new InListColumnKeyValuePair(rowKeyColumnExpression);
                        inListColumnKeyValuePairList.add(inListColumnKeyValuePair);
                    } else {
                        // if one of the columns is not part of the pk, we ignore.
                        return null;
                    }
                }
            } else {
                if (numberOfColumns != child.getChildren().size()) {
                    // if the number of the PK columns doesn't match number of values,
                    // it should not sort it in PK position.
                    return null;
                }

                for (int j = 0; j < child.getChildren().size(); j++) {
                    LiteralExpression literalExpression = (LiteralExpression) child.getChildren().get(j);
                    inListColumnKeyValuePairList.get(j).addToLiteralExpressionList(literalExpression);
                }
            }
        }
        Collections.sort(inListColumnKeyValuePairList);
        return inListColumnKeyValuePairList;
    }

    /**
     * get a PK ordered Expression RowValueConstructor
     * @param inListColumnKeyValuePairList the object stores RowKeyColumnExpression and List of LiteralExpression
     * @param isStateless
     * @param numberOfRows number of literalExpressions
     * @return the new RowValueConstructorExpression with PK ordered expressions
     */
    public static List<Expression> getSortedRowValueConstructorExpressionList(
            List<InListColumnKeyValuePair> inListColumnKeyValuePairList, boolean isStateless, int numberOfRows) {
        List<Expression> l = new ArrayList<>();
        //reconstruct columns
        List<Expression> keyExpressions = new ArrayList<>();
        for (int i = 0; i < inListColumnKeyValuePairList.size(); i++) {
            keyExpressions.add(inListColumnKeyValuePairList.get(i).getRowKeyColumnExpression());
        }
        l.add(new RowValueConstructorExpression(keyExpressions,isStateless));

        //reposition to corresponding values
        List<List<Expression>> valueExpressionsList = new ArrayList<>();

        for (int j = 0; j < inListColumnKeyValuePairList.size(); j++) {
            List<LiteralExpression> valueList = inListColumnKeyValuePairList.get(j).getLiteralExpressionList();
            for (int i = 0; i < numberOfRows; i++) {
                if (j == 0) {
                    valueExpressionsList.add(new ArrayList<Expression>());
                }
                valueExpressionsList.get(i).add(valueList.get(i));
            }
        }
        for (List<Expression> valueExpressions: valueExpressionsList) {
            l.add(new RowValueConstructorExpression(valueExpressions, isStateless));
        }
        return l;
    }

    public static class InListColumnKeyValuePair implements Comparable<InListColumnKeyValuePair> {
        RowKeyColumnExpression rowKeyColumnExpression;
        List<LiteralExpression> literalExpressionList;

        public InListColumnKeyValuePair(RowKeyColumnExpression rowKeyColumnExpression) {
            this.rowKeyColumnExpression = rowKeyColumnExpression;
            this.literalExpressionList = new ArrayList<>();
        }

        public RowKeyColumnExpression getRowKeyColumnExpression() {
            return this.rowKeyColumnExpression;
        }

        public void addToLiteralExpressionList(LiteralExpression literalExpression) {
            this.literalExpressionList.add(literalExpression);
        }

        public List<LiteralExpression> getLiteralExpressionList() {
            return this.literalExpressionList;
        }

        @Override
        public int compareTo(InListColumnKeyValuePair o) {
            return rowKeyColumnExpression.getPosition() - o.getRowKeyColumnExpression().getPosition();
        }
    }
}
