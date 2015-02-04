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
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;


public class CoerceExpression extends BaseSingleExpression {
    private PDataType toType;
    private SortOrder toSortOrder;
    private Integer maxLength;
    
    public CoerceExpression() {
    }

    public static Expression create(Expression expression, PDataType toType) throws SQLException {
        if (toType == expression.getDataType()) {
            return expression;
        }
        return new CoerceExpression(expression, toType);
    }
    
    public static Expression create(Expression expression, PDataType toType, SortOrder toSortOrder, Integer maxLength) throws SQLException {
        if (toType == expression.getDataType() && toSortOrder == expression.getSortOrder()) {
            return expression;
        }
        return new CoerceExpression(expression, toType, toSortOrder, maxLength);
    }
    
    //Package protected for tests
    CoerceExpression(Expression expression, PDataType toType) {
        this(expression, toType, SortOrder.getDefault(), null);
    }
    
    CoerceExpression(Expression expression, PDataType toType, SortOrder toSortOrder, Integer maxLength) {
        this(ImmutableList.of(expression), toType, toSortOrder, maxLength);
    }

    public CoerceExpression(List<Expression> children, PDataType toType, SortOrder toSortOrder, Integer maxLength) {
        super(children);
        Preconditions.checkNotNull(toSortOrder);
        this.toType = toType;
        this.toSortOrder = toSortOrder;
        this.maxLength = maxLength;
    }
    
    public CoerceExpression clone(List<Expression> children) {
        return new CoerceExpression(children, this.getDataType(), this.getSortOrder(), this.getMaxLength());
    }
    
    @Override
    public Integer getMaxLength() {
        return maxLength;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((maxLength == null) ? 0 : maxLength.hashCode());
        result = prime * result + ((toSortOrder == null) ? 0 : toSortOrder.hashCode());
        result = prime * result + ((toType == null) ? 0 : toType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        CoerceExpression other = (CoerceExpression)obj;
        if (maxLength == null) {
            if (other.maxLength != null) return false;
        } else if (!maxLength.equals(other.maxLength)) return false;
        if (toSortOrder != other.toSortOrder) return false;
        if (toType == null) {
            if (other.toType != null) return false;
        } else if (!toType.equals(other.toType)) return false;
        return true;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        toType = PDataType.values()[WritableUtils.readVInt(input)];
        toSortOrder = SortOrder.fromSystemValue(WritableUtils.readVInt(input));
        int byteSize = WritableUtils.readVInt(input);
        this.maxLength = byteSize == -1 ? null : byteSize;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, toType.ordinal());
        WritableUtils.writeVInt(output, toSortOrder.getSystemValue());
        WritableUtils.writeVInt(output, maxLength == null ? -1 : maxLength);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (getChild().evaluate(tuple, ptr)) {
            getDataType().coerceBytes(ptr, getChild().getDataType(), getChild().getSortOrder(), getSortOrder(),
                    getChild().getMaxLength());
            return true;
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return toType;
    }
    
    @Override
    public SortOrder getSortOrder() {
        return toSortOrder;
    }    

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("TO_" + toType.toString() + "(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ", ");
        }
        buf.append(children.get(children.size()-1) + ")");
        return buf.toString();
    }
}
