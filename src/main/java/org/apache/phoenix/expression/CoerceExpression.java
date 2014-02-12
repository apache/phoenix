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
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


public class CoerceExpression extends BaseSingleExpression {
    private PDataType toType;
    private ColumnModifier toMod;
    private Integer byteSize;
    
    public CoerceExpression() {
    }

    public static Expression create(Expression expression, PDataType toType) throws SQLException {
        return toType == expression.getDataType() ? expression : expression instanceof LiteralExpression ? LiteralExpression.newConstant(((LiteralExpression)expression).getValue(), toType) : new CoerceExpression(expression, toType);
    }
    
    //Package protected for tests
    CoerceExpression(Expression expression, PDataType toType) {
        this(expression, toType, null, null);
    }
    
    CoerceExpression(Expression expression, PDataType toType, ColumnModifier toMod, Integer byteSize) {
        super(expression);
        this.toType = toType;
        this.toMod = toMod;
        this.byteSize = byteSize;
    }

    @Override
    public Integer getByteSize() {
        return byteSize;
    }
    
    @Override
    public Integer getMaxLength() {
        return byteSize;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((byteSize == null) ? 0 : byteSize.hashCode());
        result = prime * result + ((toMod == null) ? 0 : toMod.hashCode());
        result = prime * result + ((toType == null) ? 0 : toType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        CoerceExpression other = (CoerceExpression)obj;
        if (byteSize == null) {
            if (other.byteSize != null) return false;
        } else if (!byteSize.equals(other.byteSize)) return false;
        if (toMod != other.toMod) return false;
        if (toType != other.toType) return false;
        return true;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        toType = PDataType.values()[WritableUtils.readVInt(input)];
        toMod = ColumnModifier.fromSystemValue(WritableUtils.readVInt(input));
        int byteSize = WritableUtils.readVInt(input);
        this.byteSize = byteSize == -1 ? null : byteSize;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, toType.ordinal());
        WritableUtils.writeVInt(output, ColumnModifier.toSystemValue(toMod));
        WritableUtils.writeVInt(output, byteSize == null ? -1 : byteSize);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (getChild().evaluate(tuple, ptr)) {
            getDataType().coerceBytes(ptr, getChild().getDataType(), getChild().getColumnModifier(), getColumnModifier());
            return true;
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return toType;
    }
    
    @Override
    public ColumnModifier getColumnModifier() {
            return toMod;
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
