/*
 * Copyright 2010 The Apache Software Foundation
 *
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
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;


/**
 * 
 * Implementation for <,<=,>,>=,=,!= comparison expressions
 * @author jtaylor
 * @since 0.1
 */
public class ComparisonExpression extends BaseCompoundExpression {
    private CompareOp op;
    private static final String[] CompareOpString = new String[CompareOp.values().length];
    static {
        CompareOpString[CompareOp.EQUAL.ordinal()] = " = ";
        CompareOpString[CompareOp.NOT_EQUAL.ordinal()] = " != ";
        CompareOpString[CompareOp.GREATER.ordinal()] = " > ";
        CompareOpString[CompareOp.LESS.ordinal()] = " < ";
        CompareOpString[CompareOp.GREATER_OR_EQUAL.ordinal()] = " >= ";
        CompareOpString[CompareOp.LESS_OR_EQUAL.ordinal()] = " <= ";
    }
    
    public ComparisonExpression() {
    }

    public ComparisonExpression(CompareOp op, List<Expression> children) {
        super(children);
        if (op == null) {
            throw new NullPointerException();
        }
        this.op = op;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + op.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        ComparisonExpression other = (ComparisonExpression)obj;
        if (op != other.op) return false;
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.BOOLEAN;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!children.get(0).evaluate(tuple, ptr)) {
            return false;
        }
        byte[] lhsBytes = ptr.get();
        int lhsOffset = ptr.getOffset();
        int lhsLength = ptr.getLength();
        PDataType lhsDataType = children.get(0).getDataType();
        ColumnModifier lhsColumnModifier = children.get(0).getColumnModifier();
        
        if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
        
        byte[] rhsBytes = ptr.get();
        int rhsOffset = ptr.getOffset();
        int rhsLength = ptr.getLength();
        PDataType rhsDataType = children.get(1).getDataType();
        ColumnModifier rhsColumnModifier = children.get(1).getColumnModifier();   
        if (rhsDataType == PDataType.CHAR) {
            rhsLength = StringUtil.getUnpaddedCharLength(rhsBytes, rhsOffset, rhsLength, rhsColumnModifier);
        }
        if (lhsDataType == PDataType.CHAR) {
            lhsLength = StringUtil.getUnpaddedCharLength(lhsBytes, lhsOffset, lhsLength, lhsColumnModifier);
        }
        
        
        int comparisonResult = lhsDataType.compareTo(lhsBytes, lhsOffset, lhsLength, lhsColumnModifier, 
                rhsBytes, rhsOffset, rhsLength, rhsColumnModifier, rhsDataType);
        ptr.set(ByteUtil.compare(op, comparisonResult) ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
        return true;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        op = CompareOp.values()[WritableUtils.readVInt(input)];
        super.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, op.ordinal());
        super.write(output);
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

    public CompareOp getFilterOp() {
        return op;
    }
    
    @Override
    public String toString() {
        return (children.get(0) + CompareOpString[getFilterOp().ordinal()] + children.get(1));
    }    
}