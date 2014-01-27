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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


public class CeilingDecimalExpression extends BaseSingleExpression {
    private static final MathContext CEILING_CONTEXT = new MathContext(1, RoundingMode.CEILING);
    
    public CeilingDecimalExpression() {
    }
    
    public CeilingDecimalExpression(Expression child)  {
        super(child);
    }
    
    protected MathContext getMathContext() {
        return CEILING_CONTEXT;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child =  getChild();
        if (child.evaluate(tuple, ptr)) {
            PDataType childType = child.getDataType();
            childType.coerceBytes(ptr, childType, child.getColumnModifier(), null);
            BigDecimal value = (BigDecimal) childType.toObject(ptr);
            value = value.round(getMathContext());
            byte[] b = childType.toBytes(value, child.getColumnModifier());
            ptr.set(b);
            return true;
        }
        return false;
    }

    @Override
    public ColumnModifier getColumnModifier() {
            return getChild().getColumnModifier();
    }    

    @Override
    public final PDataType getDataType() {
        return  getChild().getDataType();
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return getChild().accept(visitor);
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("CEIL(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(getChild().toString());
        }
        buf.append(")");
        return buf.toString();
    }
}
