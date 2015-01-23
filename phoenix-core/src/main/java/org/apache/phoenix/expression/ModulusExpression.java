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

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;


/**
 * 
 * Implementation of the LENGTH(<string>) build-in function. <string> is the string
 * of characters we want to find the length of. If <string> is NULL or empty, null
 * is returned.
 * 
 * 
 * @since 0.1
 */
public class ModulusExpression extends ArithmeticExpression {

    public ModulusExpression() { }

    public ModulusExpression(List<Expression> children) {
        super(children);
    }

    private Expression getDividendExpression() {
        return children.get(0);
    }
    
    private Expression getDivisorExpression() {
        return children.get(1);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // get the dividend
        Expression dividendExpression = getDividendExpression();
        if (!dividendExpression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        long dividend = dividendExpression.getDataType().getCodec().decodeLong(ptr, dividendExpression.getSortOrder());
        
        // get the divisor
        Expression divisorExpression = getDivisorExpression();
        if (!divisorExpression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        long divisor = divisorExpression.getDataType().getCodec().decodeLong(ptr, divisorExpression.getSortOrder());
        
        // actually perform modulus
        long remainder = dividend % divisor;
        
        // return the result, use encodeLong to avoid extra Long allocation
        byte[] resultPtr=new byte[PLong.INSTANCE.getByteSize()];
        getDataType().getCodec().encodeLong(remainder, resultPtr, 0);
        ptr.set(resultPtr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PLong.INSTANCE;
    }

    @Override
    protected String getOperatorString() {
        return " % ";
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

    @Override
    public ArithmeticExpression clone(List<Expression> children) {
        return new ModulusExpression(children);
    }
}
