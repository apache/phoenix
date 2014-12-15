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

import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.types.PDataType;


/**
 * 
 * Subtract expression implementation
 *
 * 
 * @since 0.1
 */
public abstract class MultiplyExpression extends ArithmeticExpression {
    private Integer maxLength;
    private Integer scale;

    public MultiplyExpression() {
    }

    public MultiplyExpression(List<Expression> children) {
        super(children);
        Expression firstChild = children.get(0);
        maxLength = getPrecision(firstChild);
        scale = getScale(firstChild);
        for (int i=1; i<children.size(); i++) {
            Expression childExpr = children.get(i);
            maxLength = getPrecision(maxLength, getPrecision(childExpr), scale, getScale(childExpr));
            scale = getScale(maxLength, getPrecision(childExpr), scale, getScale(childExpr));
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

    @Override
    public String getOperatorString() {
        return " * ";
    }
    
    private static Integer getPrecision(Integer lp, Integer rp, Integer ls, Integer rs) {
        if (ls == null || rs == null) {
            return PDataType.MAX_PRECISION;
        }
        int val = lp + rp;
        return Math.min(PDataType.MAX_PRECISION, val);
    }

    private static Integer getScale(Integer lp, Integer rp, Integer ls, Integer rs) {
        // If we are adding a decimal with scale and precision to a decimal
        // with no precision nor scale, the scale system does not apply.
        if (ls == null || rs == null) {
            return null;
        }
        int val = ls + rs;
        return Math.min(PDataType.MAX_PRECISION, val);
    }
    
    @Override
    public Integer getScale() {
        return scale;
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }
}
