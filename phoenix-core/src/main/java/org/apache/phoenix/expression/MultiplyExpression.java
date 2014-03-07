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
import org.apache.phoenix.schema.PDataType;


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
        for (int i=0; i<children.size(); i++) {
            Expression childExpr = children.get(i);
            maxLength = getPrecision(maxLength, scale, childExpr);
            scale = getScale(maxLength, scale, childExpr);
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
    
    private static Integer getPrecision(Integer lp, Integer ls, Expression childExpr) {
        Integer rp = childExpr.getMaxLength();
        if (childExpr.getDataType() == null) {
            return null;
        }
        if (rp == null) {
            rp = childExpr.getDataType().getMaxLength(null);
        }
        if (lp == null) {
            return rp;
        }
        if (rp == null) {
            return lp;
        }
        Integer rs = childExpr.getScale();
    	if (ls == null || rs == null) {
    		return PDataType.MAX_PRECISION;
    	}
        int val = lp + rp;
        return Math.min(PDataType.MAX_PRECISION, val);
    }

    private static Integer getScale(Integer lp, Integer ls, Expression childExpr) {
    	// If we are adding a decimal with scale and precision to a decimal
    	// with no precision nor scale, the scale system does not apply.
        Integer rs = childExpr.getScale();
        if (ls == null) {
            return rs;
        }
        if (rs == null) {
            return ls;
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
