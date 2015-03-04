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

import org.apache.phoenix.schema.types.PDataType;

public abstract class ArithmeticExpression extends BaseCompoundExpression {

    public ArithmeticExpression() {
    }

    public ArithmeticExpression(List<Expression> children) {
        super(children);
    }

    abstract public ArithmeticExpression clone(List<Expression> children);
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + getOperatorString());
        }
        buf.append(children.get(children.size()-1));
        buf.append(')');
        return buf.toString();
    }
    
    protected Integer getScale(Expression e) {
        Integer scale = e.getScale();
        if (scale != null) {
            return scale;
        }
        PDataType dataType = e.getDataType();
        if (dataType != null) {
            scale = dataType.getScale(null);
            if (scale != null) {
                return scale;
            }
        }
        return null;
    }
    protected int getPrecision(Expression e) {
        Integer precision = e.getMaxLength();
        if (precision != null) {
            return precision;
        }
        PDataType dataType = e.getDataType();
        if (dataType != null) {
            precision = dataType.getMaxLength(null);
            if (precision != null) {
                return precision;
            }
        }
        return PDataType.MAX_PRECISION;
    }
    
    abstract protected String getOperatorString();
}
