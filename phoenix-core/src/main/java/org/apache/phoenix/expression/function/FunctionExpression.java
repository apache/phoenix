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
package org.apache.phoenix.expression.function;

import java.util.List;

import org.apache.phoenix.expression.BaseCompoundExpression;
import org.apache.phoenix.expression.Expression;

/**
 * 
 * Compiled representation of a built-in function
 *
 * 
 * @since 0.1
 */
public abstract class FunctionExpression extends BaseCompoundExpression {
    public enum OrderPreserving {NO, YES_IF_LAST, YES};
    public FunctionExpression() {
    }
    
    public FunctionExpression(List<Expression> children) {
        super(children);
    }
    
    /**
     * Determines whether or not the result of the function invocation
     * will be ordered in the same way as the input to the function.
     * Returning YES enables an optimization to occur when a
     * GROUP BY contains function invocations using the leading PK
     * column(s).
     * @return YES if the function invocation will always preserve order for
     * the inputs versus the outputs and false otherwise, YES_IF_LAST if the
     * function preserves order, but any further column reference would not
     * continue to preserve order, and NO if the function does not preserve
     * order.
     */
    public OrderPreserving preservesOrder() {
        return OrderPreserving.NO;
    }

    abstract public String getName();
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(getName() + "(");
        if (children.size()==0)
            return buf.append(")").toString();
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ", ");
        }
        buf.append(children.get(children.size()-1) + ")");
        return buf.toString();
    }
    
}
