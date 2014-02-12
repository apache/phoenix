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
package org.apache.phoenix.compile;

import java.util.Iterator;
import java.util.Map;


import com.google.common.collect.Maps;
import org.apache.phoenix.expression.Expression;

/**
 * 
 * Class to manage list of expressions inside of a select statement by
 * deduping them.
 *
 * 
 * @since 0.1
 */
public class ExpressionManager {
    // Use a Map instead of a Set because we need to get and return
    // the existing Expression
    private final Map<Expression, Expression> expressionMap;
    
    public ExpressionManager() {
        expressionMap = Maps.newHashMap();
    }
    
    /**
     * Add the expression to the set of known expressions for the select
     * clause. If the expression is already in the set, then the new one
     * passed in is ignored.
     * @param expression the new expression to add
     * @return the new expression if not already present in the set and
     * the existing one otherwise.
     */
    public Expression addIfAbsent(Expression expression) {
        Expression existingExpression = expressionMap.get(expression);
        if (existingExpression == null) {
            expressionMap.put(expression, expression);
            return expression;
        }
        return existingExpression;
    }
    
    public int getExpressionCount() {
        return expressionMap.size();
    }
    
    public Iterator<Expression> getExpressions() {
        return expressionMap.keySet().iterator();
    }
}
